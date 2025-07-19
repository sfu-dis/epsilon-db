#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "../../utils/Misc.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <fdp.h>
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <sys/mman.h>

#include <cstring>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
class LeanStore;  // Forward declaration
namespace profiling
{
class BMTable;  // Forward declaration
}
namespace storage
{
// -------------------------------------------------------------------------------------
struct FreedBfsBatch {
   BufferFrame *freed_bfs_batch_head = nullptr, *freed_bfs_batch_tail = nullptr;
   u64 freed_bfs_counter = 0;
   // -------------------------------------------------------------------------------------
   void reset()
   {
      freed_bfs_batch_head = nullptr;
      freed_bfs_batch_tail = nullptr;
      freed_bfs_counter = 0;
   }
   // -------------------------------------------------------------------------------------
   void push(Partition& partition)
   {
      partition.dram_free_list.batchPush(freed_bfs_batch_head, freed_bfs_batch_tail, freed_bfs_counter);
      reset();
   }
   // -------------------------------------------------------------------------------------
   u64 size() { return freed_bfs_counter; }
   // -------------------------------------------------------------------------------------
   void add(BufferFrame& bf)
   {
      bf.header.next_free_bf = freed_bfs_batch_head;
      if (freed_bfs_batch_head == nullptr) {
         freed_bfs_batch_tail = &bf;
      }
      freed_bfs_batch_head = &bf;
      freed_bfs_counter++;
      // -------------------------------------------------------------------------------------
   }
};
// -------------------------------------------------------------------------------------
// TODO: revisit the comments after switching to clock replacement strategy
// Notes on Synchronization in Buffer Manager
// Terminology: PPT: Page Provider Thread, WT: Worker Thread. P: Parent, C: Child, M: Cooling stage mutex
// Latching order for all PPT operations (unswizzle, evict): M -> P -> C
// Latching order for all WT operations: swizzle: [unlock P ->] M -> P ->C, coolPage: P -> C -> M
// coolPage conflict with this order which could lead to a deadlock which we can mitigate by jumping instead of blocking in BMPlainGuard [WIP]
// -------------------------------------------------------------------------------------
class BufferManager
{
  private:
   friend struct AsyncWriteBuffer;
   friend class leanstore::LeanStore;
   friend class leanstore::profiling::BMTable;
   // -------------------------------------------------------------------------------------
   BufferFrame* bfs;
   // -------------------------------------------------------------------------------------
   const int ssd_fd;
   // -------------------------------------------------------------------------------------
   // Free  Pages
   const u8 safety_pages = 10;               // we reserve these extra pages to prevent segfaults
   u64 dram_pool_size;                       // total number of dram buffer frames
   atomic<u64> ssd_freed_pages_counter = 0;  // used to track how many pages did we really allocate
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   u64 partitions_count;
   u64 partitions_mask;
   std::vector<std::unique_ptr<Partition>> partitions;
   std::atomic<u64> clock_cursor = 0;

   // -------------------------------------------------------------------------------------
   // Threads managements
   void pageProviderThread(u64 p_begin, u64 p_end);  // [p_begin, p_end)
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   Partition& roundRobinPartition();
   BufferFrame& randomBufferFrame();
   Partition& getPartition(PID);
   u64 getPartitionID(PID);
   // -------------------------------------------------------------------------------------
   // Temporary hack: let workers evict the last page they used
   static thread_local BufferFrame* last_read_bf;
   // -------------------------------------------------------------------------------------
   // Management of the FDP device
   // think of moving this to the libfdp so that it will be used by all other engines
   // offer the interface : fdp_get_open_ru_number(plid_t, optional thread affinity)
   struct ReclaimUnitUsageMgr {
   	  fdp_dev_t fdp_dev; // from libfdp
      // std::mutex reclaim_units_mutex;
      // std::unique_ptr<u64[]> reclaim_units_usage;
      // std::vector<u64> reclaim_units_usage;
      std::thread timer_thread;
      std::atomic<bool> timer_thread_keep_running;
      std::atomic<u64> current_open_reclaim_unit;
      u64 last_seen_remaining_media_writes;
      
      ReclaimUnitUsageMgr() {
         // read the reclaim unit size from the device.
         // just start with an io management recieve command
         // this should be issued just immediately after initializing the device.

         // open the fdp device
         int err = fdp_open(FLAGS_ssd_path.c_str(), &this->fdp_dev);   
         ensure(err == 0);
         last_seen_remaining_media_writes = fdp_get_remaining_bytes_in_ru(&fdp_dev, 0); 
         std::cout << "Last Seen remaining media writes :  " << last_seen_remaining_media_writes
             << std::endl;
         current_open_reclaim_unit = 0;
         fdp_register_gc_callback(&fdp_dev, []() { std::cerr << "GC Triggered !!!!" << std::endl;});
      }

      ~ReclaimUnitUsageMgr() {
         std::cerr << "~ReclaimUnitUsageMgr()\n";
         timer_thread_keep_running.store(false);
      }

      void StartRUThread() {
        timer_thread_keep_running.store(true);
        timer_thread = std::thread([this](){ this->TimerThread(); });
        timer_thread.detach();
      }

      // Timer thread method
      void TimerThread() {
         utils::pinThisThread(((FLAGS_pin_threads) ? FLAGS_worker_threads : 0) + FLAGS_wal + FLAGS_pp_threads);
         fprintf(stderr, "Open new RU #%lu with error %f%%\n", current_open_reclaim_unit.load(std::memory_order_relaxed), (3193344U - last_seen_remaining_media_writes)*100.0f/3193344);
         // FILE *fp = fopen("ru_usage.log", "w");
         // ensure(fp != nullptr);
         while (timer_thread_keep_running) {
            // issue an io management recieve command
            u64 remaining_media_writes = fdp_get_remaining_bytes_in_ru(&fdp_dev, 0);
            // FIXME(mfd) : because of fluctuations we will use this heuristic
            // if (remaining_media_writes < 100000) {
            if (remaining_media_writes > last_seen_remaining_media_writes ) {
               // the open RU was written to capacity, we
               // std::lock_guard lock(reclaim_units_mutex);
               current_open_reclaim_unit.fetch_add(1U);
               ensure(remaining_media_writes <= 3193344U);
               fprintf(stderr, "Open new RU #%lu with error (%lu) ruamw = %lu > %lu\n", current_open_reclaim_unit.load(std::memory_order_relaxed), 3193344U - remaining_media_writes, remaining_media_writes, last_seen_remaining_media_writes);
            }
            last_seen_remaining_media_writes = remaining_media_writes;
            // fprintf(fp, "%lu\n", remaining_media_writes);
            // usleep(500 * 1000); // 1ms
			sleep(1);
         }
      }

      void RUInspectorDeamon() {
        printf("[INFO] Staerting the inspector Deamon\n");
        //std::ofstream trace_file;
        //trace_file.open("buffer_pool.ru_state.csv", std::ios::out | std::ios::trunc);
        while (1) {
          
          sleep(60);
        }
      }
   };

   ReclaimUnitUsageMgr reclaim_unit_usage_mgr;
  public:
   // -------------------------------------------------------------------------------------
   BufferManager(s32 ssd_fd);
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame& allocatePage();
   inline BufferFrame& tryFastResolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
   {
      if (swip_value.isHOT()) {
         BufferFrame& bf = swip_value.asBufferFrame();
         swip_guard.recheck();
         return bf;
      } else {
         return resolveSwip(swip_guard, swip_value);
      }
   }
   BufferFrame& resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value);
   void evictLastPage();
   void reclaimPage(BufferFrame& bf);
   // -------------------------------------------------------------------------------------
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address
    * temporarily 2- if not, then posix_check if it exists in cooling stage
    * queue, yes? remove it from the queue and return the buffer frame 3- in
    * anycase, posix_check if the threshold is exceeded, yes ? unswizzle a random
    * BufferFrame (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPageSync(PID pid, u8* destination);
   void readPageAsync(PID pid, u8* destination, std::function<void()> callback);
   void fDataSync();
   // -------------------------------------------------------------------------------------
   void startBackgroundThreads();
   void stopBackgroundThreads();
   void writeAllBufferFrames();
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);
   // -------------------------------------------------------------------------------------
   u64 getPoolSize() { return dram_pool_size; }
   DTRegistry& getDTRegistry() { return DTRegistry::global_dt_registry; }
   u64 consumedPages();
   BufferFrame& getContainingBufferFrame(const u8*);  // get the buffer frame containing the given ptr address
};                                                    // namespace storage
// -------------------------------------------------------------------------------------
class BMC
{
  public:
   static BufferManager* global_bf;
};
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
