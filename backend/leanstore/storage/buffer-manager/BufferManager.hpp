#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <sys/mman.h>

#include <condition_variable>
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
   friend class leanstore::LeanStore;
   friend class leanstore::profiling::BMTable;
   friend class AsyncWriteBuffer;
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
   struct alignas(64) padded_iostat {
     atomic<u64> io_counter = 0;
     u64 pad[7];
   };
   std::unique_ptr<padded_iostat[]> per_pp_iostats;
   std::atomic<u64> tot_gc_writes = 0;
   // Approximate # of free NAND pages on the SSD in KiB.
   atomic<s64> write_credit_available;
   void pageProviderThread(u64 pp_id, u64 p_begin, u64 p_end);  // [p_begin, p_end)
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   // -------------------------------------------------------------------------------------
   atomic<u64> ru_epoch = 0;
   const u64 RU_SIZE = 3193344UL; // Hardcoded for now, we will read from the device later. 
   struct RUEpochDiscardSet {
      std::mutex m;
      std::unordered_set<PID> pids;
      // Do we need padding here?
      alignas(64) atomic<s32> inserted{0};
      alignas(64) atomic<s32> deleted{0};
      alignas(64) atomic<bool> is_garbage_collected{false};
      alignas(64) atomic<s32> total{0};
      alignas(64) atomic<s32> invalid{0};


      void insert(PID pid) {
         std::lock_guard _l(m);
         bool ok = pids.insert(pid).second;
         ensure(ok);
         inserted.fetch_add(1, std::memory_order_relaxed);
      }
      bool erase(PID pid) {
         std::lock_guard _l(m);
         bool ok = pids.erase(pid);
         if (ok) deleted.fetch_add(1, std::memory_order_relaxed);
         return ok;
      }
      void ensureInexistant(PID pid) {
         std::lock_guard _l(m);
         ensure(pids.count(pid) == 0);
         PARANOID_BLOCK() {
            log.emplace_back(pid, 'i', nullptr);
         }
      }
      bool shouldGC() {
         // XXX(mfd) : The number of inserted elements could execeed  the RU_SIZE
         //  because we're approximating the ru_epoch boundary.
         s32 d = inserted.load(std::memory_order_acquire) - deleted.load(std::memory_order_acquire);
         s32 i = invalid.load(std::memory_order_acquire);
         // s32 d = inserted.load(std::memory_order_relaxed);
         s32 tot = total.load(std::memory_order_acquire);
         double per = (i+d) * 1.0f / tot;
         bool ok = per > 0.9;
         if (ok) {
            printf("tot = %d, invalid = %d, to_gc = %d => per %f %%\n", tot, i, d, per);
         }
         // return (( invalid.load(std::memory_order_acquire) + inserted.load(std::memory_order_relaxed)) * 1.0f/ ) > 0.9;
         return ok;
      }
      bool getBatch(std::vector<PID> &out_pids, u32 batch_size) {
         out_pids.clear();
         std::lock_guard _l(m);
         for (const auto &pid : pids) {
            out_pids.push_back(pid);
            if (out_pids.size() == batch_size) {
               break;
            }
         }
         return !out_pids.empty();
      }
      u64 size() {
         std::lock_guard _l(m);
         return pids.size();
      }
      // Debugging 
      std::vector<std::tuple<PID, char, BufferFrame*>> log;
      bool insert(PID pid, BufferFrame *bf) {
         std::unique_lock _l(m);
         bool ok = pids.insert(pid).second;
         ensure(ok);
         PARANOID_BLOCK() {
            log.emplace_back(pid, 'I', bf);
         }
         inserted.fetch_add(1, std::memory_order_relaxed);
         return ok;
      }
      bool erase(PID pid, BufferFrame *bf, char c = 'E') {
         std::lock_guard _l(m);
         bool ok = pids.erase(pid);
         if (ok) deleted.fetch_add(1, std::memory_order_relaxed);
         PARANOID_BLOCK() {
            if (ok) log.emplace_back(pid, '+', bf);
            else log.emplace_back(pid, '-', bf);
         }
         return ok;
      }
      void log_op(PID pid, BufferFrame *bf, char c) {
         PARANOID_BLOCK() {
            std::lock_guard _l(m);
            log.emplace_back(pid, c, bf);
         }
      }
      void dump_history_of_page(PID pid) {
         int c = 0;
         for (const auto& e : log) {
            if (std::get<0>(e) == pid) {
               printf("(%c, %p) ", std::get<1>(e), std::get<2>(e));
               ++c;
            }
         }
      }      
   };
   RUEpochDiscardSet ru_discard_set[4096 * 2];
   std::mutex gc_m;
   std::condition_variable gc_cv;
   std::vector<u64> to_gc_epochs;
   bool is_gc_sleeping{true};
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   BufferFrame& randomBufferFrame();
   Partition& getPartition(PID);
   u64 getPartitionID(PID);
   // -------------------------------------------------------------------------------------
   // Temporary hack: let workers evict the last page they used
   static thread_local BufferFrame* last_read_bf;

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
   // Just for debugging
   void dump_history_of_pid(PID pid) {
      u64 epoch = ru_epoch.load() + 3;
      printf("History of page with pid %u\n", pid);
      for (u64 e = 0; e < epoch; ++e) {
         printf("\n%lu ", e);
         ru_discard_set[e].dump_history_of_page(pid);
      }
   }   
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
