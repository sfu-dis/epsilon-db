#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "PageState.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"
#include "leanstore/utils/Logger.hpp"
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
namespace cr
{
struct WALEntry;  // Forward declaration
struct WALDTEntry;  // Forward declaration
}
// -------------------------------------------------------------------------------------
namespace storage
{
template <typename T> class CustomSlabAllocator;  // Forward declaration
// -------------------------------------------------------------------------------------
extern thread_local s32 tls_writer_thread_id;
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
   int log_fd = -1;
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
   // One per thread that writes to storage.
   const u32 io_writer_threads_count;
   std::unique_ptr<padded_iostat[]> writers_iostat;
   std::unique_ptr<CustomSlabAllocator<LID>[]> per_pp_allocator;
   // -------------------------------------------------------------------------------------
   // written only by ru_epoch_mgr, read by page providers.
   std::atomic<u64> rate = 100;
   u64 pad0[7];
   // -------------------------------------------------------------------------------------
   void pageProviderThread(u64 pp_id, u64 p_begin, u64 p_end);  // [p_begin, p_end)
   void ruGarbageCollectorThread(u32 gc_id);
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   atomic<bool> shutdown_flush_in_progress = false;
   atomic<u64> pp_threads_counter = 0;
   atomic<u64> gc_threads_counter = 0;
   // -------------------------------------------------------------------------------------
   std::unique_ptr<utils::Logger> logger;
   // -------------------------------------------------------------------------------------
public:
   atomic<ru_epoch_t> ru_epoch = 0; // persistant
   atomic<ru_epoch_t> oldest_uncollected_ru_epoch = 0; // persistant
   atomic<ru_epoch_t> reclaimed_ru_epoch = -1; // persistant
   atomic<ru_epoch_t> reclaiming_ru_epoch = -1; // persistant
   u64 pad1[7];
   static u64 RU_SIZE;
   bool global_force_gc = false;
   PageState *discard_state;
   u32 max_open_ru_epochs;
   u32 max_pending_lsn;
   struct RUEpochDiscardSet {
      u32 id;
      instrumented_mutex m{"ru_discard_set"};
      void* mmaped_log = nullptr;
      u64 log_segment_start = -1;
      u64 log_segment_size = -1;
      bool force_gc = false;
      alignas(CACHE_LINE_SIZE) atomic<u64> offset_batch{0};
      // -------------------------------------------------------------------------------------
      alignas(CACHE_LINE_SIZE) atomic<s32> inserted{0};
      alignas(CACHE_LINE_SIZE) atomic<s32> deleted{0};
      alignas(CACHE_LINE_SIZE) atomic<bool> is_currently_being_garbage_collected{false};
      alignas(CACHE_LINE_SIZE) atomic<s32> total{0};
      alignas(CACHE_LINE_SIZE) atomic<s32> invalid{0};
      alignas(CACHE_LINE_SIZE) atomic<s32> done_gc{static_cast<s32>(FLAGS_ru_gc_threads)};
      alignas(CACHE_LINE_SIZE) atomic<s32> total_fixed{0}; // used just as a stat
      // -------------------------------------------------------------------------------------
      ru_epoch_t cur_ru_epoch = -1;
      atomic<bool> active{false};
      // -------------------------------------------------------------------------------------
      atomic<u32> undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::OTHER] = {0};

      void open(ru_epoch_t new_ru_epoch);
      void reset();
      u32 ReclaimUnitUsage();
      bool shouldGC();
      void dump();
   };
   u64 persistant_ru_state_offset;
   struct alignas(4096) PersistantRUState {
      u64 max_open_ru_epochs; // serves as a magic debugging number also.
      s64 ru_epoch;
      s64 oldest_active_ru_epoch;
      s64 reclaimed_ru_epoch;
      u64 total_host_writes;
      u64 expected_extra_gc_writes;
      // XXX(mfd) : Persisting only totals is enough for now, we assume
      //  invalid count is always 0. In other words, we assume we're
      //   recovering from a load only workload.
      u32 totals[0];

      PersistantRUState(u32 max_open_ru_epochs);
      u64 getSize() const {
         return sizeof(PersistantRUState) + max_open_ru_epochs * sizeof(u32);
      }
      void loadFromPersistantStorage();
      void writetoPersistantStorage();
   };
   PersistantRUState *persistant_ru_state;
   struct RUEpochsState {
     private:
      u32 size;
      std::unique_ptr<RUEpochDiscardSet[]> data;

     public:
      RUEpochsState(u64 size)
        : size(size), data(std::make_unique<RUEpochDiscardSet[]>(size)) {}

      /**
       * operator[] is Exact : use when we're guarentted that the ru_epoch cannot be
       * reclaimed while we're accessing it. This typically the case for the
       * background page fixing threads.
       * The at() is a non-exact variant of operator[], it is used for stats and for the ru usage.
       * The window of error is small that we allow small error.
       */
      RUEpochDiscardSet& at(size_t index) {
         return data[index % size];
      }
      const RUEpochDiscardSet& at(size_t index) const {
         return data[index % size];
      }
      RUEpochDiscardSet& operator[](size_t index) {
         ensure_equal(data[index % size].cur_ru_epoch, s64(index));
         return at(index);
      }
      const RUEpochDiscardSet& operator[](size_t index) const {
         ensure_equal(data[index % size].cur_ru_epoch, s64(index));
         return at(index);
      }
   };
   RUEpochsState ru_discard_set;
   // -------------------------------------------------------------------------------------
   std::mutex gc_m;
   std::condition_variable gc_cv;
   std::vector<u64> to_gc_epochs;
   int is_gc_sleeping{0};
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   BufferFrame& randomBufferFrame();
   Partition& getPartition(PID);
   u64 getPartitionID(PID);
   CustomSlabAllocator<LID>& randomAllocator();
   // -------------------------------------------------------------------------------------
   // Temporary hack: let workers evict the last page they used
   static thread_local BufferFrame* last_read_bf;
  public:
   // -------------------------------------------------------------------------------------
   BufferManager(s32 ssd_fd, u64 total_blocks_in_ssd, u32 max_open_ru_epochs);
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
   BufferFrame& resolveMetaSwip(Swip<BufferFrame>& meta_swip);
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
   // Single choke point for updating header of a page before write back. Used by :
   // 1. PageProvider for eviction.
   // 2. During shutdown to write all dirty frames.
   // 3. Background Page Fixer Thread.
   void pageWriteBackPrologue(BufferFrame::Page& page);
   // -------------------------------------------------------------------------------------
   void startBackgroundThreads();
   void stopWriterThreads();
   void stopRUEpochManagerThread();
   void stopBackgroundThreads();
   void waitForAllBackgroundThreads();
   void writeAllBufferFrames();
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);
   // -------------------------------------------------------------------------------------
   u64 getPoolSize() { return dram_pool_size; }
   DTRegistry& getDTRegistry() { return DTRegistry::global_dt_registry; }
   u64 consumedPages();
   BufferFrame& getContainingBufferFrame(const u8*);  // get the buffer frame containing the given ptr address
   // -------------------------------------------------------------------------------------
   bool logRecordSanityCheck(cr::WALEntry *entry, BufferFrame::Page& page, LID lsn);
   // -------------------------------------------------------------------------------------
   // STATS
   struct Stats {
      atomic<u64> discard_state_peak_mem_usage = 0;
      atomic<u64> estimated_gc_writes = 0;
      atomic<u64> forced_gc_count = 0;
   } bm_stats;
};
// -------------------------------------------------------------------------------------
class BMC
{
  public:
   static BufferManager* global_bf;
};
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
