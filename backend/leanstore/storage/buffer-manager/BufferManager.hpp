#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "PageState.hpp"
#include "RUEpochDiscardSet.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "leanstore/storage/fdp/PlacementPolicy.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"
#include "leanstore/utils/Logger.hpp"
// -------------------------------------------------------------------------------------
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
class RUEpochManager;  // Forward declaration
class RUBackgroundPageFixer;  // Forward declaration
class SSDLayout;  // Forward declaration
class CompressedSpace;  // Forward declaration
class LBAChain;  // Forward declaration
class BlockPacker;  // Forward declaration
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
   friend class RUEpochManager;
   friend class RUBackgroundPageFixer;
   friend class LBAChain;
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
   struct alignas(CACHE_LINE_SIZE) padded_iostat {
     std::array<atomic<u64>, MAX_RUHS> io_counter{};
     u8 pad[CACHE_LINE_SIZE - sizeof(io_counter)];
   };
   static_assert(sizeof(padded_iostat) == CACHE_LINE_SIZE, "");
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
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   atomic<bool> shutdown_flush_in_progress = false;
   atomic<u64> pp_threads_counter = 0;
   std::unique_ptr<RUEpochManager> ru_epoch_mgr;
   // Owns and manages the background page fixer threads.
   std::unique_ptr<RUBackgroundPageFixer> ru_bg_page_fixer;
   // Only with FLAGS_bg_page_fixer_variant 2: the LBAs written to each RU epoch of the DEFAULT_RUH.
   std::unique_ptr<LBAChain> lba_chain;
   // -------------------------------------------------------------------------------------
   std::unique_ptr<utils::Logger> logger;
   // -------------------------------------------------------------------------------------
   fdp::PlacementPolicy fdp_placement_policy;
   // -------------------------------------------------------------------------------------
   // Only with FLAGS_compress_pages
   std::unique_ptr<CompressedSpace> compressed_space;
   void writeAllBufferFramesCompressed();
   // -------------------------------------------------------------------------------------
public:
   // One independent RU epoch stream per RUH. Only DEFAULT_RUH is persisted for now.
   std::array<atomic<ru_epoch_t>, MAX_RUHS> ru_epoch;
   std::array<atomic<ru_epoch_t>, MAX_RUHS> oldest_uncollected_ru_epoch;
   std::array<atomic<ru_epoch_t>, MAX_RUHS> reclaimed_ru_epoch;
   // Live database pages whose latest copy on the SSD was written through each RUH.
   // Used on the proxy on the utilization of the SSD per RUH.
   struct alignas(CACHE_LINE_SIZE) PaddedPageCounter {
      atomic<s64> pages{0};
   };
   std::array<PaddedPageCounter, MAX_RUHS> ruh_pages;
   // DEFAULT_RUH only: advanced by the page-fixing GC threads.
   atomic<ru_epoch_t> reclaiming_ru_epoch = -1; // persistant
   u64 pad1[7];
   static u64 RU_SIZE;
   // nullptr unless track_discard_state.
   PageState *discard_state;
   // With FLAGS_bg_page_fixer_tree_lookup the parent swips alone tell whether a page is discarded and its pending LSNs.
   // The page state is then only maintained for the state filter or the paranoid checks (FLAGS_discard_state_paranoid).
   // The discard count is not tracked without it, bf.header.discard_count stays 0.
   bool track_discard_state = false;
   u32 max_open_ru_epochs;
   u32 max_pending_lsn = 0;
   u64 persistant_ru_state_offset;
   // Number of database pages (4 KiB blocks), page max_pid - 1 is the last one before persistant_ru_state_offset.
   const PID max_pid;
   // Every allocated PID is < pid_space. Same as max_pid, except with FLAGS_compress_pages where the PIDs are logical
   // and may outnumber the blocks.
   const PID pid_space;
   // Exclusive upper bound of the PIDs allocated so far.
   PID allocatedPIDsEnd();
   // Gives a PID back to the partition it comes from.
   void recyclePID(PID pid, ru_epoch_t ru_epoch, ruh_id_t ruh);
   PersistantRUState *persistant_ru_state;
   std::vector<RUEpochsState> ru_discard_set; // one per RUH
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
   BufferManager(s32 ssd_fd, const SSDLayout& ssd_layout, u32 max_open_ru_epochs);
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
   // Called when every PID below pid_space is allocated, does not return.
   [[noreturn]] void outOfSpace();
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
   // Accounts device writes for the RU epochs, called by the writers.
   void countWrittenBlocks(ruh_id_t ruh, u64 n);
   // Same for the sealed blocks of a packer (DEFAULT_RUH), with an LBA chain each block counts to its own RU epoch.
   void countPackedBlocks(const BlockPacker& packer);
   // With compression, counts a page written to its RU epoch (page.ru_epoch, set by the BlockPacker) of the DEFAULT_RUH.
   void countPackedPage(const BufferFrame::Page& page);
   // With compression, the block (its 4 KiB LBA) is written to ru_epoch of the DEFAULT_RUH: the copy it held in its
   // previous RU epoch gets invalidated on the SSD, accounted in that RU epoch's block_invalid unless it is reclaimed.
   void countBlockWrite(u64 block, ru_epoch_t ru_epoch);
   // FLAGS_lba_chain_verify: every page on the SSD is listed in the LBA chain of its RU epoch. when is logged.
   void verifyLBAChains(const char* when);
   void readPageAsync(PID pid, u8* destination, std::function<void()> callback);
   void fDataSync();
   // Single choke point for updating header of a page before write back. Used by :
   // 1. PageProvider for eviction.
   // 2. During shutdown to write all dirty frames.
   // 3. Background Page Fixer Thread.
   void pageWriteBackPrologue(BufferFrame& bf);
   // Header updates of a dirty buffer frame that is about to be written back, followed by the prologue.
   // Pre: bf is exclusively latched and is_being_written_back is set. Used by the PageProvider and
   // by the Background Page Fixer for the hot pages.
   void prepareDirtyWriteBack(BufferFrame& bf);
   // -------------------------------------------------------------------------------------
   void startBackgroundThreads();
   void stopWriterThreads();
   void stopRUEpochManagerThread();
   void stopBackgroundThreads();
   void waitForAllBackgroundThreads();
   void writeAllBufferFrames();
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   // Kept apart from the PersistantRUState, in FLAGS_ru_table_stats_file.
   void persistPerTableStats();
   void recoverPerTableStats();
#endif
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
      atomic<u64> device_gc_events = 0;  // FDP Media Reallocated events, with fdp_count_gc_events
      atomic<s64> workers_waiting_fixer_io = 0;  // gauge: workers blocked on an IO frame of the bg page fixer
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
