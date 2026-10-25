#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>
// -------------------------------------------------------------------------------------
struct io_uring_cqe;  // Forward declaration
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
struct WALEntry;  // Forward declaration
struct Logging;  // Forward declaration
}
// -------------------------------------------------------------------------------------
namespace storage
{
class BufferManager;  // Forward declaration
struct RUEpochDiscardSet;  // Forward declaration
// -------------------------------------------------------------------------------------
// Owns the ru_gc background threads: they wait for the RU epochs handed over by the
// ru_epoch_mgr, scan their log segment, and fix and write back the discarded pages that
// live in that RU epoch as to free the corrsponding RU on the SSD.
// They are also responsible of reclaiming the log space occupied by pages in the reclaimed
// RU epoch.
class RUBackgroundPageFixer
{
  public:
   explicit RUBackgroundPageFixer(BufferManager& bm);
   // Spawns FLAGS_ru_gc_threads (detached) ru_gc threads. The threads are stopped through the
   // BufferManager shutdown flags.
   void start();
   // Hands an RU epoch over to the ru_gc threads. Returns false, without enqueuing,
   // when the queue lock is contended.
   // log_only : the RU epoch's log is full, fix its discarded pages and reclaim its log
   // but do not free the RU epoch, it is freed later by its in-order pass.
   bool tryEnqueueRUEpoch(ru_epoch_t ru_epoch, bool log_only = false);
   // Wakes up all the sleeping ru_gc threads, used on shutdown so that they could exit.
   void wakeUpAll();
   u64 runningThreads() const { return running_threads.load(); }
   // -------------------------------------------------------------------------------------
   // Debugging stats, read by the profiling thread (BMTable). Gauges, never reset.
   enum class Phase : u8 { IDLE, MAP_LOG, READ, LOOKUP, FIX, WRITE_BACK, HOT, DRAIN, BARRIER, FINISH, COUNT };
   static constexpr const char* phase_names[] = {"idle", "map_log", "read", "lookup", "fix", "write_back", "hot", "drain", "barrier", "finish"};
   // One letter per phase, for the per thread phase string.
   static constexpr char phase_letters[] = "IMRLFWHDBE";
   struct alignas(64) ThreadStats {
      std::atomic<u8> phase{static_cast<u8>(Phase::IDLE)};
      std::atomic<u64> phase_since_ns{0};  // steady clock
      std::atomic<u64> deferred_hot{0};  // hot pages waiting in deferred_hot_pids
   };
   struct Stats {
      std::unique_ptr<ThreadStats[]> threads;  // FLAGS_ru_gc_threads
      // IO frames inserted by the fixers and not yet released, the workers wanting these pages wait on them.
      std::atomic<s64> io_frames_held{0};
      std::atomic<u64> jobs_enqueued{0};
      std::atomic<u64> enqueue_failures{0};
      // The RU epoch being collected (-1 when none), since when, and how far the LBA chain walk got.
      std::atomic<s64> cur_epoch{-1};
      std::atomic<u64> cur_epoch_start_ns{0};
      std::atomic<u64> chain_entries{0};
      std::atomic<u64> chain_done{0};
   } stats;
   static u64 nowNs();

  private:
   struct ThreadContext;  // Per ru_gc thread state, defined in the .cpp
   struct GCJob {
      ru_epoch_t ru_epoch;
      bool log_only;
   };
   // -------------------------------------------------------------------------------------
   BufferManager& bm;
   // -------------------------------------------------------------------------------------
   // Shared between the ru_gc threads and the ru_epoch_mgr.
   // RU epochs to collect, in order. Only grows, each thread tracks how far it got.
   // FIXME(mfd) : The fact that it only grows is just because I was lazy to think
   // about properi concurrent reclaiming.
   std::mutex queue_m;
   std::condition_variable queue_cv;
   std::vector<GCJob> to_gc_epochs;
   int sleeping_threads{0};  // protected by queue_m
   std::atomic<u64> running_threads{0};
   // -------------------------------------------------------------------------------------
   void run(u32 gc_id);
   void waitForRUEpochsToCollect(ThreadContext& ctx);
   void collectRUEpoch(ThreadContext& ctx, GCJob job);
   void mapRUEpochLog(ThreadContext& ctx, cr::Logging& logging, bool scan_log);
   void finishRUEpoch(ThreadContext& ctx, u64 seconds_taken);
   void finishRUEpochLogReclaim(ThreadContext& ctx, u64 seconds_taken);
   // FLAGS_bg_page_fixer_variant 2: reads the LBA chain of the RU epoch into the set.
   void loadLBAChain(RUEpochDiscardSet& set, ru_epoch_t ru_epoch, bool with_entries);
   // Scans the next 4 KiB of the log and issues the reads of the discarded pages to fix.
   u64 scanLogBatch(ThreadContext& ctx, u64& offset);
   // FLAGS_bg_page_fixer_variant 2: same for a batch of the PIDs listed in the LBA chain.
   u64 scanLBAChainBatch(ThreadContext& ctx, const PID* pids, u64 count);
   // FLAGS_bg_page_fixer_tree_lookup: reads every page of a batch of the LBA chain, finds the discarded ones through
   // their parent swip, fixes them and waits for their write back.
   void fixLBAChainBatchInTree(ThreadContext& ctx, const PID* pids, u64 count);
   bool lookUpPageInTree(ThreadContext& ctx, u64 idx);
   bool mayBeDiscardedInLog(ThreadContext& ctx, PID pid);
   bool needsFix(ThreadContext& ctx, PID pid);
   bool registerPageToFix(ThreadContext& ctx, LID first_lsn, PID pid, u64 idx);
   void issueFixRead(ThreadContext& ctx, PID pid, u64& pages_to_fix, u64& submitted);
   void fixPages(ThreadContext& ctx, u64 pages_to_fix);
   void waitForFixedPagesWriteBack(ThreadContext& ctx);
   // Completion handlers of the read, resp. write, ring.
   void fixPage(ThreadContext& ctx, io_uring_cqe* cqe);
   // Applies the log records of the registered page at idx (read into buf_pages[idx]) and issues its write back.
   void redoAndWriteBack(ThreadContext& ctx, u64 idx, u64 data);
   void ackFixedPage(ThreadContext& ctx, io_uring_cqe* cqe);
   // Write back of the pages of the reclaimed log that are cached (hot).
   enum class HotPage { DONE, WRITING, RETRY };
   void fixHotPages(ThreadContext& ctx);
   HotPage tryWriteBackHotPage(ThreadContext& ctx, PID pid);
   void writeBackHotPages(ThreadContext& ctx);
   void ackHotPage(ThreadContext& ctx, io_uring_cqe* cqe);
   void drainDeferredHotPages(ThreadContext& ctx);
   void setPhase(ThreadContext& ctx, Phase phase);
   // Removes the IO frame inserted by registerPageToFix() and lets the waiting workers retry.
   // This method could probably be part of the BufferManager itself not this class.
   void releaseIOFrame(PID pid);
   void dumpCorruptedLogEntry(ThreadContext& ctx, u64 offset, const cr::WALEntry& entry, cr::WALEntry* last_entry);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
