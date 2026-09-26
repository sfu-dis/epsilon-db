#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
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
   bool tryEnqueueRUEpoch(ru_epoch_t ru_epoch);
   // Wakes up all the sleeping ru_gc threads, used on shutdown so that they could exit.
   void wakeUpAll();
   u64 runningThreads() const { return running_threads.load(); }

  private:
   struct ThreadContext;  // Per ru_gc thread state, defined in the .cpp
   // -------------------------------------------------------------------------------------
   BufferManager& bm;
   // -------------------------------------------------------------------------------------
   // Shared between the ru_gc threads and the ru_epoch_mgr.
   // RU epochs to collect, in order. Only grows, each thread tracks how far it got.
   // FIXME(mfd) : The fact that it only grows is just because I was lazy to think
   // about properi concurrent reclaiming.
   // Also, need to fix the hack that uses the size of the queue to know the ru_epoch.
   std::mutex queue_m;
   std::condition_variable queue_cv;
   std::vector<ru_epoch_t> to_gc_epochs;
   int sleeping_threads{0};  // protected by queue_m
   std::atomic<u64> running_threads{0};
   // -------------------------------------------------------------------------------------
   void run(u32 gc_id);
   void waitForRUEpochsToCollect(ThreadContext& ctx);
   void collectRUEpoch(ThreadContext& ctx, ru_epoch_t gc_ru_epoch);
   void mapRUEpochLog(ru_epoch_t gc_ru_epoch, cr::Logging& logging);
   void finishRUEpoch(ThreadContext& ctx, u64 seconds_taken);
   // Scans the next 4 KiB of the log and issues the reads of the discarded pages to fix.
   u64 scanLogBatch(ThreadContext& ctx, u64& offset);
   bool registerPageToFix(ThreadContext& ctx, const cr::WALEntry& entry, PID pid, u64 idx);
   void fixPages(ThreadContext& ctx, u64 pages_to_fix);
   // Completion handlers of the read, resp. write, ring.
   void fixPage(ThreadContext& ctx, io_uring_cqe* cqe);
   void ackFixedPage(ThreadContext& ctx, io_uring_cqe* cqe);
   // Removes the IO frame inserted by registerPageToFix() and lets the waiting workers retry.
   // This method could probably be part of the BufferManager itself not this class.
   void releaseIOFrame(PID pid);
   void dumpCorruptedLogEntry(ThreadContext& ctx, u64 offset, const cr::WALEntry& entry, cr::WALEntry* last_entry);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
