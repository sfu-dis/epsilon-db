#include "RUEpochDiscardSet.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>
// -------------------------------------------------------------------------------------
#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
void RUEpochDiscardSet::open(ru_epoch_t new_ru_epoch)
{
   if (FLAGS_enable_discarding && active.load()) {
      printf(
          "[WARN] Background Page Fixer thread could not keep up with the IO writes\n"
          "Will shutdown if this persists\n");
      dump();
      while (active.load()) {
         // This happens when the garbage collection thread is not fast enough
         // to reclaim the oldest RU. This is highly undesirable and we better
         // not enter in this state at all.
         m.unlock();
         std::this_thread::sleep_for(std::chrono::microseconds(40000));
         m.lock();
      }
   }
   ensure(!FLAGS_enable_discarding || cur_ru_epoch == -1);
   cur_ru_epoch = new_ru_epoch;
   active.store(true);
}
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
void RUEpochDiscardSet::reset()
{
   ensure(FLAGS_enable_discarding || is_currently_being_garbage_collected.load() == true);
   ensure_equal(done_gc, 0);
   clear();
}
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
// Used by the RUHs which are reclaimed without going through the page fixing GC threads.
void RUEpochDiscardSet::resetWithoutGC()
{
   ensure(FLAGS_enable_discarding);
   ensure(ruh != DEFAULT_RUH);
   ensure_equal(done_gc, FLAGS_ru_gc_threads);
   clear();
}
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
void RUEpochDiscardSet::afterLogReclaim()
{
   ensure(log_reclaimed.load());
   ensure_equal(done_gc, 0);
   clearGCState();
}
// -------------------------------------------------------------------------------------
void RUEpochDiscardSet::clearGCState()
{
   offset_batch = 0;
   is_currently_being_garbage_collected = false;
   log_segment_start = -1;
   log_segment_size = -1;
   // Last, the other GC threads spin on it.
   done_gc = FLAGS_ru_gc_threads;
}
// -------------------------------------------------------------------------------------
void RUEpochDiscardSet::clear()
{
   inserted = 0;
   deleted = 0;
   total = 0;
   invalid = 0;
   total_fixed = 0;
   log_reclaimed = false;
   clearGCState();
   cur_ru_epoch = -1;
   active.store(false);
   // std::memset(undiscardable_cause_distribution, 0, sizeof(undiscardable_cause_distribution));
   for (auto& counter : undiscardable_cause_distribution) {
      counter.store(0, std::memory_order_relaxed);
   }
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   for (auto& t : per_table) {
      t.total.store(0, std::memory_order_relaxed);
      t.invalid.store(0, std::memory_order_relaxed);
      t.inserted.store(0, std::memory_order_relaxed);
      t.deleted.store(0, std::memory_order_relaxed);
   }
#endif
}
// -------------------------------------------------------------------------------------
void RUEpochDiscardSet::dump()
{
   cout << "===== RUEpochDiscardSet =====" << endl;
   cout << "id: " << id << endl;

   cout << "mmaped_log: " << mmaped_log << endl;
   cout << "log_segment_start: " << log_segment_start << endl;
   cout << "log_segment_size: " << log_segment_size << endl;

   cout << "log_reclaimed: " << (log_reclaimed.load() ? "true" : "false") << endl;

   cout << "offset_batch: " << offset_batch.load() << endl;

   cout << "inserted: " << inserted.load() << endl;
   cout << "deleted: " << deleted.load() << endl;
   cout << "is_currently_being_garbage_collected: " << (is_currently_being_garbage_collected.load() ? "true" : "false") << endl;

   cout << "total: " << total.load() << endl;
   cout << "invalid: " << invalid.load() << endl;
   cout << "done_gc: " << done_gc.load() << endl;
   cout << "total_fixed: " << total_fixed.load() << endl;

   cout << "cur_ru_epoch: " << cur_ru_epoch << endl;
   cout << "active: " << (active.load() ? "true" : "false") << endl;

   cout << "=============================" << endl;
}
// -------------------------------------------------------------------------------------
u32 RUEpochDiscardSet::ReclaimUnitUsage()
{
   // This formula does not account for those pages that are in the buffer pool
   s32 d = inserted.load(std::memory_order_acquire) - deleted.load(std::memory_order_acquire);
   s32 i = invalid.load(std::memory_order_acquire);
   return i + d;
}
// -------------------------------------------------------------------------------------
bool RUEpochDiscardSet::shouldGC()
{
   static u64 cnt[MAX_RUHS] = {0};
   s32 d = inserted.load(std::memory_order_acquire) - deleted.load(std::memory_order_acquire);
   s32 i = invalid.load(std::memory_order_acquire);
   s32 tot = total.load(std::memory_order_acquire);
   double per = (i + d) * 1.0f / tot;
   const bool ok = per > FLAGS_ru_gc_threshold;
   if (ok || (++cnt[ruh] % 1024) == 0) {
      printf("\n RUH = %u ru_epoch = %ld tot = %d, invalid = %d, to_gc = %d => per %f %%\n", ruh, cur_ru_epoch, tot, i, d, per * 100);
      const u32 c_none = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::NONE].load(std::memory_order_acquire);
      const u32 c_new = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::NEWLY_ALLOCATED].load(std::memory_order_acquire);
      const u32 c_ppl = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::PPL_BUFFER_FULL].load(std::memory_order_acquire);
      const u32 c_depth = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::MAX_DISCARDING_DEPTH_EXCEEDED].load(std::memory_order_acquire);
      const u32 c_split = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::JUST_SPLITTED].load(std::memory_order_acquire);
      const u32 c_ru = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::RECLAIMED_RU_EPOCH].load(std::memory_order_acquire);
      const u32 c_other = undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::OTHER].load(std::memory_order_acquire);
      const u32 total = c_none + c_new + c_ppl + c_depth + c_split + c_ru + c_other;

      std::printf("total=%u | NONE=%u(%.1f%%) NEW_ALLOC=%u(%.1f%%) PPL_FULL=%u(%.1f%%) MAX_DEPTH=%u(%.1f%%) SPLIT=%u(%.1f%%) RU_EPOCH=%u(%.1f%%) OTHER=%u(%.1f%%)\n",
                  total, c_none, total ? 100.0 * c_none / total : 0.0, c_new, total ? 100.0 * c_new / total : 0.0, c_ppl,
                  total ? 100.0 * c_ppl / total : 0.0, c_depth, total ? 100.0 * c_depth / total : 0.0, c_split, total ? 100.0 * c_split / total : 0.0,
                  c_ru, total ? 100.0 * c_ru / total : 0.0, c_other, total ? 100.0 * c_other / total : 0.0);
   }
   return ok;
}
// -------------------------------------------------------------------------------------
PersistantRUState::PersistantRUState(u32 max_open_ru_epochs) : max_open_ru_epochs(max_open_ru_epochs) {}
// -------------------------------------------------------------------------------------
void PersistantRUState::loadFromPersistantStorage(int fd, u64 offset)
{
   u64 sz = utils::upAlign(getSize(), 4096);
   s64 ret = pread(fd, this, sz, offset);
   ensure_equal(ret, s64(sz));
}
// -------------------------------------------------------------------------------------
void PersistantRUState::writetoPersistantStorage(int fd, u64 offset)
{
   u64 sz = utils::upAlign(getSize(), 4096);
   s64 ret = pwrite(fd, this, sz, offset);
   ensure_equal(ret, s64(sz));
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
