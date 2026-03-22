#include "BufferManager.hpp"
#include "leanstore/utils/Misc.hpp"


namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
void BufferManager::RUEpochDiscardSet::reset()
{
   ensure_equal(pids.size(), 0);
   ensure(is_garbage_collected == true);
   ensure_equal(done_gc, 0);
   ensure(force_gc == false);
   offset_batch = 0;
   inserted = 0;
   deleted = 0;
   is_garbage_collected = false;
   total = 0;
   invalid = 0;
   done_gc = FLAGS_ru_gc_threads;
   cur_ru_epoch += BMC::global_bf->max_open_ru_epochs;
   BMC::global_bf->reclaimed_ru_epoch.fetch_add(1);
}
// -------------------------------------------------------------------------------------
u64 BufferManager::RUEpochDiscardSet::size()
{
   std::lock_guard<instrumented_mutex> _l(m);
   return pids.size();
}
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
bool BufferManager::RUEpochDiscardSet::insert(PID pid, LID lsn)
{
   if (cur_ru_epoch <= BMC::global_bf->reclaiming_ru_epoch.load(std::memory_order_acquire)) {
      return false;
   }
   bool ok = pids.insert({pid, lsn}).second;
   ensure(ok);
   inserted.fetch_add(1, std::memory_order_relaxed);
   return true;
}
// -------------------------------------------------------------------------------------
// Responsability of the caller to acquire the set lock
LID BufferManager::RUEpochDiscardSet::erase(PID pid)
{
   if (pids.count(pid) == 0) return INEXISTANT_LSN;
   LID lsn = pids[pid];
   pids.erase(pid);
   deleted.fetch_add(1, std::memory_order_relaxed);
   return lsn;
}
// -------------------------------------------------------------------------------------
bool BufferManager::RUEpochDiscardSet::shouldGC()
{
   static u64 cnt = 0;
   s32 d = inserted.load(std::memory_order_acquire) - deleted.load(std::memory_order_acquire);
   s32 i = invalid.load(std::memory_order_acquire);
   s32 tot = total.load(std::memory_order_acquire);
   double per = (i+d) * 1.0f / tot;
   bool ok = false;
   if (force_gc) {
      printf("[WARN] Forcing GC \n");
      force_gc = false;
      ok =  true;
   } else {
      ok = per > FLAGS_ru_gc_threshold;
   }
   if (ok || (++cnt % 1024) == 0) {
      printf("\n ru_epoch = %ld tot = %d, invalid = %d, to_gc = %d => per %f %%\n", cur_ru_epoch, tot, i, d, per * 100);
   }
   return ok;
}
// -------------------------------------------------------------------------------------
BufferManager::RUEpochDiscardSet *BufferManager::RUEpochsState::getSetLockedCanFail(s64 ru_epoch, bool try_lock_or_fail)
{
   ensure(ru_epoch != -1);
   auto* set = &data[ru_epoch % size];
   if (try_lock_or_fail) {
      if (!set->m.try_lock()) {
         return nullptr;
      }
   } else {
      set->m.lock();
   }
   if (set->cur_ru_epoch != ru_epoch
       || ru_epoch <= BMC::global_bf->reclaimed_ru_epoch.load(std::memory_order_acquire)) {
      set->m.unlock();
      return nullptr;
   }
   return set;
}
// -------------------------------------------------------------------------------------
BufferManager::PersistantRUState::PersistantRUState(u32 max_open_ru_epochs) :
   max_open_ru_epochs(max_open_ru_epochs) {}
// -------------------------------------------------------------------------------------
void BufferManager::PersistantRUState::loadFromPersistantStorage() 
{
   u64 sz = utils::upAlign(getSize(), 4096);
   s64 ret = pread(BMC::global_bf->ssd_fd, this, sz, BMC::global_bf->persistant_ru_state_offset);
   ensure_equal(ret, s64(sz));
}
// -------------------------------------------------------------------------------------
void BufferManager::PersistantRUState::writetoPersistantStorage()
{
   u64 sz = utils::upAlign(getSize(), 4096);
   s64 ret = pwrite(BMC::global_bf->ssd_fd, this, sz, BMC::global_bf->persistant_ru_state_offset);
   ensure_equal(ret, s64(sz));
}
// -------------------------------------------------------------------------------------
} // namespace storage
} // namespace leanstore
