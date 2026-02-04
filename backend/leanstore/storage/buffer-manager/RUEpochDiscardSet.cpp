#include "BufferManager.hpp"
#include "leanstore/utils/Misc.hpp"


namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void BufferManager::RUEpochDiscardSet::reset()
{
   std::lock_guard<instrumented_mutex> _l(m);
   ensure_equal(pids.size(), 0);
   ensure(is_garbage_collected == true);
   ensure_equal(done_gc, 0);
   inserted = 0;
   deleted = 0;
   is_garbage_collected = false;
   total = 0;
   invalid = 0;
   done_gc = FLAGS_ru_gc_threads;
   cur_ru_epoch += BMC::global_bf->max_open_ru_epochs;
}
// -------------------------------------------------------------------------------------
u64 BufferManager::RUEpochDiscardSet::size()
{
   std::lock_guard<instrumented_mutex> _l(m);
   return pids.size();
}
// -------------------------------------------------------------------------------------
void BufferManager::RUEpochDiscardSet::insert(PID pid, LID lsn)
{
   std::lock_guard<instrumented_mutex> _l(m);
   ensure(is_garbage_collected == false);
   bool ok = pids.insert({pid, lsn}).second;
   ensure(ok);
   inserted.fetch_add(1, std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
LID BufferManager::RUEpochDiscardSet::erase(PID pid)
{
   std::lock_guard<instrumented_mutex> _l(m);
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
   bool ok = per > 0.8;
   if (ok || (++cnt % 200) == 0) {
      printf("\ntot = %d, invalid = %d, to_gc = %d => per %f %%\n", tot, i, d, per * 100);
   }
   return ok;
}
// -------------------------------------------------------------------------------------
BufferManager::PersistantRUState::PersistantRUState(u32 max_open_ru_epochs) :
   max_open_ru_epochs(max_open_ru_epochs) {}
// -------------------------------------------------------------------------------------
void BufferManager::PersistantRUState::loadFromPersistantStorage() 
{
   u64 sz = utils::upAlign(getSize(), 4096);
   s64 ret = pread(BMC::global_bf->ssd_fd, this, sz, BMC::global_bf->persistant_ru_state_offset);
   ensure_equal(ret, sz);
}
// -------------------------------------------------------------------------------------
void BufferManager::PersistantRUState::writetoPersistantStorage()
{
   u64 sz = utils::upAlign(getSize(), 4096);
   s64 ret = pwrite(BMC::global_bf->ssd_fd, this, sz, BMC::global_bf->persistant_ru_state_offset);
   ensure_equal(ret, sz);
}
// -------------------------------------------------------------------------------------
} // namespace storage
} // namespace leanstore
