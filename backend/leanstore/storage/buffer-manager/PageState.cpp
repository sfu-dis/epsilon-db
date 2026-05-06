#include "PageState.hpp"
#include "BufferManager.hpp"

// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
u64 PageState::lock()
{
   u64 attempts = 0;
   u64 locked = 0;
retry:
   // if (attempts++ > 16) sched_yield();
   if ((++attempts % 1073741824) == 0) {
      printf("[WARN] suspect deadlock, stuck in lock() for %lu iterations, locked(%lu)\n", attempts, locked);
      dump();
      __asm__ volatile("int3");
   }
   u64 v1 = raw.load(std::memory_order_acquire);
   if (v1 & latch_bit) {
      ++locked;
      _mm_pause();
      goto retry;
   }
   u64 v2 = v1 | latch_bit;
   if (!raw.compare_exchange_strong(v1, v2)) {
      _mm_pause();
      goto retry;
   }
   return v1;
}
// -------------------------------------------------------------------------------------
std::pair<u64, u8> PageState::getLocked()
{
   u64 v1 = lock();
   LID lsn = v1 & lsn_mask;
   u8 nb_log_records = (v1 & nb_log_records_mask) >> nb_log_records_shift;
   return {lsn, nb_log_records};
}
// -------------------------------------------------------------------------------------
std::string PageState::to_string(PAGE_STATE s)
{
   switch (s) {
      case NOT_SURE:
         return "NOT_SURE";
      case DISCARDED:
         return "DISCARDED";
      case CLEAN:
         return "CLEAN";
      case HOT:
         return "HOT";
      default:
         return "UNKNOWN";
   }
}
// -------------------------------------------------------------------------------------
void PageState::dump()
{
   PAGE_STATE state = getSTATE();
   bool locked = isLocked();

   u64 v = raw.load(std::memory_order_acquire);

   LID lsn = v & lsn_mask;
   u8 nb_log_records = (v & nb_log_records_mask) >> nb_log_records_shift;

   std::cout << "PAGE state dump:\n";
   std::cout << "  state           = " << to_string(state) << "\n";
   std::cout << "  locked          = " << (locked ? "true" : "false") << "\n";
   std::cout << "  raw             = 0x" << std::hex << v << std::dec << "\n";
   std::cout << "  lsn             = " << lsn << "\n";
   std::cout << "  nb_log_records  = " << +nb_log_records << "\n";
   std::cout << "  pid             = " << +(this - global_discard_state) << "\n";
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
