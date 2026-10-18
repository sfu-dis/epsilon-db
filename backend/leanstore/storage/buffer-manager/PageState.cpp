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
      printf("\n[WARN] suspect deadlock, stuck in lock() for %lu iterations, locked(%lu)\n", attempts, locked);
      dump();
      print_backtrace();
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
   u8 nb_log_records = (v1 & nb_log_records_mask) >> nb_log_records_shift;
   // With more than one log record, the lsn field holds a pointer to the LSN array.
   LID lsn = (holdsLSNArray(nb_log_records)) ? reinterpret_cast<u64>(decodeLSNArray(v1)) : (v1 & full_lsn_mask);
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

   u8 nb_log_records = (v & nb_log_records_mask) >> nb_log_records_shift;
   LID lsn = (holdsLSNArray(nb_log_records)) ? reinterpret_cast<u64>(decodeLSNArray(v)) : (v & full_lsn_mask);

   std::cout << "PAGE state dump:\n";
   std::cout << "  state           = " << to_string(state) << "\n";
   std::cout << "  locked          = " << (locked ? "true" : "false") << "\n";
   std::cout << "  raw             = 0x" << std::hex << v << std::dec << "\n";
   if (nb_log_records > 1) {
      std::cout << "  lsn_list        = " << reinterpret_cast<LID*>(lsn) << "\n";
   } else {
      std::cout << "  lsn             = " << lsnToString(lsn) << "\n";
   }
   std::cout << "  nb_log_records  = " << +nb_log_records << "\n";
   if (state != HOT) {
      std::cout << "  discard_count   = " << +decodeDiscardCount(v) << "\n";
   }
   std::cout << "  pid             = " << +(this - global_discard_state) << "\n";
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
