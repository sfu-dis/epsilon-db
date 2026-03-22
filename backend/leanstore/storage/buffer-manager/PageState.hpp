#pragma once
// -------------------------------------------------------------------------------------
#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct PageState {
   // -------------------------------------------------------------------------------------
   // 1xxxxxxxxxxxx latched, 0xxxxxxxxxxxx unlatched
   static constexpr u64 latch_bit = u64(1) << 63;
   static constexpr u64 latch_mask = ~(u64(1) << 63);
   static constexpr u64 state_shift = 61;
   static constexpr u64 state_mask = u64(3) << state_shift;
   static constexpr u64 state_unmask = ~(u64(3) << state_shift);
   // XXX(mfd) : a NOT_SURE state means either free (unused PID) or that after recovery
   // I am still not sure whether the copy on SSD is up-to-date or not. This is an optimisation
   // to allow recovery while keeping the global page state ephemeral. This does not have a
   // noticeable performance impact on worker threads (unswizzeling pages). But may have
   // an impact on the garbage collecting thread by having him double check the state of the page.
   enum PAGE_STATE : u8 { NOT_SURE = 0, DISCARDED = 1, CLEAN = 2, HOT = 3 };
   static constexpr u64 state_discarded_mask = u64(DISCARDED) << state_shift;
   static constexpr u64 state_clean_mask = u64(CLEAN) << state_shift;
   static constexpr u64 state_hot_mask = u64(HOT) << state_shift;
   // 3 bits to store the length
   static constexpr u64 max_log_records_bits = 3;
   static constexpr u64 nb_log_records_shift = state_shift - max_log_records_bits;
   static constexpr u64 nb_log_records_mask = u64(7) << nb_log_records_shift;
   static constexpr u64 lsn_mask = (u64(1) << nb_log_records_shift) - 1;
   static_assert(latch_bit == 0x8000000000000000, "");
   static_assert(state_mask == 0x6000000000000000, "");
   static_assert(latch_mask == 0x7FFFFFFFFFFFFFFF, "");
   static_assert(state_unmask == 0x9FFFFFFFFFFFFFFF, "");
   static_assert(state_discarded_mask == 0x2000000000000000, "");
   static_assert(state_clean_mask == 0x4000000000000000, "");
   static_assert(state_hot_mask == 0x6000000000000000, "");
   static_assert(lsn_mask == 0x03FFFFFFFFFFFFFF, "");

   atomic<u64> raw;

   // -------------------------------------------------------------------------------------
   PageState() = default;
   PageState(const PageState& other) = delete;
   PageState& operator=(const PageState& other) = delete;
   PageState(PageState&& other) = delete;
   // -------------------------------------------------------------------------------------
   bool isLocked() { return raw.load(std::memory_order_acquire) & latch_bit; }
   PAGE_STATE getSTATE()
   {
      u64 state = (raw.load(std::memory_order_acquire) & state_mask) >> state_shift;
      ensure_lt(state, 4);
      return static_cast<PAGE_STATE>(state);
   }
   bool isClean() { return getSTATE() == PAGE_STATE::CLEAN; }
   bool isDiscarded() { return getSTATE() == PAGE_STATE::DISCARDED; }
   bool isHot() { return getSTATE() == PAGE_STATE::HOT; }
   bool isFree() { return raw.load(std::memory_order_acquire) == 0; }
   // -------------------------------------------------------------------------------------
   u64 lock()
   {
   retry:
      u64 v1 = raw.load(std::memory_order_acquire);
      if (v1 & latch_bit) {
         goto retry;
      }
      u64 v2 = v1 | latch_bit;
      if (!raw.compare_exchange_strong(v1, v2)) {
         goto retry;
      }
      return v1;
   }
   std::pair<u64, u8> getLocked()
   {
   retry:
      u64 v1 = raw.load(std::memory_order_acquire);
      if (v1 & latch_bit) {
         goto retry;
      }
      u64 v2 = v1 | latch_bit;
      if (!raw.compare_exchange_strong(v1, v2)) {
         goto retry;
      }
      // return v1 & state_unmask;
      LID lsn = v1 & lsn_mask;
      u8 nb_log_records = (v1 & nb_log_records_mask) >> nb_log_records_shift;
      return {lsn, nb_log_records};
   }
   bool tryDiscard(const std::vector<LID>& pending_lsn)
   {
      u64 v1 = raw.load(std::memory_order_acquire);
      if ((v1 & latch_bit) || ((v1 & state_hot_mask) != state_hot_mask)) {
         return false;
      }
      if (!raw.compare_exchange_strong(v1, v1 | latch_bit)) {
         return false;
      }
      ensure(isHot());
      // For now, just store the length for debugging
      u64 pending_lsns = pending_lsn.size();
      u64 new_value = 0;
      if (pending_lsns == 1) {
         new_value = pending_lsn.back() | state_discarded_mask | (pending_lsns << nb_log_records_shift);
      } else {
         auto* lsn_list = new LID[pending_lsns];
         ensure(lsn_list != nullptr);
         memcpy(lsn_list, pending_lsn.data(), pending_lsns * sizeof(LID));
         new_value = reinterpret_cast<u64>(lsn_list) | state_discarded_mask | (pending_lsns << nb_log_records_shift);
         // __asm__ volatile("int3");
      }
      // return raw.compare_exchange_strong(v1, new_value);
      raw.store(new_value, std::memory_order_release);
      return true;
   }
   // void unlockDirty(LID lsn) { raw.store(lsn | state_discarded_mask, std::memory_order_release); }
   void unlockClean(LID lsn) { raw.store(lsn | state_clean_mask, std::memory_order_release); }
   void unlockBF(BufferFrame* bf) { raw.store(reinterpret_cast<u64>(bf) | state_hot_mask, std::memory_order_release); }
   void unlock()
   {
      ensure(isLocked());
      u64 unlocked = raw.load(std::memory_order_relaxed) & latch_mask;
      raw.store(unlocked, std::memory_order_release);
   }
   // BufferFrame& asBufferFrame() { return *reinterpret_cast<BufferFrame*>(raw); }
   // -------------------------------------------------------------------------------------
   static std::string to_string(PAGE_STATE s)
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
   // Caller should be holding the lock.
   void dump()
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
   }
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(PageState) == sizeof(u64), "");
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
