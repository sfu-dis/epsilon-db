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
struct BufferFrame; // Forward Declaration
// -------------------------------------------------------------------------------------
struct PageState {
   static inline PageState* global_discard_state;
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
   static constexpr u64 max_log_id_bits = 10; // up to 1024 log_ids
   static constexpr u64 max_log_id = (u64(1) << max_log_id_bits) - 1;
   static constexpr u64 log_id_shift = nb_log_records_shift - max_log_id_bits;
   static constexpr u64 log_id_mask = max_log_id << log_id_shift;
   // static constexpr u64 lsn_mask = (u64(1) << nb_log_records_shift) - 1;
   static constexpr u64 lsn_mask = (u64(1) <<log_id_shift) - 1;
   static_assert(latch_bit == 0x8000000000000000, "");
   static_assert(state_mask == 0x6000000000000000, "");
   static_assert(latch_mask == 0x7FFFFFFFFFFFFFFF, "");
   static_assert(state_unmask == 0x9FFFFFFFFFFFFFFF, "");
   static_assert(state_discarded_mask == 0x2000000000000000, "");
   static_assert(state_clean_mask == 0x4000000000000000, "");
   static_assert(state_hot_mask == 0x6000000000000000, "");
   static_assert(max_log_id == 0x3FF);
   static_assert(log_id_mask == 0x003FF000000000000);
   static_assert(lsn_mask == 0x0000FFFFFFFFFFFF, "");

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
   u64 lock();
   std::pair<u64, u8> getLocked();
   // -------------------------------------------------------------------------------------
   template <bool keep_locked>
   bool tryDiscard(LID* pending_lsn, u64 pending_lsn_count, u64 log_id)
   {
      u64 v1 = raw.load(std::memory_order_acquire);
      if ((v1 & latch_bit) || ((v1 & state_hot_mask) != state_hot_mask)) {
         return false;
      }
      if (!raw.compare_exchange_strong(v1, v1 | latch_bit)) {
         return false;
      }
      ensure(isHot());
      ensure_lt(log_id, max_log_id);
      u64 new_value = state_discarded_mask | (pending_lsn_count << nb_log_records_shift) | (log_id << log_id_shift);
      if (pending_lsn_count == 1) {
         new_value |= pending_lsn[0];
      } else {
         new_value |= reinterpret_cast<u64>(pending_lsn);
      }
      if constexpr (keep_locked) {
         raw.store(new_value | latch_bit, std::memory_order_release);
      } else {
         raw.store(new_value, std::memory_order_release);
      }
      return true;
   }
   u32 getLogID()
   {
      // for now only accessed when the page is in discarded state
      ensure(isDiscarded());
      return (raw.load(std::memory_order_acquire) & log_id_mask) >> log_id_shift;
   }
   void unlockClean(LID lsn, u64 log_id) {
      ensure(lsn != INVALID_LSN);
      ensure((lsn & ~lsn_mask) == 0);
      ensure_lt(log_id, max_log_id);
      raw.store(lsn | state_clean_mask | (log_id << log_id_shift), std::memory_order_release);
   }
   void unlockBF(BufferFrame* bf)
   {
      raw.store(reinterpret_cast<u64>(bf) | state_hot_mask, std::memory_order_release);
   }
   void unlock()
   {
      ensure(isLocked());
      u64 unlocked = raw.load(std::memory_order_relaxed) & latch_mask;
      raw.store(unlocked, std::memory_order_release);
   }
   void free() { raw.store(0, std::memory_order_release); }
   LID asLSN() { return raw.load(std::memory_order_acquire) & lsn_mask; }
   // BufferFrame& asBufferFrame() { return *reinterpret_cast<BufferFrame*>(raw); }
   // -------------------------------------------------------------------------------------
   static std::string to_string(PAGE_STATE s);
   void dump();
};
// -------------------------------------------------------------------------------------
static_assert(sizeof(PageState) == sizeof(u64), "");
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
