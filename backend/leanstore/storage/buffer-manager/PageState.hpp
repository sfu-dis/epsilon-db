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
   // Saturating counter of how many times the page got discarded since its last write back.
   // Only meaningful in the DISCARDED/CLEAN/NOT_SURE states. While the page is cached (HOT) the
   // lowest 48 bits hold the BufferFrame pointer and the counter lives in bf.header.discard_count.
   static constexpr u64 discard_count_bits = 3;
   static constexpr u8 max_discard_count = (u8(1) << discard_count_bits) - 1;
   static constexpr u64 discard_count_shift = log_id_shift - discard_count_bits;
   static constexpr u64 discard_count_mask = u64(max_discard_count) << discard_count_shift;
   static constexpr u64 lsn_mask = (u64(1) << discard_count_shift) - 1;
   // The LSN array is LID aligned, the pointer is stored shifted to fit in the 45 bits.
   static constexpr u64 lsn_array_ptr_shift = 3;
   static constexpr u64 lsn_array_ptr_mask = lsn_mask;
   static constexpr u64 bf_ptr_mask = (u64(1) << log_id_shift) - 1;
   static_assert(latch_bit == 0x8000000000000000, "");
   static_assert(state_mask == 0x6000000000000000, "");
   static_assert(latch_mask == 0x7FFFFFFFFFFFFFFF, "");
   static_assert(state_unmask == 0x9FFFFFFFFFFFFFFF, "");
   static_assert(state_discarded_mask == 0x2000000000000000, "");
   static_assert(state_clean_mask == 0x4000000000000000, "");
   static_assert(state_hot_mask == 0x6000000000000000, "");
   static_assert(max_log_id == 0x3FF);
   static_assert(log_id_mask == 0x03FF000000000000);
   static_assert(discard_count_mask == 0x0000E00000000000, "");
   static_assert(lsn_mask == 0x00001FFFFFFFFFFF, "");
   static_assert(bf_ptr_mask == 0x0000FFFFFFFFFFFF, "");
   static_assert((lsn_mask & log_id_mask) == 0, "");
   static_assert((lsn_mask & discard_count_mask) == 0, "");
   static_assert((log_id_mask & discard_count_mask) == 0, "");
   static_assert(alignof(LID) == (u64(1) << lsn_array_ptr_shift), "");
   // A single LSN (log_id + offset) is stored in the log_id and lsn fields, with the discard count
   // bits in between. Masking them out gives back the LSN as long as the offset fits in 45 bits.
   static constexpr u64 full_lsn_mask = log_id_mask | lsn_mask;
   static_assert(log_id_shift == LSN_OFFSET_BITS, "");
   static_assert(max_log_id_bits == LSN_LOG_ID_BITS, "");
   static_assert((full_lsn_mask | discard_count_mask) == LSN_MASK, "");
   static constexpr bool fitsInPageState(LID lsn) { return (lsnOffset(lsn) & ~lsn_mask) == 0; }
   static constexpr u64 encodeDiscardCount(u8 discard_count) { return u64(discard_count) << discard_count_shift; }
   static constexpr u8 decodeDiscardCount(u64 v) { return (v & discard_count_mask) >> discard_count_shift; }
   static u64 encodeLSNArray(LID* lsn_array)
   {
      u64 ptr = reinterpret_cast<u64>(lsn_array);
      ensure((ptr & ((u64(1) << lsn_array_ptr_shift) - 1)) == 0);
      ensure(((ptr >> lsn_array_ptr_shift) & ~lsn_array_ptr_mask) == 0);
      return ptr >> lsn_array_ptr_shift;
   }
   static LID* decodeLSNArray(u64 v) { return reinterpret_cast<LID*>((v & lsn_array_ptr_mask) << lsn_array_ptr_shift); }

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
   bool tryDiscard(LID* pending_lsn, u64 pending_lsn_count, u64 log_id, u8 discard_count)
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
      ensure_lte(discard_count, max_discard_count);
      // IMPORTANT(mfd) : Whether the page state holds one LSN or a pointer to the LSN array
      // the log_id will be stored in the 10 bits immediately after the lowest 48 bits.
      // If we will just store a single LSN we store just the offset in the lowest 45 bits.
      // This way, masking out the discard count, we can interpret the lowest 58-bits as an LSN.
      u64 new_value = state_discarded_mask | (pending_lsn_count << nb_log_records_shift) | (log_id << log_id_shift) |
                      encodeDiscardCount(discard_count);
      if (pending_lsn_count == 1) {
         ensure_equal(lsnLogID(pending_lsn[0]), log_id);
         ensure(fitsInPageState(pending_lsn[0]));
         new_value |= lsnOffset(pending_lsn[0]);
      } else {
         new_value |= encodeLSNArray(pending_lsn);
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
   // Not valid while the page is cached (HOT), use bf.header.discard_count instead.
   u8 getDiscardCount()
   {
      u64 v = raw.load(std::memory_order_acquire);
      ensure((v & state_mask) != state_hot_mask);
      return decodeDiscardCount(v);
   }
   void unlockClean(LID lsn, u64 log_id, u8 discard_count) {
      ensure(isRegularLSN(lsn));
      ensure(fitsInPageState(lsn));
      ensure_equal(lsnLogID(lsn), log_id);
      ensure_lt(log_id, max_log_id);
      ensure_lte(discard_count, max_discard_count);
      raw.store(lsn | encodeDiscardCount(discard_count) | state_clean_mask, std::memory_order_release);
   }
   void unlockBF(BufferFrame* bf)
   {
      ensure((reinterpret_cast<u64>(bf) & ~bf_ptr_mask) == 0);
      raw.store(reinterpret_cast<u64>(bf) | state_hot_mask, std::memory_order_release);
   }
   void unlock()
   {
      ensure(isLocked());
      u64 unlocked = raw.load(std::memory_order_relaxed) & latch_mask;
      raw.store(unlocked, std::memory_order_release);
   }
   void free() { raw.store(0, std::memory_order_release); }
   LID asLSN() { return raw.load(std::memory_order_acquire) & full_lsn_mask; }
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
