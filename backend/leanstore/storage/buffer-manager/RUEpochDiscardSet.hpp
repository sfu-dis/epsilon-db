#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <memory>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Per RU epoch accounting, one slot of the per RUH ring.
struct RUEpochDiscardSet {
   ruh_id_t ruh = UNMAPPED_RUH;
   u32 id = - 1;  // index of this set into the circular buffer.
   instrumented_mutex m{"ru_discard_set"};
   void* mmaped_log = nullptr;
   u64 log_segment_start = -1;
   u64 log_segment_size = -1;
   bool force_gc = false;
   alignas(CACHE_LINE_SIZE) atomic<u64> offset_batch{0};
   // -------------------------------------------------------------------------------------
   alignas(CACHE_LINE_SIZE) atomic<s32> inserted{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> deleted{0};
   alignas(CACHE_LINE_SIZE) atomic<bool> is_currently_being_garbage_collected{false};
   alignas(CACHE_LINE_SIZE) atomic<s32> total{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> invalid{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> done_gc{static_cast<s32>(FLAGS_ru_gc_threads)};
   alignas(CACHE_LINE_SIZE) atomic<s32> total_fixed{0}; // used just as a stat
   // -------------------------------------------------------------------------------------
   ru_epoch_t cur_ru_epoch = -1;
   atomic<bool> active{false};
   // -------------------------------------------------------------------------------------
   atomic<u32> undiscardable_cause_distribution[UNDISCARDABLE_CAUSE::SIZE] = {0};
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   // Per table (dt_id) breakdown of total/invalid/inserted/deleted, for debugging TPC-C.
   // Sized to the 11 TPC-C tables, larger dt_ids are not tracked.
   static constexpr DTID MAX_TRACKED_DT_ID = 11;
   struct PerTableStats {
      atomic<s32> total{0};
      atomic<s32> invalid{0};
      atomic<s32> inserted{0};
      atomic<s32> deleted{0};
   };
   PerTableStats per_table[MAX_TRACKED_DT_ID];
   PerTableStats* tableStats(DTID dt_id) { return (dt_id >= 0 && dt_id < MAX_TRACKED_DT_ID) ? &per_table[dt_id] : nullptr; }
#endif
   void open(ru_epoch_t new_ru_epoch);
   // Both resets leave advancing the RUH's reclaimed_ru_epoch to the caller.
   void reset();
   void resetWithoutGC();
   u32 ReclaimUnitUsage();
   bool shouldGC();
   void dump();

  private:
   void clear();
};
// -------------------------------------------------------------------------------------
// Ring of RUEpochDiscardSet indexed by RU epoch, one per RUH.
struct RUEpochsState {
  private:
   u32 size;
   std::unique_ptr<RUEpochDiscardSet[]> data;

  public:
   RUEpochsState(u64 size)
     : size(size), data(std::make_unique<RUEpochDiscardSet[]>(size)) {}

   /**
    * operator[] is Exact : use when we're guarentted that the ru_epoch cannot be
    * reclaimed while we're accessing it. This typically the case for the
    * background page fixing threads.
    * The at() is a non-exact variant of operator[], it is used for stats and for the ru usage.
    * The window of error is small that we allow small error.
    */
   RUEpochDiscardSet& at(size_t index) {
      return data[index % size];
   }
   const RUEpochDiscardSet& at(size_t index) const {
      return data[index % size];
   }
   RUEpochDiscardSet& operator[](size_t index) {
      ensure_equal(data[index % size].cur_ru_epoch, s64(index));
      return at(index);
   }
   const RUEpochDiscardSet& operator[](size_t index) const {
      ensure_equal(data[index % size].cur_ru_epoch, s64(index));
      return at(index);
   }
};
// -------------------------------------------------------------------------------------
// On disk RU metadata, stored right after the database pages.
struct alignas(4096) PersistantRUState {
   u64 max_open_ru_epochs; // serves as a magic debugging number also.
   std::array<ru_epoch_t, MAX_RUHS> ru_epoch;
   std::array<ru_epoch_t, MAX_RUHS> oldest_active_ru_epoch;
   std::array<ru_epoch_t, MAX_RUHS> reclaimed_ru_epoch;
   // XXX(mfd) : Persisting only totals is enough for now, we assume
   //  invalid count is always 0. In other words, we assume we're
   //   recovering from a load only workload.
   // Laid out RUH major, max_open_ru_epochs slots per RUH, see total().
   u32 totals[0];

   PersistantRUState(u32 max_open_ru_epochs);
   static u64 sizeFor(u64 max_open_ru_epochs) {
      return sizeof(PersistantRUState) + max_open_ru_epochs * MAX_RUHS * sizeof(u32);
   }
   u64 getSize() const { return sizeFor(max_open_ru_epochs); }
   // Total of the i-th RU epoch of ruh, counted from its oldest active RU epoch.
   u32& total(ruh_id_t ruh, u64 i) {
      ensure_lt(i, max_open_ru_epochs);
      return totals[ruh * max_open_ru_epochs + i];
   }
   void loadFromPersistantStorage(int fd, u64 offset);
   void writetoPersistantStorage(int fd, u64 offset);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
