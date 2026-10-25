#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <memory>
#include <utility>
#include <vector>
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
   // Set by the ru_epoch_mgr once it dispatched the RU epoch because its log got full. Its log
   // is then reclaimed without freeing the RU epoch, which stays undiscardable until it is reclaimed.
   atomic<bool> log_reclaimed{false};
   // FLAGS_bg_page_fixer_variant 2: loaded from the LBA chain by the first GC thread to reach the RU epoch, the PIDs
   // written to it (offset_batch then indexes them) and the chain's blocks (PID, RU epoch they were written to).
   std::vector<PID> lba_chain_entries;
   std::vector<std::pair<PID, ru_epoch_t>> lba_chain_blocks;
   alignas(CACHE_LINE_SIZE) atomic<u64> offset_batch{0};
   // -------------------------------------------------------------------------------------
   // Logical page level: the pages written to the RU epoch (total), their copies that got rewritten elsewhere (invalid)
   // and the ones currently discarded (inserted - deleted), independent of the block they reside in.
   // They drive the bg page fixing (ReclaimUnitUsage, shouldGC).
   alignas(CACHE_LINE_SIZE) atomic<s32> inserted{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> deleted{0};
   alignas(CACHE_LINE_SIZE) atomic<bool> is_currently_being_garbage_collected{false};
   alignas(CACHE_LINE_SIZE) atomic<s32> total{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> invalid{0};
   // SSD block level, the same as the page level ones unless FLAGS_compress_pages. With compression, block_total
   // counts the 4 KiB blocks written to the RU epoch (the LBA chain blocks included), it is the one that fills the RU.
   // TODO(mfd) : with compression, block_invalid, block_inserted and block_deleted are not maintained yet, they stay 0.
   alignas(CACHE_LINE_SIZE) atomic<s32> block_inserted{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> block_deleted{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> block_total{0};
   alignas(CACHE_LINE_SIZE) atomic<s32> block_invalid{0};
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
   // Prepares the set for the in-order pass after its log got reclaimed, keeps the accounting.
   void afterLogReclaim();
   u32 ReclaimUnitUsage();
   bool shouldGC();
   void dump();

  private:
   void clear();
   // Resets the state of the GC pass only, shared by clear() and afterLogReclaim().
   void clearGCState();
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
   // The block totals then the page totals, each laid out RUH major, max_open_ru_epochs slots per RUH,
   // see blockTotal() and total().
   u32 totals[0];

   // With FLAGS_bg_page_fixer_variant 2, the totals are followed by the head of the LBA chain of each RU
   // epoch of the DEFAULT_RUH (see LBAChain), the layout of the other variants is left unchanged.
   PersistantRUState(u32 max_open_ru_epochs);
   static u64 sizeFor(u64 max_open_ru_epochs, bool with_lba_chain_heads) {
      const u64 size = chainHeadsOffset(max_open_ru_epochs);
      return with_lba_chain_heads ? size + max_open_ru_epochs * sizeof(PID) : size;
   }
   u64 getSize() const { return sizeFor(max_open_ru_epochs, FLAGS_bg_page_fixer_variant == 2); }
   // Block total of the i-th RU epoch of ruh, counted from its oldest active RU epoch.
   u32& blockTotal(ruh_id_t ruh, u64 i) {
      ensure_lt(i, max_open_ru_epochs);
      return totals[ruh * max_open_ru_epochs + i];
   }
   // Page total, same indexing.
   u32& total(ruh_id_t ruh, u64 i) {
      ensure_lt(i, max_open_ru_epochs);
      return totals[(MAX_RUHS + ruh) * max_open_ru_epochs + i];
   }
   // Head of the LBA chain of the RU epoch, INVALID_PID if none of its blocks is sealed yet.
   PID& chainHead(ru_epoch_t ru_epoch) {
      ensure(FLAGS_bg_page_fixer_variant == 2);
      return reinterpret_cast<PID*>(reinterpret_cast<u8*>(this) + chainHeadsOffset(max_open_ru_epochs))[ru_epoch % max_open_ru_epochs];
   }
   // Byte offset from the start of the state of the 4 KiB block holding the head of the RU epoch.
   u64 chainHeadBlockOffset(ru_epoch_t ru_epoch) const {
      const u64 offset = chainHeadsOffset(max_open_ru_epochs) + (ru_epoch % max_open_ru_epochs) * sizeof(PID);
      return offset - offset % 4096;
   }
   void loadFromPersistantStorage(int fd, u64 offset);
   void writetoPersistantStorage(int fd, u64 offset);
   // Writes only the 4 KiB block that holds the chain head of the RU epoch.
   void writeChainHeadToPersistantStorage(int fd, u64 offset, ru_epoch_t ru_epoch);

  private:
   static u64 chainHeadsOffset(u64 max_open_ru_epochs) {
      return utils::upAlign(sizeof(PersistantRUState) + 2 * max_open_ru_epochs * MAX_RUHS * sizeof(u32), sizeof(PID));
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
