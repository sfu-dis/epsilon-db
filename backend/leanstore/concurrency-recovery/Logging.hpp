#pragma once

#include "Units.hpp"
#include "WALEntry.hpp"
#include "LogManager.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/OptimisticSpinStruct.hpp"
#include "leanstore/utils/CircularQueue.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"

namespace leanstore
{
namespace cr
{

struct Worker;
struct Transaction;

struct Logging {
   static constexpr s64 CR_ENTRY_SIZE = sizeof(WALMetaEntry);
   static constexpr u64 MAX_PENDING_HOLES = 16384;
   static atomic<u64> global_min_gsn_flushed;   // The minimum of all workers maximum flushed GSN
   static atomic<u64> global_sync_to_this_gsn;  // Artifically increment the workers GSN to this point at the next round to prevent GSN from
                                                // skewing and undermining RFA
   static atomic<u64> global_min_commit_ts_flushed;
   // -------------------------------------------------------------------------------------
   u32 log_id;
   instrumented_mutex mutex{"log_buf"};
   WALMetaEntry* active_mt_entry;
   WALDTEntry* active_dt_entry;
   // -------------------------------------------------------------------------------------
   std::atomic<TXID> hardened_gsn = 0;                                // W: LW, R: LC
   // -------------------------------------------------------------------------------------
   // Protect W+GCT shared data (worker <-> group commit thread)
   struct WorkerToLW {
      u64 version = 0;
      LID last_gsn = 0;
      u64 wal_written_offset = 0;
   };
   utils::OptimisticSpinStruct<WorkerToLW> wt_to_lw;
   // -------------------------------------------------------------------------------------
   // Accessible only by the group commit thread
   u64 wal_log_cursor = 0;
   u64 wal_buffer_round = 0, wal_next_to_clean = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> wal_gct_cursor = 0;  // GCT->W
   u8* wal_buffer;    // W->GCT
   LID wal_lsn_counter = 0;
   LID log_gsn_clock;
   u64 log_segment_start = -1;
   u64 log_segment_size = -1;
   bool is_sink_log = false;
   std::atomic<bool> redirect_to_sink_log = false;
   // -------------------------------------------------------------------------------------
   struct hole_t {
      static constexpr u32 HOLE_LATCH_BIT = (u32(1) << 31);
      static_assert(HOLE_LATCH_BIT == 0x80000000, "");
      atomic<u32> log_buffer_offset;
      LID gsn;
      hole_t() : log_buffer_offset(-1), gsn(0) {}
      // always create it locked;
      hole_t(u32 offset, LID gsn) : log_buffer_offset(offset|HOLE_LATCH_BIT), gsn(gsn) {}
      void unlock()
      {
         ensure(isLocked());
         u32 unlocked = log_buffer_offset.load(std::memory_order_relaxed) & ~HOLE_LATCH_BIT;
         log_buffer_offset.store(unlocked);
      }
      bool isLocked() { return log_buffer_offset.load(std::memory_order_acquire) & HOLE_LATCH_BIT; }
   };
   utils::CircularQueue<hole_t, MAX_PENDING_HOLES> holes;
   // -------------------------------------------------------------------------------------
   // Should be called only by the group committer thread.
   void reset()
   {
      std::lock_guard _l(mutex);
      if (wal_log_cursor != wal_gct_cursor.load()) {
         cerr << "Trying to reset a log while there are some log entries in the buffer" << endl;
         raise(SIGTRAP);
      }
      redirect_to_sink_log.store(false);
      wal_lsn_counter = 0;
      wal_log_cursor = 0;
      publishOffset();
      wal_gct_cursor.store(0);
   }
   // -------------------------------------------------------------------------------------
   void collect_filled_holes(bool wait_if_none)
   {
      // I should be holding the log buffer mutex inside this method.
   retry:
      auto* last_erased_hole = holes.erase_front_while([](hole_t& hole) {
         return !hole.isLocked();
      });
      if (last_erased_hole == nullptr) {
         if (wait_if_none) {
            _mm_pause();
            goto retry;
         }
         return;
      }
      assert(last_erased_hole != nullptr);
      ensure(!holes.full());

      // notify the GCT thread with the new visible log buffer offset and its gsn.
      auto wt2gct = wt_to_lw.getNoSync();
      wt2gct.wal_written_offset = last_erased_hole->log_buffer_offset.load(std::memory_order_relaxed);
      wt2gct.last_gsn = last_erased_hole->gsn;
      wt_to_lw.pushSync(wt2gct);
   }
   // -------------------------------------------------------------------------------------
   // log buffer mutex is held during this call and no new holes can appear.
   // This will wait for all holes in the log buffer to be filled.
   void drain_holes()
   {
      if (holes.empty()) {
         LOG_INFO(LogManager::global->logger, "Log buffer does not contain any hole");
         return;
      }
      u64 stuck_counter = 0;
      u64 drained_holes_count = 0;
      auto wt2gct = wt_to_lw.getNoSync();
      for (auto it = holes.begin(); it != holes.end(); ++it) {
         while (it->isLocked()) {
            _mm_pause();
            if (++stuck_counter == 1073741824) {
               LOG_ERROR(LogManager::global->logger, "Deadlock detected in drain_holes");
               print_backtrace();
               ensure(false);
            }
         }
         wt2gct.wal_written_offset = it->log_buffer_offset.load(std::memory_order_relaxed);
         wt2gct.last_gsn = it->gsn;
         ++drained_holes_count;
      }
      wt_to_lw.pushSync(wt2gct);
      LOG_INFO(LogManager::global->logger, "Done, drained %lu holes", drained_holes_count);
   }
   // -------------------------------------------------------------------------------------
   template <typename T>
   class WALEntryHandler
   {
     public:
      u8* entry;
      u64 total_size;
      u64 lsn;
      u32 in_memory_offset;
      Logging *logging;
      hole_t* hole;
      inline T* operator->() { return reinterpret_cast<T*>(entry); }
      inline T& operator*() { return *reinterpret_cast<T*>(entry); }
      WALEntryHandler() = default;
      WALEntryHandler(u8* entry, u64 size, u64 lsn, u64 in_memory_offset, Logging *logging, hole_t* hole)
          : entry(entry), total_size(size), lsn(lsn), in_memory_offset(in_memory_offset),
            logging(logging), hole(hole)
      {
      }
      void submit()
      {
         hole->unlock();
         cr::Worker::my().publishGSN();
         logging->opportunisticCollectHoles();
      }
   };
   // -------------------------------------------------------------------------------------
   // Must be called without the log buffer mutex
   void opportunisticCollectHoles()
   {
      if (mutex.try_lock()) {
         collect_filled_holes(false /* don't block */);
         mutex.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   hole_t* insertHole()
   {
      collect_filled_holes(holes.full()); // wait if the the buffer is full.
      hole_t* hole = holes.emplace_back(wal_log_cursor, log_gsn_clock);
      ensure(hole != nullptr);
      return hole;
   }
   // -------------------------------------------------------------------------------------
   template <typename T>
   WALEntryHandler<T> reserveDTEntry(u64 requested_size, PID pid, LID gsn, DTID dt_id)
   {
      ensure(!redirect_to_sink_log.load());
      const u64 total_size = sizeof(WALDTEntry) + requested_size;
      const LID lsn = reserveLSN(total_size);
      active_dt_entry = new (wal_buffer + wal_log_cursor) WALDTEntry();
      const u32 this_entry_log_cursor = wal_log_cursor;
      wal_log_cursor += total_size;
      active_dt_entry->lsn.store(lsn, std::memory_order_release);
      active_dt_entry->magic_debugging_number = 99;
      active_dt_entry->type = WALEntry::TYPE::DT_SPECIFIC;
      active_dt_entry->size = total_size;
      // -------------------------------------------------------------------------------------
      active_dt_entry->pid = pid;
      active_dt_entry->gsn = gsn;
      active_dt_entry->dt_id = dt_id;
      // -------------------------------------------------------------------------------------
      auto* hole = insertHole();
      return {active_dt_entry->payload, total_size, active_dt_entry->lsn, this_entry_log_cursor, this, hole};
   }
   // -------------------------------------------------------------------------------------
   LID reservePPLEntry(storage::BufferFrame::PPL& ppl)
   {
      walEnsureEnoughSpace(ppl.wal_entry.size);
      log_gsn_clock++;
      const LID ppl_lsn = reserveLSN(ppl.wal_entry.size);
      ppl.wal_entry.lsn = ppl_lsn;
      ppl.wal_entry.prev_lsn = INVALID_LSN;
      ppl.header.gsn = log_gsn_clock;
      const u32 this_entry_log_cursor = wal_log_cursor;
      wal_log_cursor += ppl.wal_entry.size;
      // It is guarentted that the log GSN >= pageGSN.
      auto* hole = insertHole();
      mutex.unlock();
      std::memcpy(wal_buffer + this_entry_log_cursor, &ppl, ppl.wal_entry.size);
      hole->unlock();
      opportunisticCollectHoles();
      return ppl_lsn;
   }
   // -------------------------------------------------------------------------------------
   u64 logSegmentFreeSpace()
   {
      return log_segment_size - wal_lsn_counter;
   }
   // -------------------------------------------------------------------------------------
   LID reserveLSN(u64 requested_size)
   {
      ensure(is_sink_log || (wal_lsn_counter < log_segment_size));
      const auto lsn = this->log_segment_start + wal_lsn_counter;
      if (FLAGS_wal_pwrite) {
         wal_lsn_counter += requested_size;
      }
      ensure(walContiguousFreeSpace() >= requested_size);
      // XXX(mfd): This is an "unwanted" byproduct of static partitioning the log device into
      //  a fixed set of log segments. If more traffic goes to a single log segment than we
      //   close it and disable discarding of the corresponding ru epoch.
      //    All previous WAL traffic is now redirected to the sink logs.
      if (!is_sink_log && (logSegmentFreeSpace() <= FLAGS_wal_buffer_size)) {
         redirect_to_sink_log.store(true, std::memory_order_release);
         // need to drain all holes here.
         drain_holes();
         // After draining all holes, no more log records will appear in this log until it is reclaimed.
         // TODO(mfd) : Optinally just garbage collect the corresponding RU epoch.
         LOG_WARN(LogManager::global->logger, "Log %u is full, disable discarding for it and redirect the log entries to sink logs", log_id);

      }
      return lsn;
   }
   // Should only be used when resetting the log segment.
   void publishOffset() { wt_to_lw.updateAttribute(&WorkerToLW::wal_written_offset, wal_log_cursor); }
#if 0
   void publishMaxGSNOffset()
   {
      auto current = wt_to_lw.getNoSync();
      current.wal_written_offset = wal_log_cursor;
      current.last_gsn = log_gsn_clock;
      wt_to_lw.pushSync(current);
   }
#endif
   // -------------------------------------------------------------------------------------
   u32 walFreeSpace();
   u32 walContiguousFreeSpace();
   void walEnsureEnoughSpace(u32 requested_size);
   u8* walReserve(u32 requested_size);
   // -------------------------------------------------------------------------------------
   // Iterate over current TX entries
   u64 current_tx_wal_start;
   void iterateOverCurrentTXEntries(std::function<void(const WALEntry& entry)> callback);
   // -------------------------------------------------------------------------------------
   // Without Payload, by submit no need to update clock (gsn)
   WALMetaEntry& reserveWALMetaEntry(WALEntry::TYPE type);
   void submitWALMetaEntry(u64 active_tx_start_ts);
   inline LID getCurrentGSN() { return log_gsn_clock; }
   inline void setCurrentGSN(LID gsn) { log_gsn_clock = gsn; }
   inline void syncGSN(LID other_gsn) {
      if (other_gsn > log_gsn_clock) {
         log_gsn_clock = other_gsn;
      }
   }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
