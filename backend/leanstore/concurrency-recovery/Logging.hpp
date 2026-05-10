#pragma once

#include "Units.hpp"
#include "WALEntry.hpp"
#include "LogManager.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/OptimisticSpinStruct.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"

namespace leanstore
{
namespace cr
{

struct Worker;
struct Transaction;

struct Logging {
   static constexpr s64 CR_ENTRY_SIZE = sizeof(WALMetaEntry);
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
      inline T* operator->() { return reinterpret_cast<T*>(entry); }
      inline T& operator*() { return *reinterpret_cast<T*>(entry); }
      WALEntryHandler() = default;
      WALEntryHandler(u8* entry, u64 size, u64 lsn, u64 in_memory_offset, Logging *logging)
          : entry(entry), total_size(size), lsn(lsn), in_memory_offset(in_memory_offset), logging(logging)
      {
      }
      void submit() { logging->submitDTEntry(total_size); }
   };
   // -------------------------------------------------------------------------------------
   template <typename T>
   WALEntryHandler<T> reserveDTEntry(u64 requested_size, PID pid, LID gsn, DTID dt_id)
   {
      ensure(!redirect_to_sink_log.load());
      const u64 total_size = sizeof(WALDTEntry) + requested_size;
      const LID lsn = reserveLSN(total_size);
      active_dt_entry = new (wal_buffer + wal_log_cursor) WALDTEntry();
      active_dt_entry->lsn.store(lsn, std::memory_order_release);
      active_dt_entry->magic_debugging_number = 99;
      active_dt_entry->type = WALEntry::TYPE::DT_SPECIFIC;
      active_dt_entry->size = total_size;
      // -------------------------------------------------------------------------------------
      active_dt_entry->pid = pid;
      active_dt_entry->gsn = gsn;
      active_dt_entry->dt_id = dt_id;
      return {active_dt_entry->payload, total_size, active_dt_entry->lsn, wal_log_cursor, this};
   }
   void submitDTEntry(u64 total_size);
   // -------------------------------------------------------------------------------------
   LID reserveLSN(u64 requested_size)
   {
      ensure(is_sink_log || (wal_lsn_counter < log_segment_size));
      const auto lsn = this->log_segment_start + wal_lsn_counter;
      if (FLAGS_wal_pwrite) {
         wal_lsn_counter += requested_size;
      }
      ensure(walContiguousFreeSpace() >= requested_size);
      return lsn;
   }
   void publishOffset() { wt_to_lw.updateAttribute(&WorkerToLW::wal_written_offset, wal_log_cursor); }
   void publishMaxGSNOffset()
   {
      auto current = wt_to_lw.getNoSync();
      current.wal_written_offset = wal_log_cursor;
      current.last_gsn = log_gsn_clock;
      wt_to_lw.pushSync(current);
   }
   std::tuple<LID, u64> fetchMaxGSNOffset()
   {
      const auto current = wt_to_lw.getSync();
      return {current.last_gsn, current.wal_written_offset};
   }
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
#if 0
   Logging& other(WORKERID other_worker_id) { return Worker::my().all_workers[other_worker_id]->logging; }
#endif
};

// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
