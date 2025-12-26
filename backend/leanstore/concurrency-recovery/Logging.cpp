#include "WALEntry.hpp"
#include "Logging.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
atomic<u64> Logging::global_min_gsn_flushed = 0;
atomic<u64> Logging::global_min_commit_ts_flushed = 0;
atomic<u64> Logging::global_sync_to_this_gsn = 0;
// -------------------------------------------------------------------------------------
u32 Logging::walFreeSpace()
{
   // A , B , C : a - b + c % c
   const auto gct_cursor = wal_gct_cursor.load();
   if (gct_cursor == wal_log_cursor) {
      return FLAGS_wal_buffer_size;
   } else if (gct_cursor < wal_log_cursor) {
      return gct_cursor + (FLAGS_wal_buffer_size - wal_log_cursor);
   } else {
      return gct_cursor - wal_log_cursor;
   }
}
// -------------------------------------------------------------------------------------
u32 Logging::walContiguousFreeSpace()
{
   const auto gct_cursor = wal_gct_cursor.load();
   return (gct_cursor > wal_log_cursor) ? gct_cursor - wal_log_cursor : FLAGS_wal_buffer_size - wal_log_cursor;
}
// -------------------------------------------------------------------------------------
void Logging::walEnsureEnoughSpace(u32 requested_size)
{
   if (FLAGS_wal) {
      u32 wait_untill_free_bytes = requested_size + CR_ENTRY_SIZE;
      if ((FLAGS_wal_buffer_size - wal_log_cursor) < static_cast<u32>(requested_size + CR_ENTRY_SIZE)) {
         wait_untill_free_bytes += FLAGS_wal_buffer_size - wal_log_cursor;  // we have to skip this round
      }
      // Spin until we have enough space
      if (FLAGS_wal_variant == 2 && walFreeSpace() < wait_untill_free_bytes) {
         wt_to_lw.optimistic_latch.notify_all();
      }
      while (walFreeSpace() < wait_untill_free_bytes) {
      }
      if (walContiguousFreeSpace() < requested_size + CR_ENTRY_SIZE) {  // always keep place for CR entry
         WALMetaEntry& entry = *reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_log_cursor);
         entry.size = sizeof(WALMetaEntry);
         entry.type = WALEntry::TYPE::CARRIAGE_RETURN;
         entry.size = FLAGS_wal_buffer_size - wal_log_cursor;
         wal_lsn_counter += entry.size;
         DEBUG_BLOCK()
         {
            entry.computeCRC();
         }
         // -------------------------------------------------------------------------------------
         wal_log_cursor = 0;
         publishOffset();
         wal_next_to_clean = 0;
         wal_buffer_round++;  // Carriage Return
      }
      ensure(walContiguousFreeSpace() >= requested_size);
      ensure(wal_log_cursor + requested_size + CR_ENTRY_SIZE <= FLAGS_wal_buffer_size);
   }
}
// -------------------------------------------------------------------------------------
WALMetaEntry& Logging::reserveWALMetaEntry(WALEntry::TYPE type)
{
   ensure(type <= WALEntry::TYPE::TX_ABORT);
   walEnsureEnoughSpace(sizeof(WALMetaEntry));
   active_mt_entry = reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_log_cursor);
   active_mt_entry->type = type;
   active_mt_entry->lsn.store(this->log_segment_start + wal_lsn_counter, std::memory_order_release);
   wal_lsn_counter += sizeof(WALMetaEntry);
   active_mt_entry->size = sizeof(WALMetaEntry);
   return *active_mt_entry;
}
// -------------------------------------------------------------------------------------
void Logging::submitWALMetaEntry(u64 active_tx_start_ts)
{
   if(!((wal_log_cursor >= current_tx_wal_start) || (wal_log_cursor + sizeof(WALMetaEntry) < current_tx_wal_start))) {
      // my().active_tx.wal_larger_than_buffer = true;
      raise(SIGTRAP);
   }
   DEBUG_BLOCK()
   {
      active_mt_entry->computeCRC();
   }
   wal_log_cursor += sizeof(WALMetaEntry);
   auto current = wt_to_lw.getNoSync();
   current.wal_written_offset = wal_log_cursor;
   current.precommitted_tx_commit_ts = active_tx_start_ts;
   wt_to_lw.pushSync(current);
}
// -------------------------------------------------------------------------------------
void Logging::submitDTEntry(u64 total_size)
{
   if(!((wal_log_cursor >= current_tx_wal_start) || (wal_log_cursor + total_size  < current_tx_wal_start))) {
      // my().active_tx.wal_larger_than_buffer = true;
      raise(SIGTRAP);
   }
   DEBUG_BLOCK()
   {
      active_dt_entry->computeCRC();
   }
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().wal_write_bytes += total_size;
   }
   wal_log_cursor += total_size;
   publishMaxGSNOffset();
}
// -------------------------------------------------------------------------------------
// Called by worker, so concurrent writes on the buffer
void Logging::iterateOverCurrentTXEntries(std::function<void(const WALEntry& entry)> callback)
{
   u64 cursor = current_tx_wal_start;
   while (cursor != wal_log_cursor) {
      const WALEntry& entry = *reinterpret_cast<WALEntry*>(wal_buffer + cursor);
      ensure(entry.size > 0);
      DEBUG_BLOCK()
      {
         if (entry.type != WALEntry::TYPE::CARRIAGE_RETURN)
            entry.checkCRC();
      }
      if (entry.type == WALEntry::TYPE::CARRIAGE_RETURN) {
         cursor = 0;
      } else {
         callback(entry);
         cursor += entry.size;
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
