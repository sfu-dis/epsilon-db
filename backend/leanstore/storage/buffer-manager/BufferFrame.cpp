#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "leanstore/concurrency-recovery/LogManager.hpp"
#include "leanstore/concurrency-recovery/Logging.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
bool BufferFrame::submitPPLEntry()
{
   ensure(FLAGS_per_page_logging);
   if (!isDiscardable())
      return false;
   ru_epoch_t reclaiming_v1 = BMC::global_bf->reclaiming_ru_epoch.load(std::memory_order_acquire);
   if (reclaiming_v1 >= page.ru_epoch) {
      // after implementing GC of pages in the buffer pool, this invariant should hold
      // ensure(reclaiming == page.ru_epoch);
      markUnDiscardable();
      return false;
   }
   ensure(header.logging != nullptr);
   auto& logging = *header.logging;
   logging.mutex.lock();
   if (logging.redirect_to_sink_log.load()) {
      logging.mutex.unlock();
      markUnDiscardable();
      return false;
   }
   ru_epoch_t reclaiming_v2 = BMC::global_bf->reclaiming_ru_epoch.load(std::memory_order_acquire);
   if (reclaiming_v2 != reclaiming_v1 && reclaiming_v2 >= page.ru_epoch) {
      logging.mutex.unlock();
      markUnDiscardable();
      return false;
   }
   ppl.header.pid = header.pid;
   ppl.header.dt_id = page.dt_id;
   ppl.absorbed_writes = header.absorbed_writes;
   ensure_lte(page.GSN, logging.getCurrentGSN());
   const LID ppl_lsn = logging.reservePPLEntry(ppl);
   page.last_written_lsn = ppl_lsn;
   // page.GSN = ppl.header.gsn;
   header.pending_lsn_count = 1;
   header.pending_lsn[0] = ppl_lsn;
   return true;
}
// -------------------------------------------------------------------------------------
bool BufferFrame::PPL::insertLogRecord(u8* log_record_buf, u32 log_record_size)
{
   ensure(FLAGS_per_page_logging);
   if (!hasSpaceFor(log_record_size)) {
      ensure(FLAGS_ppl_merge_threshold > 1);
      return false;
   }
   if (FLAGS_ppl_merge_threshold <= 1) {
      // pure ppl, not adaptive
      ensure_equal(last_entry_offset, u16(-1));
      ensure_equal(payload_size(), 0);
   }
   last_entry_offset = payload_size();
   std::memcpy(log_records + payload_size(), log_record_buf, log_record_size);
   nb_log_records += 1;
   wal_entry.size += log_record_size;
   return true;
}
// -------------------------------------------------------------------------------------
void BufferFrame::PPL::insertPPL(const PPL& other)
{
   ensure(FLAGS_per_page_logging);
   ensure(hasSpaceFor(other.payload_size()));
   // The PPL is always the first entry.
   ensure_equal(last_entry_offset, u16(-1));
   ensure_equal(payload_size(), 0);
   last_entry_offset = 0;
   std::memcpy(log_records, other.log_records, other.payload_size());
   nb_log_records = other.nb_log_records;
   wal_entry.size += other.payload_size();
   last_entry_offset = other.last_entry_offset;
}
// -------------------------------------------------------------------------------------
void BufferFrame::dump()
{
   cout << "\nBuffer Frame Dump: \n";
   header.dump();
   page.dump();
}
// -------------------------------------------------------------------------------------
void BufferFrame::Header::dump()
{
   auto state_to_string = [](STATE s) {
      switch (s) {
         case STATE::FREE:
            return "FREE";
         case STATE::HOT:
            return "HOT";
         case STATE::COOL:
            return "COOL";
         case STATE::LOADED:
            return "LOADED";
      }
      return "UNKNOWN";
   };

   cout << "=== BufferFrame::Header ===\n"
        << " last_writer_worker_id = " << +last_writer_worker_id << "\n"
        << " last_written_plsn     = " << last_written_plsn << "\n"
        << " state                 = " << state_to_string(state) << "\n"
        << " is_being_written_back = " << is_being_written_back.load() << "\n"
        << " keep_in_memory        = " << keep_in_memory << "\n"
        << " pid                   = " << pid << "\n"
        << " next_free_bf          = " << next_free_bf << "\n"
        << " logging ptr           = " << logging << "\n"
        << " flush_sink_log        = " << flush_sink_log << "\n"
        << " fixed_at_plsn         = " << fixed_at_plsn << "\n"
        << " pending_lsn_count     = " << +pending_lsn_count << "\n"
        << " discardable           = " << discardable.load() << "\n";

   if (pending_lsn_count > 0) {
      cout << " pending_lsn: ";
      for (u8 i = 0; i < pending_lsn_count; i++) {
         cout << pending_lsn[i] << " ";
      }
      cout << "\n";
   }

   cout << "===========================\n";
}
// -------------------------------------------------------------------------------------
void BufferFrame::Page::dump()
{
   cout << "Page @ " << this << "\n"
        << "  PLSN: " << PLSN << "\n"
        << "  GSN: " << GSN << "\n"
        << "  dt_id: " << dt_id << "\n"
        << "  dt_name: " << DTRegistry::global_dt_registry.getDTName(dt_id) << "\n"
        << "  magic_debugging_number: " << magic_debugging_number << "\n"
        << "  prev_ru_epoch: " << prev_ru_epoch << "\n"
        << "  ru_epoch: " << ru_epoch << "\n"
        << "  last_written_lsn: " << last_written_lsn << "\n"
        << "  belonging to log : " << cr::LogManager::global->LSN2LogID(last_written_lsn) << "\n"
        << "  log_id: " << log_id << "\n";
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
