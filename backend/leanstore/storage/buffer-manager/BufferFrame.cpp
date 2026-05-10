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
   ru_epoch_t reclaiming_v2 = BMC::global_bf->reclaimed_ru_epoch.load(std::memory_order_acquire);
   if (reclaiming_v2 != reclaiming_v1 && reclaiming_v2 >= page.ru_epoch) {
      logging.mutex.unlock();
      markUnDiscardable();
      return false;
   }
   logging.walEnsureEnoughSpace(ppl.wal_entry.size);
   LID logGSN = std::max<LID>(page.GSN + 1, logging.getCurrentGSN() + 1);
   LID syncGSN = cr::Logging::global_sync_to_this_gsn.load(std::memory_order_acquire);
   if (syncGSN > logGSN) {
      logGSN = syncGSN;
   }
   page.GSN = logGSN;
   logging.setCurrentGSN(logGSN);
   LID ppl_lsn = logging.reserveLSN(ppl.wal_entry.size);
   page.last_written_lsn = ppl_lsn;
   ppl.wal_entry.lsn = ppl_lsn;
   ppl.wal_entry.prev_lsn = INVALID_LSN;
   ppl.header.gsn = logGSN;
   ppl.header.pid = header.pid;
   ppl.header.dt_id = page.dt_id;
   std::memcpy(logging.wal_buffer + logging.wal_log_cursor, &ppl, ppl.wal_entry.size);
   // use existing submitDTEntry to make the entry visible to the group committer.
   // this will release the log buffer mutex.
   logging.submitDTEntry(ppl.wal_entry.size);
   header.pending_lsn_count = 1;
   header.pending_lsn[0] = ppl_lsn;
   return true;
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
        << "  fdp_plid: " << fdp_plid << "\n"
        << "  nbfixed: " << nbfixed << "\n"
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
