#include "BufferFrame.hpp"
#include "leanstore/concurrency-recovery/LogManager.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
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
