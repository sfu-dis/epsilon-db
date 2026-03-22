#include "BufferFrame.hpp"
#include "leanstore/concurrency-recovery/LogManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void BufferFrame::Page::dump()
{
   cout << "Page @ " << this << "\n"
        << "  PLSN: " << PLSN << "\n"
        << "  GSN: " << GSN << "\n"
        << "  dt_id: " << dt_id << "\n"
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
