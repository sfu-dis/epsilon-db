#pragma once
#include "Macros.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <array>
#include <cstdio>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
class BufferManager;  // Forward declaration
// -------------------------------------------------------------------------------------
// Owns the ru_epoch_mgr background thread: opens a new RU epoch per RUH once RU_SIZE pages
// got written to it, rate limit the writers when running out of free RU epochs, and hands
// the oldest garbage heavy RU epochs to the background page fixer threads.
class RUEpochManager
{
  public:
   explicit RUEpochManager(BufferManager& bm);
   DO_NOT_COPY_AND_MOVE(RUEpochManager);
   // The only interface to this, the buffer manager is responsible for starting the
   // ru_epoch_mgr thread through this method. It is also responsible for stopping it.
   // This thread is the last thread that will exit the system during shutdown.
   void start();

  private:
   BufferManager& bm;
   // Per RUH: pages written in the open RU epoch, and the last seen counter of each writer thread.
   std::array<u64, MAX_RUHS> tot_page_written{};
   std::array<std::vector<u64>, MAX_RUHS> last_seen;
   // DEFAULT_RUH only: RU epochs below it are forced to GC regardless of their usage. A new batch is
   // only forced once every RU epoch of the previous one got reclaimed.
   ru_epoch_t force_gc_up_to = 0;
   // Threshold tracing: THRESHOLDS_DIR/threshold.txt holds the summary of all RUHs for every sample,
   // THRESHOLDS_DIR/ruh_<ruh>/sample_<n>.txt the per table decomposition of one RUH for sample n.
   static constexpr const char* THRESHOLDS_DIR = "thresholds";
   FILE* summary_fp = nullptr;
   u64 sample_id = 0;
   // -------------------------------------------------------------------------------------
   void run();
   void initWriteCounters();
   void checkForNewRUEpochs();
   void checkForNewRUEpoch(ruh_id_t ruh);
   void dispatchOldestRUEpochToGC();
   // Dispatches the RU epochs whose log got full so that their log space gets reclaimed.
   void dispatchFullLogRUEpochsToGC();
   void initThresholdsTrace();
   void traceThresholds(const char* label);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   void tracePerTableStats(FILE* tfp, ruh_id_t ruh, ru_epoch_t oldest, ru_epoch_t newest);
#endif
   s64 freeRUEpochs();
   void regulateFreeRUEpochs();
   void updateWriteRate(s64 free_ru_epochs);
   double oldestUncollectedRUEpochUsage(ruh_id_t ruh);
   s64 forceGCOldestRUEpochs(s64 free_ru_epochs);
   void forceGCDefaultRUEpochsBatch();
   bool tryReclaimNonDefaultRUEpoch(ruh_id_t ruh, bool force_reclaim);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
