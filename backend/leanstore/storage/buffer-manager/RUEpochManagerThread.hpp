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
   std::array<u64, NUM_RUHS> tot_page_written{};
   std::array<std::vector<u64>, NUM_RUHS> last_seen;
   // -------------------------------------------------------------------------------------
   void run();
   void initWriteCounters();
   void checkForNewRUEpochs();
   void checkForNewRUEpoch(ruh_id_t ruh);
   void dispatchOldestRUEpochToGC();
   void traceThresholds(FILE* tfp);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   void tracePerTableStats(FILE* tfp, ruh_id_t ruh, ru_epoch_t oldest, ru_epoch_t newest);
#endif
   s64 freeRUEpochsAfterOpeningOne();
   void forceGCOldestRUEpochs();
   void tryReclaimNonDefaultRUEpoch(ruh_id_t ruh);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
