#include "RUEpochManagerThread.hpp"

#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "RUBackgroundPageFixer.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
RUEpochManager::RUEpochManager(BufferManager& bm) : bm(bm) {}
// -------------------------------------------------------------------------------------
void RUEpochManager::start()
{
   std::thread([this]() { run(); }).detach();
}
// -------------------------------------------------------------------------------------
void RUEpochManager::run()
{
   pthread_setname_np(pthread_self(), "ru_epoch_mgr");
   bm.bg_threads_counter++;
   initWriteCounters();
   FILE* tfp = fopen("thresholds.txt", "w");
   u64 milliseconds = 0;
   while (bm.bg_threads_keep_running || bm.shutdown_flush_in_progress.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      checkForNewRUEpochs();
      if (FLAGS_enable_discarding) {
         dispatchOldestRUEpochToGC();
         // Reclaim the non discardable RU Epochs inline.
         // For the first 2 RUH set up that I will use
         // This will never fire.
         // Because the other RUH contains dead pages.
      }
      // each minute
      if (FLAGS_trace_ru_threshold && FLAGS_enable_discarding) {
         if (milliseconds == (1000 * 60)) {
            milliseconds = 0;
            traceThresholds(tfp);
         }
         milliseconds += 10;
      }
   }
   // Wake up all grabage collection threads so that they could exit normally
   if (FLAGS_enable_discarding) {
      bm.ru_bg_page_fixer->wakeUpAll();
   }
   while (bm.pp_threads_counter) {
   }
   while (bm.ru_bg_page_fixer->runningThreads()) {
   }
   // No thread is able to write now
   checkForNewRUEpochs();
   // sanity check block
   {
      for (ruh_id_t ruh = 0; ruh < NUM_RUHS; ++ruh) {
         for (u64 io_handler = 0; io_handler < bm.io_writer_threads_count; ++io_handler) {
            ensure_equal(bm.writers_iostat[io_handler].io_counter[ruh], last_seen[ruh][io_handler]);
         }
      }
   }
   bm.bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
void RUEpochManager::initWriteCounters()
{
   for (ruh_id_t ruh = 0; ruh < NUM_RUHS; ++ruh) {
      tot_page_written[ruh] = 0;
      last_seen[ruh].assign(bm.io_writer_threads_count, 0);
      if (FLAGS_recover) {
         for (u32 io_handler = 0; io_handler < bm.io_writer_threads_count; ++io_handler) {
            last_seen[ruh][io_handler] = bm.writers_iostat[io_handler].io_counter[ruh].load();
            tot_page_written[ruh] += last_seen[ruh][io_handler];
         }
      }
   }
}
// -------------------------------------------------------------------------------------
void RUEpochManager::checkForNewRUEpochs()
{
   for (ruh_id_t ruh = 0; ruh < NUM_RUHS; ++ruh) {
      checkForNewRUEpoch(ruh);
   }
}
// -------------------------------------------------------------------------------------
void RUEpochManager::checkForNewRUEpoch(ruh_id_t ruh)
{
   for (u64 io_handler = 0; io_handler < bm.io_writer_threads_count; ++io_handler) {
      u64 new_value = bm.writers_iostat[io_handler].io_counter[ruh].load(std::memory_order::acquire);
      ensure(new_value >= last_seen[ruh][io_handler]);
      tot_page_written[ruh] += new_value - last_seen[ruh][io_handler];
      last_seen[ruh][io_handler] = new_value;
   }
   if (tot_page_written[ruh] < BufferManager::RU_SIZE) {
      return;
   }
   tot_page_written[ruh] -= BufferManager::RU_SIZE;
   const ru_epoch_t new_ru_epoch = bm.ru_epoch[ruh].load(std::memory_order_relaxed) + 1;
   if (FLAGS_enable_discarding) {
      // Once we almost exhausted all free RUs, we enter the steady state where we reclaim RUs continously.
      // This will only happen if the workload access pattern never reaches the threshold of RU usage.
      // When this happens the first time, I reclaim agressively the oldest 20 ru epochs. This is very
      // similar to the device GC behaviour because, after all, there is no much time for the ru usage
      // to improve the threshold.
      const s64 free_ru_epochs = freeRUEpochsAfterOpeningOne();
      // Probably too early ?
      if (free_ru_epochs < FLAGS_overprovisioning_ru_epochs) {
         // cap the new rate at 80%
         u64 new_rate = (free_ru_epochs * 80) / FLAGS_overprovisioning_ru_epochs;
         bm.rate.store(new_rate, std::memory_order_release);
         LOG_WARN(bm.logger, "Updated the rate of writes to %lu%%, free ru epochs = %ld", new_rate, free_ru_epochs);
         // TODO(mfd) : Pick the one with the highest RU Usage and free it.
         // For now I will start with the two RUH examples therefore I force only the default RUH for now.
         // Later I should also force the HOT RUH.
         // forceGCOldestRUEpochs();
      } else {
         if (bm.rate.load(std::memory_order_relaxed) < 100) {
            bm.rate.store(100, std::memory_order_release);
            LOG_INFO(bm.logger, "Restored the rate of writes to 100%%, free ru epochs = %ld", free_ru_epochs);
         }
      }
   }
   auto& set = bm.ru_discard_set[ruh].at(new_ru_epoch);
   {
      std::lock_guard _l(set.m);
      set.open(new_ru_epoch);
   }
   bm.ru_epoch[ruh].store(new_ru_epoch, std::memory_order_release);
   LOG_INFO(bm.logger, "RUH %u : Opened up a new RU Epoch %lu, closed previous ru epoch with %lu pages!!!", ruh, new_ru_epoch,
            bm.ru_discard_set[ruh][new_ru_epoch - 1].total.load());
}
// -------------------------------------------------------------------------------------
void RUEpochManager::dispatchOldestRUEpochToGC()
{
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   const ru_epoch_t newest = bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   if (oldest < newest && bm.ru_discard_set[DEFAULT_RUH][oldest].shouldGC()) {
      if (bm.ru_bg_page_fixer->tryEnqueueRUEpoch(oldest)) {
         bm.oldest_uncollected_ru_epoch[DEFAULT_RUH]++;
      }
   }
}
// -------------------------------------------------------------------------------------
void RUEpochManager::traceThresholds(FILE* tfp)
{
   for (ruh_id_t ruh = 0; ruh < NUM_RUHS; ++ruh) {
      // This method is run only by the ru_epoch_mgr thread who is responsible for incrementing
      // the oldest_uncollected_ru_epoch. That's why I don't use oldest_active_ru_epoch even
      // though it reports more information since it may be advanced concurrently by the
      // bg page fixer threads.
      ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[ruh].load(std::memory_order_relaxed);
      ru_epoch_t newest = bm.ru_epoch[ruh].load(std::memory_order_relaxed);
      fprintf(tfp, "RUH : %u, Active RUs : %u\n", ruh, newest - oldest + 1);
      for (ru_epoch_t r = oldest; r < newest; ++r) {
         // TODO(mfd) : Left to handle ru_discard_set per RUH
         auto& set = bm.ru_discard_set[ruh][r];
         // fprintf(tfp, "(%ld,%.2f)", r, set.ReclaimUnitUsage() * 100.0 / set.total.load(std::memory_order_relaxed));
         fprintf(tfp, "(%ld,%d,%.2f)", r, set.ReclaimUnitUsage() * 100.0 / set.total.load(std::memory_order_relaxed), set.invalid.load(std::memory_order_relaxed));
      }
      fprintf(tfp, "\n");
   }
   fflush(tfp);
}
// -------------------------------------------------------------------------------------
// Free RU epochs left in the budget shared by all RUHs once one more RU epoch gets opened.
s64 RUEpochManager::freeRUEpochsAfterOpeningOne()
{
   s64 used_ru_epochs = 1;
   for (ruh_id_t ruh = 0; ruh < NUM_RUHS; ++ruh) {
      used_ru_epochs += bm.ru_epoch[ruh].load(std::memory_order_relaxed) - bm.reclaimed_ru_epoch[ruh].load(std::memory_order_relaxed);
   }
   ensure_lte(used_ru_epochs, bm.max_open_ru_epochs);
   return static_cast<s64>(bm.max_open_ru_epochs) - used_ru_epochs;
}
// -------------------------------------------------------------------------------------
void RUEpochManager::forceGCOldestRUEpochs()
{
   /// XXX(mfd) : For now this will force only the DEFAULT_RUH, which is the discardable RUH.
   /// Eventually If I force GC, I need to pick the 20 oldest RU epochs from both streams.
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   const ru_epoch_t newest = bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   if (oldest >= newest) {
      // Only the open RU epoch is left, nothing to force.
      return;
   }
   auto& sets = bm.ru_discard_set[DEFAULT_RUH];
   if (!bm.global_force_gc) {
      bm.global_force_gc = true;
      const ru_epoch_t up_to = std::min<ru_epoch_t>(oldest + 20, newest - 1);
      for (ru_epoch_t r = oldest; r < up_to; ++r) {
         sets[r].force_gc = true;
         bm.bm_stats.forced_gc_count.fetch_add(1, std::memory_order_relaxed);
      }
   } else {
      sets[oldest].force_gc = true;
      bm.bm_stats.forced_gc_count.fetch_add(1, std::memory_order_relaxed);
   }
}
// -------------------------------------------------------------------------------------
// Pages of a non default RUH are never fixed, the RU epoch is declared reclaimed as soon as
// it is garbage heavy enough (or forced), leaving the relocation of its valid pages to the device.
void RUEpochManager::tryReclaimNonDefaultRUEpoch(ruh_id_t ruh)
{
   ensure(ruh != DEFAULT_RUH);
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[ruh].load(std::memory_order_relaxed);
   if (oldest >= bm.ru_epoch[ruh].load(std::memory_order_relaxed)) {
      return;
   }
   auto& set = bm.ru_discard_set[ruh][oldest];
   if (!set.shouldGC()) {
      return;
   }
   {
      std::lock_guard _l(set.m);
      set.resetWithoutGC();
      bm.reclaimed_ru_epoch[ruh].fetch_add(1);
   }
   bm.oldest_uncollected_ru_epoch[ruh].store(oldest + 1, std::memory_order_release);
   LOG_INFO(bm.logger, "RUH %u : Reclaimed RU epoch %ld", ruh, oldest);
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
