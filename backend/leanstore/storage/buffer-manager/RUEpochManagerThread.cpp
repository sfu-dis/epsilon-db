#include "RUEpochManagerThread.hpp"

#include "BufferManager.hpp"
#include "DTRegistry.hpp"
#include "Exceptions.hpp"
#include "RUBackgroundPageFixer.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
namespace
{
constexpr mode_t TRACE_FILE_MODE = 0660;
constexpr mode_t TRACE_DIR_MODE = 0770;
void makeTraceDir(const std::string& path)
{
   posix_check(mkdir(path.c_str(), TRACE_DIR_MODE) == 0 || errno == EEXIST);
   posix_check(chmod(path.c_str(), TRACE_DIR_MODE) == 0);
}
FILE* openTraceFile(const std::string& path)
{
   const int fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, TRACE_FILE_MODE);
   posix_check(fd >= 0);
   posix_check(fchmod(fd, TRACE_FILE_MODE) == 0);
   FILE* fp = fdopen(fd, "w");
   posix_check(fp != nullptr);
   return fp;
}
}  // namespace
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
   if (FLAGS_trace_ru_threshold) {
      initThresholdsTrace();
   }
   u64 milliseconds = 0;
   while (bm.bg_threads_keep_running || bm.shutdown_flush_in_progress.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      checkForNewRUEpochs();
      if (FLAGS_enable_discarding) {
         dispatchOldestRUEpochToGC();
         // The HOT RUH holds the self invalidating pages, its RU epochs are reclaimed inline.
         // TODO(mfd) : Force reclaiming the HOT RUH as well when running out of free RU epochs.
         if (fdp::hot_ruh != UNMAPPED_RUH) {
            tryReclaimNonDefaultRUEpoch(fdp::hot_ruh);
         }
      }
      // each minute
      if (FLAGS_trace_ru_threshold && FLAGS_enable_discarding) {
         if (milliseconds == (1000 * 60)) {
            milliseconds = 0;
            traceThresholds(("minute " + std::to_string(sample_id + 1)).c_str());
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
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   // Also for the load (no discarding), covers the shutdown flush.
   if (FLAGS_trace_ru_threshold) {
      traceThresholds("shutdown");
   }
#endif
   if (summary_fp) {
      fclose(summary_fp);
      summary_fp = nullptr;
   }
   // sanity check block
   {
      for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
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
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      tot_page_written[ruh] = 0;
      last_seen[ruh].assign(bm.io_writer_threads_count, 0);
      if (!FLAGS_initialize_leanstore) {
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
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
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
// Starts from an empty THRESHOLDS_DIR so that no sample of a previous run is left behind.
void RUEpochManager::initThresholdsTrace()
{
   std::filesystem::remove_all(THRESHOLDS_DIR);
   makeTraceDir(THRESHOLDS_DIR);
   summary_fp = openTraceFile(std::string(THRESHOLDS_DIR) + "/threshold.txt");
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      makeTraceDir(std::string(THRESHOLDS_DIR) + "/ruh_" + std::to_string(ruh));
   }
#endif
}
// -------------------------------------------------------------------------------------
void RUEpochManager::traceThresholds(const char* label)
{
   const u64 sample = ++sample_id;
   FILE* tfp = summary_fp;
   fprintf(tfp, "==================== Sample %lu (%s) ====================\n", sample, label);
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      // This method is run only by the ru_epoch_mgr thread who is responsible for incrementing
      // the oldest_uncollected_ru_epoch. That's why I don't use oldest_active_ru_epoch even
      // though it reports more information since it may be advanced concurrently by the
      // bg page fixer threads.
      ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[ruh].load(std::memory_order_relaxed);
      ru_epoch_t newest = bm.ru_epoch[ruh].load(std::memory_order_relaxed);
      fprintf(tfp, "RUH %u : %ld active RUs (closed %ld..%ld, open %ld)\n", ruh, newest - oldest + 1, oldest, newest - 1, newest);
      if (oldest < newest) {
         fprintf(tfp, "   %10s %10s %10s %10s %10s\n", "RU epoch", "total", "invalid", "discarded", "usage");
      }
      for (ru_epoch_t r = oldest; r < newest; ++r) {
         auto& set = bm.ru_discard_set[ruh][r];
         // Same formula as ReclaimUnitUsage, but from a single load of each counter so that
         // invalid% + discarded% always adds up to usage%.
         const s32 total = set.total.load(std::memory_order_relaxed);
         const s32 invalid = set.invalid.load(std::memory_order_relaxed);
         const s32 discarded = set.inserted.load(std::memory_order_relaxed) - set.deleted.load(std::memory_order_relaxed);
         fprintf(tfp, "   %10ld %10d %9.2f%% %9.2f%% %9.2f%%\n", r, total, invalid * 100.0 / total, discarded * 100.0 / total,
                 (invalid + discarded) * 100.0 / total);
      }
      fprintf(tfp, "\n");
#ifdef PER_TPCC_TABLE_STATS_DEBUG
      FILE* rfp = openTraceFile(std::string(THRESHOLDS_DIR) + "/ruh_" + std::to_string(ruh) + "/sample_" + std::to_string(sample) + ".txt");
      fprintf(rfp, "Sample %lu (%s), RUH %u\n", sample, label, ruh);
      tracePerTableStats(rfp, ruh, oldest, newest);
      fclose(rfp);
#endif
   }
   fflush(tfp);
}
// -------------------------------------------------------------------------------------
#ifdef PER_TPCC_TABLE_STATS_DEBUG
// One line per table per active RU epoch (the open one included), all percentages but the relative threshold are of the RU epoch's total.
void RUEpochManager::tracePerTableStats(FILE* tfp, ruh_id_t ruh, ru_epoch_t oldest, ru_epoch_t newest)
{
   for (ru_epoch_t r = oldest; r <= newest; ++r) {
      auto& set = bm.ru_discard_set[ruh][r];
      const s32 ru_total = set.total.load(std::memory_order_relaxed);
      fprintf(tfp, "RUH %u, RU epoch %ld%s, total = %d\n", ruh, r, r == newest ? " (open)" : "", ru_total);
      if (ru_total == 0) {
         continue;
      }
      for (DTID dt_id = 0; dt_id < RUEpochDiscardSet::MAX_TRACKED_DT_ID; ++dt_id) {
         auto& t = *set.tableStats(dt_id);
         const s32 total = t.total.load(std::memory_order_relaxed);
         if (total == 0) {
            continue;
         }
         const s32 invalid = t.invalid.load(std::memory_order_relaxed);
         const s32 discarded = t.inserted.load(std::memory_order_relaxed) - t.deleted.load(std::memory_order_relaxed);
         // relative : of the table's own total in this RU epoch, global : of the RU epoch's total.
         fprintf(tfp, "   %s: %.2f%%, invalid %.2f%%, discarded %.2f%%, relative threshold %.2f%%, global threshold %.2f%%\n",
                 DTRegistry::global_dt_registry.getDTName(dt_id).c_str(), total * 100.0 / ru_total, invalid * 100.0 / ru_total,
                 discarded * 100.0 / ru_total, (invalid + discarded) * 100.0 / total, (invalid + discarded) * 100.0 / ru_total);
      }
   }
}
#endif
// -------------------------------------------------------------------------------------
// Free RU epochs left in the budget shared by all RUHs once one more RU epoch gets opened.
s64 RUEpochManager::freeRUEpochsAfterOpeningOne()
{
   s64 used_ru_epochs = 1;
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
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
