#include "RUEpochManagerThread.hpp"

#include "BufferManager.hpp"
#include "DTRegistry.hpp"
#include "Exceptions.hpp"
#include "RUBackgroundPageFixer.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/LogManager.hpp"
#include "leanstore/concurrency-recovery/Logging.hpp"
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
constexpr mode_t TRACE_FILE_MODE = 0666;
constexpr mode_t TRACE_DIR_MODE = 0777;
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
         regulateFreeRUEpochs();
         dispatchFullLogRUEpochsToGC();
         dispatchOldestRUEpochToGC();
         // The HOT RUH holds the self invalidating pages, its RU epochs are reclaimed inline.
         if (fdp::hot_ruh != UNMAPPED_RUH) {
            tryReclaimNonDefaultRUEpoch(fdp::hot_ruh, /* not forced */ false);
         }
      }
      // each minute
      if (FLAGS_trace_ru_threshold) {
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
   // -------------------------------------------------------------------------------------
   // Opening a new RU epoch
   // -------------------------------------------------------------------------------------
   tot_page_written[ruh] -= BufferManager::RU_SIZE;
   const ru_epoch_t new_ru_epoch = bm.ru_epoch[ruh].load(std::memory_order_relaxed) + 1;
   if (FLAGS_enable_discarding) {
      // The budget is regulated by regulateFreeRUEpochs(), this only catches its overflow.
      ensure_lte(0, freeRUEpochs());
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
   if (oldest >= newest) {
      return;
   }
   auto& set = bm.ru_discard_set[DEFAULT_RUH][oldest];
   // Its log is already reclaimed and it is undiscardable since, nothing to wait for.
   const bool log_reclaimed = set.log_reclaimed.load(std::memory_order_relaxed);
   // Being forced is not consumed here, a failed enqueue is simply retried on the next tick.
   const bool forced = oldest < force_gc_up_to;
   if (log_reclaimed || forced || set.shouldGC()) {
      if (bm.ru_bg_page_fixer->tryEnqueueRUEpoch(oldest)) {
         bm.oldest_uncollected_ru_epoch[DEFAULT_RUH]++;
      }
   }
}
// -------------------------------------------------------------------------------------
// A full log is redirected to the sink logs by the workers (see Logging::reserveLSN()), which
// disables discarding for its RU epoch. Its discarded pages are fixed right away so that the log
// space gets reclaimed, the RU epoch itself is only freed when it becomes the oldest one.
void RUEpochManager::dispatchFullLogRUEpochsToGC()
{
   if (!cr::LogManager::global->isPartitionedByRUepoch()) {
      return;
   }
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   const ru_epoch_t newest = bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   for (ru_epoch_t r = oldest; r <= newest; ++r) {
      auto& set = bm.ru_discard_set[DEFAULT_RUH][r];
      if (set.log_reclaimed.load(std::memory_order_relaxed)
          || !cr::LogManager::getLog(r).redirect_to_sink_log.load(std::memory_order_acquire)) {
         continue;
      }
      // Before enqueuing, the GC threads expect it set.
      set.log_reclaimed.store(true, std::memory_order_release);
      if (!bm.ru_bg_page_fixer->tryEnqueueRUEpoch(r, /* log_only */ true)) {
         set.log_reclaimed.store(false, std::memory_order_release);
         return;
      }
      LOG_INFO(bm.logger, "RU epoch %ld : log %u is full, dispatched to reclaim its log", r, cr::LogManager::getLogID(r));
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
         auto& set = bm.ru_discard_set[ruh].at(r);
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
      auto& set = bm.ru_discard_set[ruh].at(r);
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
// Free RU epochs left in the budget shared by all RUHs, the open RU epochs count as used.
// Negative once an RU epoch got opened while none was free.
s64 RUEpochManager::freeRUEpochs()
{
   s64 used_ru_epochs = 0;
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      used_ru_epochs += bm.ru_epoch[ruh].load(std::memory_order_relaxed) - bm.reclaimed_ru_epoch[ruh].load(std::memory_order_acquire);
   }
   return static_cast<s64>(bm.max_open_ru_epochs) - used_ru_epochs;
}
// -------------------------------------------------------------------------------------
//    free <= OP + margin : the oldest RU epochs are forced to GC.
//    free <  OP          : the writes are additionally rate limited.
void RUEpochManager::regulateFreeRUEpochs()
{
   s64 free_ru_epochs = freeRUEpochs();
   if (free_ru_epochs <= static_cast<s64>(FLAGS_overprovisioning_ru_epochs + FLAGS_ru_force_gc_margin)) {
      free_ru_epochs = forceGCOldestRUEpochs(free_ru_epochs);
   }
   updateWriteRate(free_ru_epochs);
}
// -------------------------------------------------------------------------------------
// The rate (%) is the probability for the page provider to write back a dirty page:
//    free >= OP       : 100
//    0 < free < OP    : free * 80 / OP, at least ru_rate_limit_floor
//    free <= 0        : 0, opening one more RU epoch would exceed the budget
void RUEpochManager::updateWriteRate(s64 free_ru_epochs)
{
   const s64 op = FLAGS_overprovisioning_ru_epochs;
   s64 new_rate;
   if (free_ru_epochs <= 0) {
      new_rate = 0;
   } else if (free_ru_epochs >= op) {
      new_rate = 100;
   } else {
      // cap the rate at 80% once rate limited, op > 0 here.
      new_rate = std::max<s64>((free_ru_epochs * 80) / op, std::min<s64>(FLAGS_ru_rate_limit_floor, 80));
   }
   if (static_cast<u64>(new_rate) == bm.rate.load(std::memory_order_relaxed)) {
      return;
   }
   bm.rate.store(new_rate, std::memory_order_release);
   if (new_rate == 100) {
      LOG_INFO(bm.logger, "Restored the rate of writes to 100%%, free ru epochs = %ld", free_ru_epochs);
   } else {
      LOG_WARN(bm.logger, "Updated the rate of writes to %ld%%, free ru epochs = %ld", new_rate, free_ru_epochs);
   }
}
// -------------------------------------------------------------------------------------
// Usage ((invalid + discarded) / total) of the oldest closed RU epoch of the RUH not yet handed to GC,
// -1 if there is none, i.e. only the open RU epoch is left.
double RUEpochManager::oldestUncollectedRUEpochUsage(ruh_id_t ruh)
{
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[ruh].load(std::memory_order_relaxed);
   if (oldest >= bm.ru_epoch[ruh].load(std::memory_order_relaxed)) {
      return -1;
   }
   auto& set = bm.ru_discard_set[ruh][oldest];
   const s32 total = set.total.load(std::memory_order_relaxed);
   return total == 0 ? 0 : set.ReclaimUnitUsage() * 1.0 / total;
}
// -------------------------------------------------------------------------------------
// Repeatedly frees the oldest RU epoch of the RUH with the highest usage until we are out of the forcing
// zone. A HOT RU epoch is reclaimed inline since it is merely a metadata change, the DEFAULT RUH is
// forced in batches. Returns the free RU epochs after the inline reclaims.
s64 RUEpochManager::forceGCOldestRUEpochs(s64 free_ru_epochs)
{
   /// XXX(mfd) : Only the DEFAULT_RUH and the HOT RUH are considered, the other RUHs are never forced.
   const s64 force_gc_zone = FLAGS_overprovisioning_ru_epochs + FLAGS_ru_force_gc_margin;
   while (free_ru_epochs <= force_gc_zone) {
      const double default_usage = oldestUncollectedRUEpochUsage(DEFAULT_RUH);
      const double hot_usage = (fdp::hot_ruh != UNMAPPED_RUH) ? oldestUncollectedRUEpochUsage(fdp::hot_ruh) : -1;
      if (hot_usage < 0 && default_usage < 0) {
         break;
      }
      if (hot_usage > default_usage) {
         LOG_INFO(bm.logger, "Forcing HOT RU epoch, usage %.2f%% > DEFAULT usage %.2f%%", hot_usage * 100, default_usage * 100);
         ensure(tryReclaimNonDefaultRUEpoch(fdp::hot_ruh, /* forced */ true));
         bm.bm_stats.forced_gc_count.fetch_add(1, std::memory_order_relaxed);
         free_ru_epochs++;
         continue;
      }
      // The DEFAULT RU epochs free up asynchronously, once GCed.
      forceGCDefaultRUEpochsBatch();
      break;
   }
   return free_ru_epochs;
}
// -------------------------------------------------------------------------------------
void RUEpochManager::forceGCDefaultRUEpochsBatch()
{
   // The previous batch is still being garbage collected, its RU epochs will free up the budget.
   if (bm.reclaimed_ru_epoch[DEFAULT_RUH].load(std::memory_order_acquire) + 1 < force_gc_up_to) {
      return;
   }
   // Since the previous batch is reclaimed, oldest >= force_gc_up_to.
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   const ru_epoch_t newest = bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   if (oldest >= newest) {
      // Every closed RU epoch is already dispatched to GC, only the open one is left.
      return;
   }
   // The batch covers up to the last closed RU epoch, never the open one.
   force_gc_up_to = std::min<ru_epoch_t>(oldest + FLAGS_ru_force_gc_batch, newest);
   bm.bm_stats.forced_gc_count.fetch_add(force_gc_up_to - oldest, std::memory_order_relaxed);
   LOG_WARN(bm.logger, "Forcing GC of RU epochs %ld..%ld", oldest, force_gc_up_to - 1);
}
// -------------------------------------------------------------------------------------
// Pages of a non default RUH are never fixed, the RU epoch is declared reclaimed as soon as
// it is garbage heavy enough (or forced), leaving the relocation of its valid pages to the device.
bool RUEpochManager::tryReclaimNonDefaultRUEpoch(ruh_id_t ruh, bool force_reclaim)
{
   ensure(ruh != DEFAULT_RUH);
   const ru_epoch_t oldest = bm.oldest_uncollected_ru_epoch[ruh].load(std::memory_order_relaxed);
   if (oldest >= bm.ru_epoch[ruh].load(std::memory_order_relaxed)) {
      return false;
   }
   auto& set = bm.ru_discard_set[ruh][oldest];
   if (!set.shouldGC() && !force_reclaim) {
      return false;
   }
   {
      std::lock_guard _l(set.m);
      set.resetWithoutGC();
      bm.reclaimed_ru_epoch[ruh].fetch_add(1);
   }
   bm.oldest_uncollected_ru_epoch[ruh].store(oldest + 1, std::memory_order_release);
   LOG_INFO(bm.logger, "RUH %u : Reclaimed RU epoch %ld%s", ruh, oldest, force_reclaim ? "(forced)" : "");
   return true;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
