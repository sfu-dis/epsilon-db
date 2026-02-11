#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>


#include <chrono>
#include <cstring>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std::chrono_literals;
namespace leanstore
{
namespace cr
{
static constexpr u64 LOG_DEV_BLK_SIZE = 4096UL;
// -------------------------------------------------------------------------------------
void CRManager::groupCommiter()
{
   using Time = decltype(std::chrono::high_resolution_clock::now());
   [[maybe_unused]] Time phase_1_begin, phase_1_end, phase_2_begin, phase_2_end, write_begin, write_end;
   // -------------------------------------------------------------------------------------
   running_threads++;
   std::string thread_name("group_committer");
   pthread_setname_np(pthread_self(), thread_name.c_str());
   CPUCounters::registerThread(thread_name, false);
   // -------------------------------------------------------------------------------------
   [[maybe_unused]] u64 round_i = 0;  // For debugging
   // -------------------------------------------------------------------------------------
   LID min_durable_gsn;
   LID prev_min_all_workers_gsn = 0;
   LID min_all_workers_gsn; 
   LID min_all_active_logs_gsn;
   LID min_all_straggler_logs_gsn;
   LID min_all_logs_gsn;
   LID max_all_logs_gsn;  // Sync all workers to this point
   TXID min_all_workers_hardened_commit_ts;
   std::vector<u64> ready_to_commit_rfa_cut;  // Exclusive ) ==
   std::vector<Logging::WorkerToLW> wt_to_lw_copy;
   std::vector<TXID> per_worker_hardened_precommit_ts;
   ready_to_commit_rfa_cut.resize(workers_count, 0);
   wt_to_lw_copy.resize(log_manager->log_count);
   per_worker_hardened_precommit_ts.resize(workers_count);
   std::vector<LID> per_log_last_seen_gsn(log_manager->log_count, 0);
   if (FLAGS_recover) {
      for (u32 log_i = 0; log_i < log_manager->log_count; ++log_i) {
         per_log_last_seen_gsn[log_i] = log_manager->meta->log_segments[log_i].hardened_gsn;
      }
   }
   // -------------------------------------------------------------------------------------
   while (keep_running) {
      // TODO(mfd) : remove restting from here to log_manager->submit
      log_manager->io_slot = 0;
      round_i++;
      CRCounters::myCounters().gct_rounds++;
      COUNTERS_BLOCK(gct_phases) { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      min_all_workers_hardened_commit_ts = std::numeric_limits<TXID>::max();
      min_all_workers_gsn = std::numeric_limits<LID>::max();
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
         Worker& worker = *workers[w_i];
         per_worker_hardened_precommit_ts[w_i] = worker.last_precommitted_tx_commit_ts.load(std::memory_order_acquire);
         min_all_workers_hardened_commit_ts = std::min<TXID>(min_all_workers_hardened_commit_ts, per_worker_hardened_precommit_ts[w_i]);
         if (FLAGS_wal_rfa) {
            std::unique_lock<instrumented_mutex> g(worker.precommitted_queue_mutex);
            ready_to_commit_rfa_cut[w_i] = worker.precommitted_queue_rfa.size();
         } else {
            ensure_equal(worker.precommitted_queue_rfa.size(), 0);
         }
         min_all_workers_gsn = std::min<LID>(min_all_workers_gsn, worker.gct_visible_worker_gsn_clock.load(std::memory_order_acquire));
      }
      ensure_lt(min_all_workers_gsn, std::numeric_limits<LID>::max());
      ensure_lte(prev_min_all_workers_gsn, min_all_workers_gsn);
      // -------------------------------------------------------------------------------------
      // The min durable gsn is the minimum gsn of all logs that have new entries and of that 
      //  of all workers. This is because any new log record that will appear in the future 
      //   in those logs will have at least the gsn of the worker with smallest gsn.
      min_durable_gsn = std::numeric_limits<LID>::max();
      min_all_active_logs_gsn = std::numeric_limits<LID>::max();
      min_all_straggler_logs_gsn = std::numeric_limits<LID>::max();
      min_all_logs_gsn = std::numeric_limits<LID>::max();
      max_all_logs_gsn = 0;
      bool straggler = false;
      // -------------------------------------------------------------------------------------
      // Phase 1
      for (u32 log_i = 0; log_i < log_manager->log_count; log_i++) {
         Logging& logging = log_manager->all_logs[log_i];
         auto& log2gct = wt_to_lw_copy[log_i] = logging.wt_to_lw.getSync();
         // -------------------------------------------------------------------------------------
         max_all_logs_gsn = std::max<LID>(max_all_logs_gsn, log2gct.last_gsn);
         if (log2gct.last_gsn == per_log_last_seen_gsn[log_i]) {
            straggler = true;
            min_all_straggler_logs_gsn = std::min<LID>(min_all_straggler_logs_gsn, log2gct.last_gsn);
            continue;
         }
         per_log_last_seen_gsn[log_i] = log2gct.last_gsn;
         min_all_active_logs_gsn = std::min<LID>(min_all_active_logs_gsn, log2gct.last_gsn);
         if (log2gct.wal_written_offset > logging.wal_gct_cursor) {
            const u64 lower_offset = utils::downAlign(logging.wal_gct_cursor, LOG_DEV_BLK_SIZE);
            const u64 upper_offset = utils::upAlign(log2gct.wal_written_offset, LOG_DEV_BLK_SIZE);
            const u64 size_aligned = upper_offset - lower_offset;
            const bool block_full = (upper_offset == log2gct.wal_written_offset);
            // -------------------------------------------------------------------------------------
            // TODO: add the concept of chunks
            log_manager->add_pwrite(log_i, lower_offset, size_aligned, block_full);
         } else if (log2gct.wal_written_offset < logging.wal_gct_cursor) {
            {
               // ------------XXXXXXXXX
               const u64 lower_offset = utils::downAlign(logging.wal_gct_cursor, LOG_DEV_BLK_SIZE);
               const u64 upper_offset = FLAGS_wal_buffer_size;
               const u64 size_aligned = upper_offset - lower_offset;
               // -------------------------------------------------------------------------------------
               log_manager->add_pwrite(log_i, lower_offset, size_aligned, true);
            }
            {
               // XXXXXX---------------
               const u64 lower_offset = 0;
               const u64 upper_offset = utils::upAlign(log2gct.wal_written_offset, LOG_DEV_BLK_SIZE);
               const u64 size_aligned = upper_offset - lower_offset;
               const bool block_full = (upper_offset == log2gct.wal_written_offset);
               // -------------------------------------------------------------------------------------
               log_manager->add_pwrite(log_i, lower_offset, size_aligned, block_full);
            }
         }
      }
      min_all_logs_gsn = std::min<LID>(min_all_straggler_logs_gsn, min_all_active_logs_gsn);
      if (min_all_active_logs_gsn == std::numeric_limits<LID>::max()) {
         // no new log records in any log
         continue;
      }
      // -------------------------------------------------------------------------------------
      // Phase 2
      COUNTERS_BLOCK(gct_phases)
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         write_begin = phase_1_end;
      }
      // -------------------------------------------------------------------------------------
      // Flush
      log_manager->submitAndWait();
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK(gct_phases)
      {
         write_end = std::chrono::high_resolution_clock::now();
         phase_2_begin = write_end;
      }
      // -------------------------------------------------------------------------------------
      for (u32 log_i = 0; log_i < log_manager->log_count; log_i++) {
         Logging& logging = log_manager->all_logs[log_i];
         logging.wal_gct_cursor.store(wt_to_lw_copy[log_i].wal_written_offset, std::memory_order_release);
         log_manager->meta->log_segments[log_i].hardened_gsn = wt_to_lw_copy[log_i].last_gsn;
         logging.hardened_gsn.store(wt_to_lw_copy[log_i].last_gsn, std::memory_order_release);
      }
      if (straggler) {
         ensure(min_all_straggler_logs_gsn != std::numeric_limits<LID>::max());
         if (min_all_straggler_logs_gsn < std::min<LID>(min_all_workers_gsn, min_all_active_logs_gsn)) {
            // Any new log records in the stragller log is garenteed to have at least
            //  the gsn of the oldest worker. Use this gsn to increase the LWM of durable
            //   GSNs. BUT!, pay attention, workers can also be straggling.
            min_durable_gsn = std::min<LID>(min_all_active_logs_gsn, min_all_workers_gsn);
         } else {
            min_durable_gsn = min_all_logs_gsn;
         }
      } else {
         ensure_equal(min_all_active_logs_gsn, min_all_logs_gsn);
         min_durable_gsn = min_all_logs_gsn;
      }
      ensure_lt(min_durable_gsn, std::numeric_limits<LID>::max());
      // Phase 2, commit
      u64 committed_tx = 0;
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) { 
         Worker& worker = *workers[w_i];
         worker.hardened_commit_ts.store(per_worker_hardened_precommit_ts[w_i], std::memory_order_release);
         TXID signaled_up_to = std::numeric_limits<TXID>::max();
         // TODO: prevent contention on mutex
         {
            std::unique_lock<instrumented_mutex> g(worker.precommitted_queue_mutex);
            // -------------------------------------------------------------------------------------
            u64 tx_i = 0;
            for (tx_i = 0;
                 tx_i < worker.precommitted_queue.size() && worker.precommitted_queue[tx_i].max_observed_gsn <= min_durable_gsn &&
                 worker.precommitted_queue[tx_i].start_ts <= min_all_workers_hardened_commit_ts;
                 tx_i++) {
               worker.precommitted_queue[tx_i].state = Transaction::STATE::COMMITTED;
            }
            if (tx_i > 0) {
               signaled_up_to = std::min<TXID>(signaled_up_to, worker.precommitted_queue[tx_i - 1].commitTS());
               worker.precommitted_queue.erase(worker.precommitted_queue.begin(), worker.precommitted_queue.begin() + tx_i);
               committed_tx += tx_i;
            }
            // -------------------------------------------------------------------------------------
            for (tx_i = 0; tx_i < ready_to_commit_rfa_cut[w_i]; tx_i++) {
               worker.precommitted_queue_rfa[tx_i].state = Transaction::STATE::COMMITTED;
            }
            if (tx_i > 0) {
               signaled_up_to = std::min<TXID>(signaled_up_to, worker.precommitted_queue_rfa[tx_i - 1].commitTS());
               worker.precommitted_queue_rfa.erase(worker.precommitted_queue_rfa.begin(),
                                                           worker.precommitted_queue_rfa.begin() + tx_i);
               committed_tx += tx_i;
            }
         }
         if (signaled_up_to < std::numeric_limits<TXID>::max() && signaled_up_to > 0) {
            worker.signaled_commit_ts.store(signaled_up_to, std::memory_order_release);
         }
      }
      CRCounters::myCounters().gct_committed_tx += committed_tx;
      COUNTERS_BLOCK(gct_phases)
      {
         phase_2_end = std::chrono::high_resolution_clock::now();
         CRCounters::myCounters().gct_phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
         CRCounters::myCounters().gct_phase_2_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end - phase_2_begin).count());
         CRCounters::myCounters().gct_write_ms += (std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_begin).count());
      }
      // -------------------------------------------------------------------------------------
      ensure_lte(Logging::global_min_gsn_flushed.load(), min_durable_gsn);
      ensure_lt(min_durable_gsn, std::numeric_limits<LID>::max());
      Logging::global_min_gsn_flushed.store(min_durable_gsn, std::memory_order_release);
      log_manager->meta->min_durable_gsn = min_durable_gsn;
      ensure(min_all_logs_gsn != std::numeric_limits<LID>::max());
      log_manager->meta->min_all_logs_gsn = min_all_logs_gsn;
      ensure(max_all_logs_gsn != 0);
      log_manager->meta->global_sync_to_this_gsn = max_all_logs_gsn;
      Logging::global_sync_to_this_gsn.store(max_all_logs_gsn, std::memory_order_release);
      log_manager->persistMetaBlock();
      // -------------------------------------------------------------------------------------
      prev_min_all_workers_gsn = min_all_workers_gsn;
   }
   running_threads--;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
