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
   LID min_all_workers_gsn;  // For Remote Flush Avoidance
   LID max_all_workers_gsn;  // Sync all workers to this point
   TXID min_all_workers_hardened_commit_ts;
   std::vector<u64> ready_to_commit_rfa_cut;  // Exclusive ) ==
   std::vector<Logging::WorkerToLW> wt_to_lw_copy;
   std::vector<TXID> per_worker_hardened_precommit_ts;
   ready_to_commit_rfa_cut.resize(workers_count, 0);
   wt_to_lw_copy.resize(log_manager->log_count);
   per_worker_hardened_precommit_ts.resize(workers_count);
   // -------------------------------------------------------------------------------------
   while (keep_running) {
      log_manager->io_slot = 0;
      round_i++;
      CRCounters::myCounters().gct_rounds++;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      min_all_workers_hardened_commit_ts = std::numeric_limits<TXID>::max();
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) { 
         Worker& worker = *workers[w_i];
         per_worker_hardened_precommit_ts[w_i] = worker.last_precommitted_tx_commit_ts.load(std::memory_order_acquire);
         min_all_workers_hardened_commit_ts = std::min<TXID>(min_all_workers_hardened_commit_ts, per_worker_hardened_precommit_ts[w_i]);
         {
            std::unique_lock<std::mutex> g(worker.precommitted_queue_mutex);
            ready_to_commit_rfa_cut[w_i] = worker.precommitted_queue_rfa.size();
         }
      }
      // -------------------------------------------------------------------------------------
      // TODO(mfd) : change the name from worker to log
      min_all_workers_gsn = std::numeric_limits<LID>::max();
      max_all_workers_gsn = 0;
      // -------------------------------------------------------------------------------------
      // Phase 1
      for (u32 log_i = 0; log_i < log_manager->log_count; log_i++) {
         Logging& logging = log_manager->all_logs[log_i];
         // TODO(mfd) : All this logic should be the responsability of the Log Manager
         auto& log2gct = wt_to_lw_copy[log_i] = logging.wt_to_lw.getSync();
         // -------------------------------------------------------------------------------------
         max_all_workers_gsn = std::max<LID>(max_all_workers_gsn, log2gct.last_gsn);
         min_all_workers_gsn = std::min<LID>(min_all_workers_gsn, log2gct.last_gsn);
         if (log2gct.wal_written_offset > logging.wal_gct_cursor) {
            const u64 lower_offset = utils::downAlign(logging.wal_gct_cursor, LOG_DEV_BLK_SIZE);
            const u64 upper_offset = utils::upAlign(log2gct.wal_written_offset, LOG_DEV_BLK_SIZE);
            const u64 size_aligned = upper_offset - lower_offset;
            const bool block_full = (upper_offset == log2gct.wal_written_offset);
            // -------------------------------------------------------------------------------------
            if (FLAGS_wal_pwrite) {
               // TODO: add the concept of chunks
               log_manager->add_pwrite(log_i, lower_offset, size_aligned, block_full);
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
            }
         } else if (log2gct.wal_written_offset < logging.wal_gct_cursor) {
            {
               // ------------XXXXXXXXX
               const u64 lower_offset = utils::downAlign(logging.wal_gct_cursor, LOG_DEV_BLK_SIZE);
               const u64 upper_offset = FLAGS_wal_buffer_size;
               const u64 size_aligned = upper_offset - lower_offset;
               // -------------------------------------------------------------------------------------
               if (FLAGS_wal_pwrite) {
                  log_manager->add_pwrite(log_i, lower_offset, size_aligned, true);
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
               }
            }
            {
               // XXXXXX---------------
               const u64 lower_offset = 0;
               const u64 upper_offset = utils::upAlign(log2gct.wal_written_offset, LOG_DEV_BLK_SIZE);
               const u64 size_aligned = upper_offset - lower_offset;
               const bool block_full = (upper_offset == log2gct.wal_written_offset);
               // -------------------------------------------------------------------------------------
               if (FLAGS_wal_pwrite) {
                  log_manager->add_pwrite(log_i, lower_offset, size_aligned, block_full);
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
               }
            }
         }
      }
      if (log_manager->io_slot == 0) continue;
      // -------------------------------------------------------------------------------------
      // Phase 2
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         write_begin = phase_1_end;
      }
      // -------------------------------------------------------------------------------------
      // Flush
      if (FLAGS_wal_pwrite) {
         log_manager->submitAndWait();
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK()
      {
         write_end = std::chrono::high_resolution_clock::now();
         phase_2_begin = write_end;
      }
      // -------------------------------------------------------------------------------------
      // XXX(mfd) : this could be done earlier in a callback for each completed io write
      for (u32 log_i = 0; log_i < log_manager->log_count; log_i++) {
         Logging& logging = log_manager->all_logs[log_i];
         logging.wal_gct_cursor.store(wt_to_lw_copy[log_i].wal_written_offset, std::memory_order_release);
      }
      // Phase 2, commit
      u64 committed_tx = 0;
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) { 
         Worker& worker = *workers[w_i];
         worker.hardened_commit_ts.store(per_worker_hardened_precommit_ts[w_i], std::memory_order_release);
         TXID signaled_up_to = std::numeric_limits<TXID>::max();
         // TODO: prevent contention on mutex
         {
            std::unique_lock<std::mutex> g(worker.precommitted_queue_mutex);
            // -------------------------------------------------------------------------------------
            u64 tx_i = 0;
            for (tx_i = 0;
                 tx_i < worker.precommitted_queue.size() && worker.precommitted_queue[tx_i].max_observed_gsn <= min_all_workers_gsn &&
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
      COUNTERS_BLOCK()
      {
         phase_2_end = std::chrono::high_resolution_clock::now();
         CRCounters::myCounters().gct_phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
         CRCounters::myCounters().gct_phase_2_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end - phase_2_begin).count());
         CRCounters::myCounters().gct_write_ms += (std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_begin).count());
      }
      // -------------------------------------------------------------------------------------
      assert(Logging::global_min_gsn_flushed.load() <= min_all_workers_gsn);
      Logging::global_min_gsn_flushed.store(min_all_workers_gsn, std::memory_order_release);
      Logging::global_sync_to_this_gsn.store(max_all_workers_gsn, std::memory_order_release);
      log_manager->meta->min_all_workers_gsn = min_all_workers_gsn;
      if (FLAGS_wal_pwrite) {
         log_manager->persistMetaBlock();
      }
   }
   running_threads--;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
