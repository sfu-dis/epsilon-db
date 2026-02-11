#pragma once
#include "HistoryTreeInterface.hpp"
#include "Transaction.hpp"
#include "WALEntry.hpp"
#include "LogManager.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/sync-primitives/InstrumentedMutex.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct Logging;
// -------------------------------------------------------------------------------------
static constexpr u16 STATIC_MAX_WORKERS = std::numeric_limits<WORKERID>::max();
// -------------------------------------------------------------------------------------
// Abbreviations: WT (Worker Thread), GCT (Group Commit Thread or whoever writes the WAL)
// Stages: pre-committed (SI passed) -> hardened (its own WALs are written and fsync) -> committed/signaled (all dependencies are flushed too and the
// user got the OK)
struct Worker {
   // Static members
   static thread_local Worker* tls_ptr;
   // -------------------------------------------------------------------------------------
   // Concurrency Control
   static unique_ptr<atomic<u64>[]> global_workers_current_snapshot;
   static atomic<TXID> global_oldest_oltp_start_ts, global_oltp_lwm;
   static atomic<TXID> global_oldest_all_start_ts, global_all_lwm;
   static atomic<TXID> global_newest_olap_start_ts;
   static std::shared_mutex global_mutex;
   // -------------------------------------------------------------------------------------
   static constexpr u64 WORKERS_BITS = 8;
   static constexpr u64 WORKERS_INCREMENT = 1ull << WORKERS_BITS;
   static constexpr u64 WORKERS_MASK = (1ull << WORKERS_BITS) - 1;
   static constexpr u64 LATCH_BIT = (1ull << 63);
   static constexpr u64 RC_BIT = (1ull << 62);
   static constexpr u64 OLAP_BIT = (1ull << 61);
   static constexpr u64 OLTP_OLAP_SAME_BIT = OLAP_BIT;
   static constexpr u64 CLEAN_BITS_MASK = ~(LATCH_BIT | OLAP_BIT | RC_BIT);
   // TXID : [LATCH_BIT | RC_BIT | OLAP_BIT | id];
   // LWM : [LATCH_BIT | RC_BIT | OLTP_OLAP_SAME_BIT | id];
   // -------------------------------------------------------------------------------------
   // Worker Local
   struct WorkerLoggingInfo {
      LID rfa_gsn_flushed;
      bool remote_flush_dependency = false;
      // New: RFA: check for user tx dependency on tuple insert, update, lookup. Deletes are treated as system transaction
      std::vector<std::tuple<WORKERID, TXID>> rfa_checks_at_precommit;
      void checkLogDepdency(WORKERID other_worker_id, TXID other_user_tx_id)
      {
         if (FLAGS_recover)
            return;
         if (!remote_flush_dependency && Worker::my().worker_id != other_worker_id) {
            Worker* other = my().all_workers[other_worker_id];
            if (other->signaled_commit_ts < other_user_tx_id) {
               rfa_checks_at_precommit.push_back({other_worker_id, other_user_tx_id});
            }
         }
      }
   } per_worker_logging_info;
   LID worker_gsn_clock; // Will be the same as log_gsn_clock in case of per worker log.
   std::atomic<LID> gct_visible_worker_gsn_clock;
   // Shared between Group Committer and Worker
   instrumented_mutex precommitted_queue_mutex{"precommitted_queue"};
   std::vector<Transaction> precommitted_queue;
   std::vector<Transaction> precommitted_queue_rfa;
   std::atomic<TXID>  last_precommitted_tx_commit_ts = 0;
   std::atomic<TXID> hardened_commit_ts = 0, signaled_commit_ts = 0;  // W: LW, R: WT
   // -------------------------------------------------------------------------------------
   // Concurrency Control
   // LWM: start timestamp of the transaction that has its effect visible by all in its class
   struct ConcurrencyControl {
      static atomic<u64> global_clock;
      // -------------------------------------------------------------------------------------
      atomic<TXID> local_lwm_latch = 0;
      atomic<TXID> oltp_lwm_receiver;
      atomic<TXID> all_lwm_receiver;
      atomic<TXID> local_latest_write_tx = 0, local_latest_lwm_for_tx = 0;
      TXID local_all_lwm, local_oltp_lwm;
      TXID local_global_all_lwm_cache = 0;
      unique_ptr<TXID[]> local_snapshot_cache;  // = Readview
      unique_ptr<TXID[]> local_snapshot_cache_ts;
      unique_ptr<TXID[]> local_workers_start_ts;
      // -------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------
      // WiredTiger/PG/MySQL variant
      struct {
         std::unique_ptr<atomic<TXID>[]> local_workers_tx_id;  // ReadView Vector
         u64 local_workers_tx_id_cursor = 0;
         TXID current_snapshot_min_tx_id;
         TXID current_snapshot_max_tx_id;
         atomic<TXID> snapshot_min_tx_id = 0;
      } wt_pg;
      // -------------------------------------------------------------------------------------
      // LeanStore NoSteal
      // Nothing for now
      // -------------------------------------------------------------------------------------
      HistoryTreeInterface& history_tree;
      // -------------------------------------------------------------------------------------
      // Commmit Tree (single-writer multiple-reader)
      struct CommitTree {
         u64 capacity;
         std::pair<TXID, TXID>* array;
         std::shared_mutex mutex;
         u64 cursor = 0;
         void cleanIfNecessary();
         TXID commit(TXID start_ts);
         std::optional<std::pair<TXID, TXID>> LCBUnsafe(TXID start_ts);
         TXID LCB(TXID start_ts);
         CommitTree(const u64 workers_count) : capacity(workers_count + 1) { array = new std::pair<TXID, TXID>[capacity]; }
      };
      CommitTree commit_tree;
      // -------------------------------------------------------------------------------------
      // Clean up state
      u64 cleaned_untill_oltp_lwm = 0;
      // -------------------------------------------------------------------------------------
      void garbageCollection();
      void refreshGlobalState();
      void switchToReadCommittedMode();
      void switchToSnapshotIsolationMode();
      // -------------------------------------------------------------------------------------
      enum class VISIBILITY : u8 { VISIBLE_ALREADY, VISIBLE_NEXT_ROUND, UNDETERMINED };
      bool isVisibleForAll(WORKERID worker_id, TXID start_ts);
      bool isVisibleForMe(WORKERID worker_id, u64 tts, bool to_write = true);
      VISIBILITY isVisibleForIt(WORKERID whom_worker_id, WORKERID what_worker_id, u64 tts);
      VISIBILITY isVisibleForIt(WORKERID whom_worker_id, TXID commit_ts);
      TXID getCommitTimestamp(WORKERID worker_id, TXID start_ts);
      // -------------------------------------------------------------------------------------
      ConcurrencyControl& other(WORKERID other_worker_id) { return my().all_workers[other_worker_id]->cc; }
      // -------------------------------------------------------------------------------------
      inline u64 insertVersion(DTID dt_id, bool is_remove, u64 payload_length, std::function<void(u8*)> cb)
      {
         utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree_insert);
         const u64 new_command_id = (my().command_id++) | ((is_remove) ? TYPE_MSB(COMMANDID) : 0);
         history_tree.insertVersion(my().worker_id, my().active_tx.startTS(), new_command_id, dt_id, is_remove, payload_length, cb);
         return new_command_id;
      }
      inline bool retrieveVersion(WORKERID its_worker_id,
                                  TXID its_tx_id,
                                  COMMANDID its_command_id,
                                  std::function<void(const u8*, u64 payload_length)> cb)
      {
         utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree_retrieve);
         const bool is_remove = its_command_id & TYPE_MSB(COMMANDID);
         const bool found = history_tree.retrieveVersion(its_worker_id, its_tx_id, its_command_id, is_remove, cb);
         return found;
      }  // -------------------------------------------------------------------------------------
      ConcurrencyControl(HistoryTreeInterface& ht, const u64 workers_count) : history_tree(ht), commit_tree(workers_count) {}
   } cc;
   // -------------------------------------------------------------------------------------
   u64 command_id = 0;
   Transaction active_tx;
   // -------------------------------------------------------------------------------------
   const u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   const s32 ssd_fd;
   const bool is_page_provider = false;
   // -------------------------------------------------------------------------------------
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count, HistoryTreeInterface& versions_space, s32 fd, const bool is_page_provider);
   static inline Worker& my() { return *Worker::tls_ptr; }
   ~Worker();
   // -------------------------------------------------------------------------------------
   // Experiments hacks

  public:
   // -------------------------------------------------------------------------------------
   // TX Control
   void startTX(TX_MODE next_tx_type = TX_MODE::OLTP,
                TX_ISOLATION_LEVEL next_tx_isolation_level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION,
                bool read_only = false);
   void commitTX();
   void abortTX();
   void shutdown();
   inline WORKERID workerID() { return worker_id; }
   inline LID getCurrentGSN() { return worker_gsn_clock; }
   inline void setCurrentGSN(LID gsn) { worker_gsn_clock = gsn; }
   inline void syncGSN(LID other_gsn) { 
      if (other_gsn > worker_gsn_clock) {
         worker_gsn_clock = other_gsn;
      }
   }
   Logging& myLog();
};
// -------------------------------------------------------------------------------------
// Shortcuts
inline Transaction& activeTX()
{
   return cr::Worker::my().active_tx;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
