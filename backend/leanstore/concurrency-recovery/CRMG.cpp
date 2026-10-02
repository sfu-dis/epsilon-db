#include "CRMG.hpp"

#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <chrono>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
// Threads id order: workers (xN) -> Group Committer Thread (x1) -> Page Provider Threads (xP)
CRManager* CRManager::global = nullptr;
std::atomic<u64> CRManager::fsync_counter = 0;
// -------------------------------------------------------------------------------------
CRManager::CRManager(HistoryTreeInterface& versions_space, s32 ssd_fd, s32 log_dev_fd, u64 log_start_offset, u64 log_size)
    : ssd_fd(ssd_fd), log_dev_fd(log_dev_fd), log_start_offset(log_start_offset),
      log_size(log_size), versions_space(versions_space)
{
   logger = std::make_unique<utils::Logger>("cr_manager_journal.txt");
   workers_count = FLAGS_worker_threads;
   ensure(workers_count < MAX_WORKER_THREADS);
   // -------------------------------------------------------------------------------------
   Worker::global_workers_current_snapshot = std::make_unique<atomic<u64>[]>(workers_count);
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      u32 log_count = FLAGS_wal_partition_by == "worker" ? workers_count : FLAGS_wal_partitions_count;
      log_manager = std::make_unique<cr::LogManager>(log_count, log_dev_fd, log_start_offset, log_size);
   } else {
      log_manager = nullptr;
   }
   cr::LogManager::global = log_manager.get();
   // -------------------------------------------------------------------------------------
   worker_threads.reserve(workers_count);
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      worker_threads.emplace_back([&, t_i]() {
         std::string thread_name("worker_" + std::to_string(t_i));
         pthread_setname_np(pthread_self(), thread_name.c_str());
         if (FLAGS_pin_threads) {
            utils::pinThisThread(t_i);
         }
         // -------------------------------------------------------------------------------------
         if (FLAGS_cpu_counters) {
            CPUCounters::registerThread(thread_name, false);
         }
         WorkerCounters::myCounters().worker_id = t_i;
         CRCounters::myCounters().worker_id = t_i;
         // -------------------------------------------------------------------------------------
         workers[t_i] = new Worker(t_i, workers, workers_count, versions_space, ssd_fd, log_dev_fd, false);
         Worker::tls_ptr = workers[t_i];
         // -------------------------------------------------------------------------------------
         running_threads++;
         while (running_threads != (workers_count + FLAGS_wal))
            ;
         auto& meta = worker_threads_meta[t_i];
         while (keep_running) {
            std::unique_lock guard(meta.mutex);
            meta.cv.wait(guard, [&]() { return keep_running == false || meta.job_set; });
            if (!keep_running) {
               break;
            }
            meta.wt_ready = false;
            meta.job();
            meta.wt_ready = true;
            meta.job_done = true;
            meta.job_set = false;
            meta.cv.notify_one();
         }
         running_threads--;
      });
   }
   for (auto& t : worker_threads) {
      t.detach();
   }
   // -------------------------------------------------------------------------------------
   // Wait until all worker threads are initialized
   while (running_threads < workers_count) {
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      std::thread group_commiter([&]() {
         if (FLAGS_pin_threads) {
            utils::pinThisThread(workers_count);
         }
         groupCommiter();
      });
      group_commiter.detach();
   }
}
// -------------------------------------------------------------------------------------
void CRManager::registerMeAsSpecialWorker()
{
   cr::Worker::tls_ptr = new Worker(std::numeric_limits<WORKERID>::max(), workers, workers_count, versions_space, ssd_fd, -1, true);
}
// -------------------------------------------------------------------------------------
void CRManager::scheduleJobSync(u64 t_i, std::function<void()> job)
{
   setJob(t_i, job);
   joinOne(t_i, [&](WorkerThread& meta) { return meta.job_done; });
}
// -------------------------------------------------------------------------------------
void CRManager::scheduleJobAsync(u64 t_i, std::function<void()> job)
{
   setJob(t_i, job);
}
// -------------------------------------------------------------------------------------
void CRManager::scheduleJobs(u64 workers, std::function<void()> job)
{
   for (u32 t_i = 0; t_i < workers; t_i++) {
      setJob(t_i, job);
   }
}
void CRManager::scheduleJobs(u64 workers, std::function<void(u64 t_i)> job)
{
   for (u32 t_i = 0; t_i < workers; t_i++) {
      setJob(t_i, [=]() { return job(t_i); });
   }
}

// -------------------------------------------------------------------------------------
void CRManager::joinAll()
{
   for (u32 t_i = 0; t_i < workers_count; t_i++) {
      joinOne(t_i, [&](WorkerThread& meta) { return meta.wt_ready && !meta.job_set; });
   }
}
// -------------------------------------------------------------------------------------
void CRManager::setJob(u64 t_i, std::function<void()> job)
{
   ensure(t_i < workers_count);
   auto& meta = worker_threads_meta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return !meta.job_set && meta.wt_ready; });
   meta.job_set = true;
   meta.job_done = false;
   meta.job = job;
   guard.unlock();
   meta.cv.notify_one();
}
// -------------------------------------------------------------------------------------
void CRManager::joinOne(u64 t_i, std::function<bool(WorkerThread&)> condition)
{
   ensure(t_i < workers_count);
   auto& meta = worker_threads_meta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return condition(meta); });
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> CRManager::serialize()
{
   std::unordered_map<std::string, std::string> map;
   map["global_logical_clock"] = std::to_string(Worker::ConcurrencyControl::global_clock.load());
   return map;
}
// -------------------------------------------------------------------------------------
void CRManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   Worker::ConcurrencyControl::global_clock = std::stol(map["global_logical_clock"]);
   Worker::global_all_lwm = std::stol(map["global_logical_clock"]);
}
// -------------------------------------------------------------------------------------
void CRManager::stopWorkerThreads()
{
   if (workers_stopped) {
      LOG_DEBUG(logger, "Worker threads already stopped, nothing to do");
      return;
   }
   LOG_INFO(logger, "Stopping %u worker threads (running threads incl. group committer: %lu)", workers_count, running_threads.load());
   keep_running.store(false, std::memory_order_release);
   // Grab each worker mutex before notifying so that a worker that is about to
   // wait on its cv cannot miss the wakeup.
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      auto& meta = worker_threads_meta[t_i];
      {
         std::lock_guard guard(meta.mutex);
      }
      meta.cv.notify_all();
   }
   LOG_INFO(logger, "Notified all worker threads, waiting for them to exit");
   // The group committer is still running and counted in running_threads.
   const u64 gct_threads = (FLAGS_wal && !gct_stopped) ? 1 : 0;
   auto last_report = std::chrono::steady_clock::now();
   while (running_threads > gct_threads) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      if (std::chrono::steady_clock::now() - last_report >= std::chrono::seconds(1)) {
         last_report = std::chrono::steady_clock::now();
         LOG_WARN(logger, "Still waiting for %lu worker threads to finish their current job", running_threads.load() - gct_threads);
      }
   }
   workers_stopped = true;
   LOG_INFO(logger, "All %u worker threads stopped", workers_count);
}
// -------------------------------------------------------------------------------------
void CRManager::stopGroupCommitter()
{
   if (!FLAGS_wal) {
      LOG_INFO(logger, "WAL is disabled, no group committer thread to stop");
      gct_stopped = true;
      return;
   }
   if (gct_stopped) {
      LOG_DEBUG(logger, "Group committer thread already stopped, nothing to do");
      return;
   }
   // The last group commit round must see all the log records, so no worker can be running.
   ensure(workers_stopped);
   ensure_equal(running_threads.load(), 1);
   LOG_INFO(logger, "Stopping the group committer thread, it will run one last round to persist pending log records");
   gct_keep_running.store(false, std::memory_order_release);
   // The group committer logs its last round, do not interleave logs with it.
   while (running_threads) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
   }
   gct_stopped = true;
   LOG_INFO(logger, "Group committer thread stopped, durable GSN = %lu", Logging::global_min_gsn_flushed.load());
}
// -------------------------------------------------------------------------------------
void CRManager::stopAllThreads()
{
   LOG_INFO(logger, "Stopping all CRManager threads");
   stopWorkerThreads();
   stopGroupCommitter();
   ensure_equal(running_threads.load(), 0);
   LOG_INFO(logger, "All CRManager threads stopped");
}
// -------------------------------------------------------------------------------------
CRManager::~CRManager()
{
   if (!workers_stopped || !gct_stopped) {
      LOG_WARN(logger, "CRManager destroyed before its threads were stopped, stopping them now");
      stopAllThreads();
   }
   // The group committer reads the workers, they can only be freed once it exited.
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      delete workers[t_i];
   }
   LOG_INFO(logger, "Deleted %u workers, CRManager destroyed", workers_count);
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
