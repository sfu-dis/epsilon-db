#pragma once
#include "Exceptions.hpp"
#include "HistoryTreeInterface.hpp"
#include "Units.hpp"
#include "Logging.hpp"
#include "LogManager.hpp"
#include "Worker.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/Logger.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <functional>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
/*
  Manages a fixed number of worker threads, each one gets a partition
 */
class CRManager
{
  public:
   static constexpr u64 MAX_WORKER_THREADS = std::numeric_limits<WORKERID>::max();
   static CRManager* global;
   Worker* workers[MAX_WORKER_THREADS];
   // -------------------------------------------------------------------------------------
   std::atomic<u64> running_threads = 0;
   std::atomic<bool> keep_running = true;      // Worker threads
   std::atomic<bool> gct_keep_running = true;  // Group committer thread, stopped after all workers
   // -------------------------------------------------------------------------------------
   struct WorkerThread {
      std::mutex mutex;
      std::condition_variable cv;
      std::function<void()> job;
      bool wt_ready = true;   // Idle
      bool job_set = false;   // Has job
      bool job_done = false;  // Job done
   };
   std::vector<std::thread> worker_threads;
   WorkerThread worker_threads_meta[MAX_WORKER_THREADS];
   u32 workers_count;
   // -------------------------------------------------------------------------------------
   const s32 ssd_fd; // TODO(mfd) : Remove
   const s32 log_dev_fd;
   const u64 log_start_offset;
   const u64 log_size;
   HistoryTreeInterface& versions_space;
   // -------------------------------------------------------------------------------------
   std::unique_ptr<cr::LogManager> log_manager;
   // -------------------------------------------------------------------------------------
   std::unique_ptr<utils::Logger> logger;
   // -------------------------------------------------------------------------------------
   CRManager(HistoryTreeInterface&, s32 ssd_fd, s32 log_dev_fd, u64 log_start_offset, u64 log_size);
   ~CRManager();
   // -------------------------------------------------------------------------------------
   void registerMeAsSpecialWorker();
   // -------------------------------------------------------------------------------------
   /**
    * @brief Schedule same job on specific amount of workers.
    *
    * @param workers amount of workers
    * @param job Job to do. Same for each worker.
    */
   void scheduleJobs(u64 workers, std::function<void()> job);
   /**
    * @brief Schedule worker_id specific job on specific amount of workers.
    *
    * @param workers amount of workers
    * @param job Job to do. Different for each worker.
    */
   void scheduleJobs(u64 workers, std::function<void(u64 t_i)> job);
   /**
    * @brief Schedules one job asynchron on specific worker.
    *
    * @param t_i worker to compute job
    * @param job job
    */
   void scheduleJobAsync(u64 t_i, std::function<void()> job);
   /**
    * @brief Schedules one job on one specific worker and waits for completion.
    *
    * @param t_i worker to compute job
    * @param job job
    */
   void scheduleJobSync(u64 t_i, std::function<void()> job);
   /**
    * @brief Waits for all Workers to complete.
    *
    */
   void joinAll();
   /**
    * @brief Stops all worker threads, the group committer keeps running.
    * Idempotent.
    */
   void stopWorkerThreads();
   /**
    * @brief Stops the group committer thread. It runs one last round after all
    * workers stopped to persist any pending log records, then checks that all
    * log buffers are empty. Must be called after stopWorkerThreads(). Idempotent.
    */
   void stopGroupCommitter();
   /**
    * @brief Stops the worker threads then the group committer thread.
    */
   void stopAllThreads();
   // -------------------------------------------------------------------------------------
   // State Serialization
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);

  private:
   static std::atomic<u64> fsync_counter;
   // -------------------------------------------------------------------------------------
   void groupCommiter();
   void gctCollectAllFilledHoles();
   void gctVerifyLogBuffersAreEmpty();
   // -------------------------------------------------------------------------------------
   bool workers_stopped = false;
   bool gct_stopped = false;
   // -------------------------------------------------------------------------------------
   /**
    * @brief Set the Job to specific worker.
    *
    * @param t_i specific worker
    * @param job job
    */
   void setJob(u64 t_i, std::function<void()> job);
   /**
    * @brief Wait for one worker to complete.
    *
    * @param t_i worker_id.
    * @param condition what is the completion condition?
    */
   void joinOne(u64 t_i, std::function<bool(WorkerThread&)> condition);
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
