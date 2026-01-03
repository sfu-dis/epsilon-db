#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
struct instrumented_mutex {
   static atomic<u64> mutex_id;
   static std::unordered_map<std::string, u64> name2id;
   static std::mutex init_mutex;

   instrumented_mutex(const std::string& name)
   {
      std::lock_guard _l(init_mutex);
      if (name2id.count(name) == 0) {
         id = mutex_id.fetch_add(1);
         ensure(id < WorkerCounters::max_instrumented_mutexes);
         name2id[name] = id;
      } else {
         id = name2id[name];
      }
   }

   instrumented_mutex(const instrumented_mutex&) = delete;
   instrumented_mutex& operator=(const instrumented_mutex&) = delete;

   void lock()
   {
      WorkerCounters::myCounters().total_lock_calls[id]++;
      if (m.try_lock()) {
         return;
      }
      WorkerCounters::myCounters().contended_lock_calls[id]++;
      m.lock();
   }

   void unlock() { m.unlock(); }

  private:
   std::mutex m;
   u64 id;
};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
