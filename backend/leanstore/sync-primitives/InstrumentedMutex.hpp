#pragma once
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

    instrumented_mutex() {
        // TODO(mfd) : get a lock id 
        // map from lock name to lock id
    }
    instrumented_mutex(const instrumented_mutex&) = delete;
    instrumented_mutex& operator=(const instrumented_mutex&) = delete;

    void lock() {
        WorkerCounters::myCounters().total_lock_calls++;
        if (m_.try_lock()) {
            return;
        }
        WorkerCounters::myCounters().contended_lock_calls++;
        m_.lock();
    }

    void unlock() {
        m_.unlock();
    }

private:
   std::mutex m_;

};
// -------------------------------------------------------------------------------------
} // namespace leanstore
