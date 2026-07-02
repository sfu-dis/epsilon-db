#include "FreeList.hpp"

#include "Exceptions.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void FreeList::batchPush(BufferFrame* batch_head, BufferFrame* batch_tail, u64 batch_counter)
{
   JMUW<std::unique_lock<std::mutex>> guard(mutex);
   batch_tail->header.next_free_bf = head;
   head = batch_head;
   counter += batch_counter;
}
// -------------------------------------------------------------------------------------
void FreeList::push(BufferFrame& bf)
{
   paranoid(bf.header.state == BufferFrame::STATE::FREE);
   bf.header.latch.assertNotExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   JMUW<std::unique_lock<std::mutex>> guard(mutex);
   bf.header.next_free_bf = head;
   head = &bf;
   counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::tryPop(bool can_yield)
{
   JMUW<std::unique_lock<std::mutex>> guard(mutex);
   BufferFrame* free_bf = head;
   COUNTERS_BLOCK(failed_try_pop) { ++WorkerCounters::myCounters().total_try_pop; }
   if (head == nullptr) {
      if (can_yield) {
         if (!WorkerCounters::myCounters().waiting_for_frame) {
            // store the start time in nano seconds.
            WorkerCounters::myCounters().start_frame_wait = std::chrono::high_resolution_clock::now();
            WorkerCounters::myCounters().waiting_for_frame = true;
         }
         // yielding the CPU after 2 tries showed best behaviour.
         if (++cr::Worker::my().consecutive_failed_try_pop > 1) {
            guard->unlock();
            WorkerCounters::myCounters().yielded_cpu_no_frame++;
            sched_yield();
            cr::Worker::my().consecutive_failed_try_pop = 0;
         }
      }
      COUNTERS_BLOCK(failed_try_pop) { ++WorkerCounters::myCounters().failed_try_pop; }
      jumpmu::jump(TRY_POP);
   } else {
      if (can_yield) {
         if (WorkerCounters::myCounters().waiting_for_frame) {
            WorkerCounters::myCounters().waiting_for_frame = false;
            // calculate wait time
            auto end_wait_for_frame = std::chrono::high_resolution_clock::now();
            auto wait_frame_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_wait_for_frame - WorkerCounters::myCounters().start_frame_wait).count();
            WorkerCounters::myCounters().blocked_on_frame_ns += wait_frame_ns;
         }
         cr::Worker::my().consecutive_failed_try_pop = 0;
      }
      head = head->header.next_free_bf;
      counter--;
      paranoid(free_bf->header.state == BufferFrame::STATE::FREE);
   }
   return *free_bf;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
