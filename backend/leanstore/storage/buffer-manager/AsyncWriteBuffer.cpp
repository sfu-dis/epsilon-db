#include "AsyncWriteBuffer.hpp"
#include "Tracing.hpp"

#include "Exceptions.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>

#include <cstring>
// -------------------------------------------------------------------------------------
DEFINE_uint32(insistence_limit, 1, "");
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size) : fd(fd), page_size(page_size), batch_max_size(batch_max_size)
{
   write_buffer = make_unique<BufferFrame::Page[]>(batch_max_size);
   write_buffer_commands = make_unique<WriteCommand[]>(batch_max_size);
/*
   iocbs = make_unique<struct iocb[]>(batch_max_size);
   iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
   events = make_unique<struct io_event[]>(batch_max_size);
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   const int ret = io_setup(batch_max_size, &aio_context);
   if (ret != 0) {
      throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
   }
*/

   events = make_unique<struct io_uring_cqe*[]>(batch_max_size);
   unsigned flags = 0;
   flags |= IORING_SETUP_SQE128;
   flags |= IORING_SETUP_CQE32;
   int rc = io_uring_queue_init(2 * FLAGS_replacement_chunk_size, &ring, flags);
   if (rc != 0) {
      throw ex::GenericException("io_uring_queue_init failed, ret code = " + std::to_string(rc));
   }

}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::full()
{
   if (pending_requests >= batch_max_size - 2) {
      return true;
   } else {
      return false;
   }
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(BufferFrame& bf, PID pid)
{
   assert(!full());
   assert(u64(&bf.page) % 512 == 0);
   assert(pending_requests <= batch_max_size);
   COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_page_writes[bf.page.dt_id]++; }
   // -------------------------------------------------------------------------------------
   PARANOID_BLOCK()
   {
      if (FLAGS_pid_tracing && !FLAGS_recycle_pages) {
         Tracing::mutex.lock();
         if (Tracing::ht.contains(pid)) {
            auto& entry = Tracing::ht[pid];
            ensure(std::get<0>(entry) == bf.page.dt_id);
         }
         Tracing::mutex.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   auto slot = pending_requests++;
   write_buffer_commands[slot].bf = &bf;
   write_buffer_commands[slot].pid = pid;
   bf.page.magic_debugging_number = pid;
   auto node = reinterpret_cast<btree::BTreeNode*>(bf.page.dt);
   node->update_freq++;
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   void* write_buffer_slot_ptr = &write_buffer[slot];
   u16 plid = bf.page.fdp_plid;
   struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
   ensure(sqe != nullptr);
   fdp_io_uring_prep_write(sqe, fd, write_buffer_slot_ptr, page_size, page_size * pid, plid);
   io_uring_sqe_set_data(sqe, write_buffer_slot_ptr);
/*
   io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pid);
   iocbs[slot].data = write_buffer_slot_ptr;
   iocbs_ptr[slot] = &iocbs[slot];
*/
   if (FLAGS_io_trace) {
      // add to trace, use tsc as timesamp
      // TODO(mfd) : Use RU epochs instead of rdtsc
      tracing.buffer.push_back({__rdtsc(), pid, bf.page.dt_id});
   }
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::submit()
{
   if (pending_requests > 0) {
      // int ret_code = io_submit(aio_context, pending_requests, iocbs_ptr.get());
      int ret_code = io_uring_submit(&ring);
      ensure(ret_code == s32(pending_requests));
      return pending_requests;
   }
   // write trace to file if it's full
   if (FLAGS_io_trace && tracing.buffer.size() >= tracing.max_buffer_size) {
      tracing.writeIOTrace();
   }
   return 0;
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::pollEventsSync()
{
   u64 ret = 0;
   if (pending_requests > 0) {
      // const int done_requests = io_getevents(aio_context, pending_requests, pending_requests, events.get(), NULL);
      // if (u32(done_requests) != pending_requests) {
      ret = pending_requests;;
      const int rc = io_uring_wait_cqe_nr(&ring, events.get(), pending_requests);
      if (rc != 0) {
         cerr << rc << endl;
         // cerr << done_requests << endl;
         raise(SIGTRAP);
         ensure(false);
      }
      pending_requests = 0;
      return ret;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::getWrittenBfs(std::function<void(BufferFrame&, u64, PID)> callback, u64 n_events)
{
/*
   for (u64 i = 0; i < n_events; i++) {
      const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      ensure(events[i].res == page_size);
      explainIfNot(events[i].res2 == 0);
      auto written_lsn = write_buffer[slot].PLSN;
      callback(*write_buffer_commands[slot].bf, written_lsn, write_buffer_commands[slot].pid);
   }
*/
   struct io_uring_cqe *cqe;
   unsigned head;
   u64 i = 0;
   io_uring_for_each_cqe(&ring, head, cqe) {
      const auto slot = (u64(io_uring_cqe_get_data(cqe)) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      ensure(cqe->res == 0);
      auto written_lsn = write_buffer[slot].PLSN;
      callback(*write_buffer_commands[slot].bf, written_lsn, write_buffer_commands[slot].pid);
      ++i;
   }
   assert(i == n_events);
   io_uring_cq_advance(&ring, n_events);
}
AsyncWriteBuffer::IOTracing::IOTracing()
{
   if (FLAGS_io_trace) {
      buffer = std::vector<IOTraceEvent>(max_buffer_size);
      std::lock_guard<std::mutex> lock(mutex);
      std::ofstream trace_file;
      trace_file.open("iotrace.csv", std::ios::out | std::ios::trunc);
      trace_file << "timestamp,pid,dt_id\n";
   }
}
void AsyncWriteBuffer::IOTracing::writeIOTrace()
{
   std::lock_guard<std::mutex> lock(mutex);
   std::ofstream trace_file;
   trace_file.open(FLAGS_io_trace_file, std::ios::out | std::ios::app);
   trace_file.imbue(std::locale::classic());
   for (auto& event : buffer) {
      trace_file << event.timestamp << "," << event.pid << "," << event.dt_id << "\n";
   }
   buffer.clear();
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
