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

   events = make_unique<struct io_uring_cqe*[]>(batch_max_size);
   unsigned flags = (IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
   int rc = io_uring_queue_init(2 * FLAGS_replacement_chunk_size, &ring, flags);
   if (rc != 0) {
      throw ex::GenericException("io_uring_queue_init failed, ret code = " + std::to_string(rc));
   }
   rc = io_uring_register_files(&ring, &fd, 1);
   ensure_equal(rc, 0);
   // TODO(mfd) : consider registering the write buffer arrray.
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
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().dt_page_writes[bf.page.dt_id]++;
   }
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
   if (bf.page.ru_epoch == UNMAPPED_RU_EPOCH) {
      ensure(bf.header.not_yet_persisted);
      bf.page.magic_debugging_number = pid;
   } else {
      ensure_equal(bf.page.magic_debugging_number, pid);
   }
   ++bf.page.write_back_count;
   // XXX(mfd) : Tentitavely update the RU epoch without waiting for the
   // write to return. This is to allow correct mapping to logs when the
   // page is being written back. Consider storing the tentative ru_epoch
   // in the page frame.
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   void* write_buffer_slot_ptr = &write_buffer[slot];
   struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
   ensure(sqe != nullptr);
   io_uring_prep_write(sqe, fd, write_buffer_slot_ptr, page_size, page_size * pid);
   io_uring_sqe_set_data(sqe, write_buffer_slot_ptr);
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
      ret = pending_requests;
      const int rc = io_uring_wait_cqe_nr(&ring, events.get(), pending_requests);
      if (rc != 0) {
         cerr << rc << endl;
         raise(SIGTRAP);
         ensure(false);
      }
      pending_requests = 0;
      return ret;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::getWrittenBfs(std::function<bool(BufferFrame&, LID, ru_epoch_t)> callback, u64 n_events)
{
   struct io_uring_cqe* cqe;
   unsigned head;
   u64 i = 0;
   std::vector<u64> retry_slots;
   io_uring_for_each_cqe(&ring, head, cqe)
   {
      const auto slot = (u64(io_uring_cqe_get_data(cqe)) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      ensure_equal(cqe->res, static_cast<s32>(page_size));
      auto written_plsn = write_buffer[slot].PLSN;
      u64 written_ru_epoch = write_buffer[slot].ru_epoch;
      bool success = callback(*write_buffer_commands[slot].bf, written_plsn, written_ru_epoch);
      if (!success) {
         retry_slots.push_back(slot);
      }
      ++i;
   }
   ensure_equal(i, n_events);
   io_uring_cq_advance(&ring, n_events);
   while (!retry_slots.empty()) {
      std::vector<u64> next;
      for (const auto& slot : retry_slots) {
         auto written_plsn = write_buffer[slot].PLSN;
         u64 written_ru_epoch = write_buffer[slot].ru_epoch;
         bool success = callback(*write_buffer_commands[slot].bf, written_plsn, written_ru_epoch);
         if (!success) {
            next.push_back(slot);
         }
      }
      retry_slots = next;
   }
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
