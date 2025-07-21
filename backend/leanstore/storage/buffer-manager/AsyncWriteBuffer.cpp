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
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, const u64 pp_id) : fd(fd), page_size(page_size), batch_max_size(batch_max_size)
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
   invalidated_per_ruh = make_unique<std::vector<s32>[]>(FLAGS_pp_threads);
   for (u64 ruh = 0; ruh < FLAGS_pp_threads; ++ruh) {
     /* Just for now assume and assert that #RUs won't exceed
     4095 to avoid dealing with corneer cases. (It will take ~5 days 
     running to reach close to ru 4000). */
     invalidated_per_ruh[ruh].resize(4096, 0);
     invalidated_per_ruh[ruh][0] = -1; // We'll use this later to identify house owners
   }
   /* Used for stats dumping. We need one per ruhs but maintain the max
   among all for simplicity.*/
   max_seen_ru = 1;
   trace_file_per_ruh = std::make_unique<std::ofstream[]>(FLAGS_pp_threads);
   for (u64 ruh = 0; ruh < FLAGS_pp_threads; ++ruh) {
     std::ostringstream oss;
     oss << "death_histogram_" << pp_id << "_ruh_"<< ruh << ".csv";
     trace_file_per_ruh[ruh].open(oss.str().c_str(), std::ios::out | std::ios::trunc);
   }
   // Each provider thread will write to it's own RUH.
   plid = plid_t(pp_id);
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
   // which RU the current valid page is 
   write_buffer_commands[slot].valid_page_in_ru = bf.page.reclaim_unit;
   // which RU will the valid page be on
   write_buffer[slot].reclaim_unit = { .ruh = plid, .ru = open_ru};
   bf.page.magic_debugging_number = pid;
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   void* write_buffer_slot_ptr = &write_buffer[slot];
   // io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pid);
   // iocbs[slot].data = write_buffer_slot_ptr;
   // iocbs_ptr[slot] = &iocbs[slot];
   struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
   ensure(sqe != nullptr);
   fdp_io_uring_prep_write(sqe, fd, write_buffer_slot_ptr, page_size, page_size * pid, plid);
   io_uring_sqe_set_data(sqe, write_buffer_slot_ptr);
   if (FLAGS_io_trace) {
      // add to trace, use tsc as timesamp
      tracing.buffer.push_back({__rdtsc(), pid, bf.page.dt_id});
   }
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::submit()
{
   static u64 tot_invalidating_writes = 0; // just for stats
   if (pending_requests > 0) {
      estimated_ruamw -= pending_requests;
      if (estimated_ruamw <= 0) {
         estimated_ruamw = ru_size;
         printf("[INFO] Estimate open new RU #%d\n", ++open_ru);
         // `remaining_valid.push_back(estimated_ruamw);
         max_seen_ru++;
         // ensure(remaining_valid.size() == u64(open_ru + 1));
      }
      ensure(estimated_ruamw > 0);
      // int ret_code = io_submit(aio_context, pending_requests, iocbs_ptr.get());
      int ret_code = io_uring_submit(&ring);
      ensure(ret_code == s32(pending_requests));
      /** Use the time after submission but before spinning for completion
      to update application level metadata for the fdp device. */
      for (u32 slot = 0; slot < pending_requests; ++slot) {
        WriteCommand &cmd = write_buffer_commands[slot];
        ReclaimUnit ru = cmd.valid_page_in_ru;
        if (ru.ru > 0) {
          ensure(ru.ruh >= 0);
          ensure(ru.ru < 4096);
          ensure(++invalidated_per_ruh[ru.ruh][ru.ru] <= ru_size);
          if (ru.ru > max_seen_ru) max_seen_ru = ru.ru;
          if (tot_invalidating_writes++ % 16 * 1048576 == 0) {
            for (u64 ruh = 0; ruh < FLAGS_pp_threads; ++ruh) {
              for(u64 i = 1; i < max_seen_ru; i++) {
                trace_file_per_ruh[ruh] << invalidated_per_ruh[ruh][i] << ",";
              }
              trace_file_per_ruh[ruh] << std::endl;
            }
          }
        }
      }
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
       ret = pending_requests;;
      // const int done_requests = io_getevents(aio_context, pending_requests, pending_requests, events.get(), NULL);
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
void AsyncWriteBuffer::getWrittenBfs(std::function<void(BufferFrame&, u64, PID)> callback, u64 n_events)
{
   // for (u64 i = 0; i < n_events; i++) {
   struct io_uring_cqe *cqe;
   unsigned head;
   u64 i = 0;
   io_uring_for_each_cqe(&ring, head, cqe) {
      const auto slot = (u64(io_uring_cqe_get_data(cqe)) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      // ensure(events[i].res == page_size);
      ensure(cqe->res == 0);
      // explainIfNot(events[i].res2 == 0);
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
