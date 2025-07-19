#include "AsyncWriteBuffer.hpp"
#include "Tracing.hpp"
// #include "Partition.hpp"
#include "BufferManager.hpp"

#include "Exceptions.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>

#include <cstring>
// #include <fdp.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(insistence_limit, 1, "");
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, fdp_dev_t *dev) : fd(fd), page_size(page_size), batch_max_size(batch_max_size)
{
   write_buffer = make_unique<BufferFrame::Page[]>(batch_max_size);
   write_buffer_commands = make_unique<WriteCommand[]>(batch_max_size);
   iocbs = make_unique<struct iocb[]>(batch_max_size);
   iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
   events = make_unique<struct io_event[]>(batch_max_size);
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   const int ret = io_setup(batch_max_size, &aio_context);
   if (ret != 0) {
      throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
   }
   estimated_ruamw = fdp_get_remaining_bytes_in_ru(dev, 0);
   printf("AsyncWriteBuffer() : ruamw at start = %lu\n", estimated_ruamw);
   remaining_valid.reserve(65536);
   remaining_valid.push_back(estimated_ruamw);
   // register the callback here that will print to a file the status of ruhs
   // open the file here where will we write the status
   trace_file.open("death_histogram.csv", std::ios::out | std::ios::trunc);
/*
   std::thread daemon;
   daemon = std::thread( [this, &trace_file](){
     printf("[INFO] Started deamon thread for collecting global ru valid status\n");
     while (true) {
       for(const auto& ru : remaining_valid) {
         trace_file << ru << ",";
       }
       trace_file << std::endl;
       sleep(60);
     }
   });
   daemon.detach();
*/
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
   write_buffer_commands[slot].last_ru_written_to = bf.page.reclaim_unit;
   bf.page.magic_debugging_number = pid;
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   void* write_buffer_slot_ptr = &write_buffer[slot];
   io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pid);
   iocbs[slot].data = write_buffer_slot_ptr;
   iocbs_ptr[slot] = &iocbs[slot];
   if (FLAGS_io_trace) {
      // add to trace, use tsc as timesamp
      tracing.buffer.push_back({__rdtsc(), pid, bf.page.dt_id});
   }
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::submit()
{
   // determine the remaining bytes in the current ruh
   // loop through all pages in the slots and assign an ru id to them 
   static u64 tot_invalidating_writes = 0; // just for stats
   if (pending_requests > 0) {
      if (estimated_ruamw <= pending_requests) {
		estimated_ruamw = ru_size - pending_requests + estimated_ruamw;
		remaining_valid.push_back(estimated_ruamw);
		printf("[INFO] Estimate open new RU #%lu\n", ++open_ru);
        ensure(remaining_valid.size() == u64(open_ru));
      }
      for (u32 slot = 0; slot < pending_requests; ++slot) {
		write_buffer[slot].reclaim_unit = open_ru;
      }
      estimated_ruamw -= pending_requests;
      ensure(estimated_ruamw > 0);
      int ret_code = io_submit(aio_context, pending_requests, iocbs_ptr.get());
      ensure(ret_code == s32(pending_requests));
      // use the time after submission but before spinning for completion to 
      // update application level metadata for the fdp device.
      for (u32 slot = 0; slot < pending_requests; ++slot) {
        WriteCommand &cmd = write_buffer_commands[slot];
        s64 ru = cmd.last_ru_written_to;
        PID pid = cmd.pid;
        // determine from which partition the page is taken 
        // Partition &partition = BMC::global_bf->getPartition(pid);
        // partition.remove_page_from_ru(pid, ru);
        if ( ru == 0 ) {
          /** this database page is written for the first time.
          so it does not invalidate any copy. No metadata to update.*/
          static u64 new_pages = 0;
          if (++new_pages  % 1048576 == 0) printf("[INFO] %luGB Currently have been newly written\n", 4*new_pages/1048576);
        } else {
          ensure(ru <= open_ru); 
          // if (ru == open_ru) printf("[WARNING] Invalidating a page from the currently open ru\n");
          // THINK(mfd) : Could this possibly ever goes below zero ?
          --remaining_valid[ru];
          if (tot_invalidating_writes++ % 1048576 == 0) {
            for(const auto& ru : remaining_valid) {
              trace_file << ru << ",";
            }
            trace_file << std::endl;
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
   if (pending_requests > 0) {
      const int done_requests = io_getevents(aio_context, pending_requests, pending_requests, events.get(), NULL);
      if (u32(done_requests) != pending_requests) {
         cerr << done_requests << endl;
         raise(SIGTRAP);
         ensure(false);
      }
      pending_requests = 0;
      return done_requests;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::getWrittenBfs(std::function<void(BufferFrame&, u64, PID)> callback, u64 n_events)
{
   for (u64 i = 0; i < n_events; i++) {
      const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      ensure(events[i].res == page_size);
      explainIfNot(events[i].res2 == 0);
      auto written_lsn = write_buffer[slot].PLSN;
      callback(*write_buffer_commands[slot].bf, written_lsn, write_buffer_commands[slot].pid);
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
