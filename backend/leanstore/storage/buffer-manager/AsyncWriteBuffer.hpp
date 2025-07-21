#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <functional>
#include <list>
#include <unordered_map>
#include <fdp.h>
#include <fstream>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer
{
  private:
   struct WriteCommand {
      BufferFrame* bf;
      PID pid;
      ReclaimUnit valid_page_in_ru;
   };
   // io_context_t aio_context;
   struct io_uring ring;
   int fd;
   u64 page_size, batch_max_size;
   u64 pending_requests = 0;
   // -------------------------------------------------------------------------------------
   plid_t plid = -1; // Each page provider writes to his own plid.
   // start from 1 because 0 will represent frames that are not yet persisted.
   RUID open_ru = 1;
   // XXX(mfd) : hard coded for now, later read it from the device controller
   static constexpr u64 ru_size = 3194433ULL;
   // remaining media bytes in the currently open RU.
   s64 estimated_ruamw = ru_size;
   size_t max_seen_ru;
   std::unique_ptr<std::vector<s32>[]> invalidated_per_ruh;
   std::unique_ptr<std::ofstream[]> trace_file_per_ruh; // XXX(mfd) : Just temporary for tracing, remove later
   // -------------------------------------------------------------------------------------
   struct IOTracing {
      struct IOTraceEvent{
         u64 timestamp;
         PID pid;
         DTID dt_id;
      };
      const u64 max_buffer_size = 10*1024;
      std::vector<IOTraceEvent> buffer;
      std::mutex mutex;
      IOTracing();
      void writeIOTrace();
   };
   IOTracing tracing;
   // -------------------------------------------------------------------------------------
  public:
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   // std::unique_ptr<struct iocb[]> iocbs;
   // std::unique_ptr<struct iocb*[]> iocbs_ptr;
   // std::unique_ptr<struct io_event[]> events;
   std::unique_ptr<struct io_uring_cqe *[]> events;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, const u64 pp_id);
   // Caller takes care of sync
   bool full();
   void add(BufferFrame& bf, PID pid);
   u64 submit();
   u64 pollEventsSync();
   void getWrittenBfs(std::function<void(BufferFrame&, u64, PID)> callback, u64 n_events);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
