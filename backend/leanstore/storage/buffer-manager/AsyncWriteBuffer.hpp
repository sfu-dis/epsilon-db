#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <functional>
#include <list>
#include <unordered_map>
#include <fstream>
#include <mutex>

// forward declaration
// FIXME(mfd) : Quick ugly fix
struct fdp_dev;
typedef struct fdp_dev fdp_dev_t;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct Partition; // forward declaration
class AsyncWriteBuffer
{
  private:
   struct WriteCommand {
      BufferFrame* bf;
      PID pid;
      RUID last_ru_written_to;
   };
   io_context_t aio_context;
   int fd;
   u64 page_size, batch_max_size;
   u64 pending_requests = 0;
   // -------------------------------------------------------------------------------------
   // start from 1 because 0 will represent unwritten frames.
   RUID open_ru = 1;
   // XXX(mfd) : hard coded for now, later read it from the device controller
   static constexpr u64 ru_size = 3194433ULL;
   u64 estimated_ruamw = ru_size;
   std::vector<s64> remaining_valid;
   std::ofstream trace_file; // XXX(mfd) : Just temporary for tracing, remove later
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
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, fdp_dev_t *dev);
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
