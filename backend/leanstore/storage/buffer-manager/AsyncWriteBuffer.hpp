#pragma once
#include "../btree/core/BTreeNode.hpp"
#include "BufferFrame.hpp"
#include "CompressedSpace.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <fstream>
#include <functional>
#include <list>
#include <mutex>
#include <unordered_map>
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
      LID plsn;
      ru_epoch_t ru_epoch;
   };
   struct io_uring ring;
   int fd;
   u64 page_size, batch_max_size;
   u64 pending_requests = 0;
   // -------------------------------------------------------------------------------------
   // FLAGS_compress_pages : the pages are packed into the blocks of write_buffer, each write is a block.
   std::unique_ptr<BlockPacker> packer;
   u64 submitted_commands = 0;
   u64 submitted_blocks = 0;
   // -------------------------------------------------------------------------------------
   struct IOTracing {
      struct IOTraceEvent {
         u64 timestamp;
         PID pid;
         DTID dt_id;
      };
      const u64 max_buffer_size = 10 * 1024;
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
   std::unique_ptr<struct io_uring_cqe*[]> events;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size);
   // Caller takes care of sync
   bool full();
   // Returns the PID the page is written to, a new one with FLAGS_compress_pages.
   PID add(BufferFrame& bf, PID pid);
   u64 submit();
   u64 pollEventsSync();
   void getWrittenBfs(std::function<bool(BufferFrame&, LID, ru_epoch_t)> callback, u64 n_events);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
