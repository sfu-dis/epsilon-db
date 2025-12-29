#pragma once

#include "Logging.hpp"
#include "Units.hpp"
#include "Worker.hpp"

#include <libaio.h>

namespace leanstore
{
namespace cr
{

struct per_worker_log_segment {
   u64 start_off;
   u64 end_off;
   u64 offset;
   u64 last_start_offset;
};

struct meta_block {
   u64 number_logs;
   LID min_all_workers_gsn;
   TXID min_all_workers_hardened_commit_ts;
   struct per_worker_log_segment log_segments[0];
};

struct LogManager {
   static constexpr u64 LOG_DEV_BLK_SIZE = 4096;
   static LogManager* global;
   Logging* all_logs;
   const u32 log_count;
   const s32 log_dev_fd;
   // u64 nb_meta_blocks = 1;
   u64 log_dev_size;
   u64 log_start_offset;
   u64 log_segment_size;
   struct meta_block* meta = nullptr;
   u64 meta_size;
   // -------------------------------------------------------------------------------------
   // Async IO
   u64 batch_max_size;
   s32 io_slot = 0;
   std::unique_ptr<struct iocb[]> iocbs = make_unique<struct iocb[]>(batch_max_size);
   std::unique_ptr<struct iocb*[]> iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
   std::unique_ptr<struct io_event[]> events = make_unique<struct io_event[]>(batch_max_size);
   io_context_t aio_context;

   LogManager(u32 nb_logs, s32 log_dev_fd, u64 log_dev_size);

   // static u32 getLogID() { return Worker::my().worker_id; }

   static Logging& getLog();

   static void trimLogSegment(u32 log_segment_id)
   {
      // TODO
   }

   void add_pwrite(u32 log_i, u64 buffer_offset, u64 size, bool block_full);

   void submitAndWait();
   
   void persistMetaBlock();
};

}  // namespace cr
}  // namespace leanstore
