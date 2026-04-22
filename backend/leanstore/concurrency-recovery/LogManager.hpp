#pragma once
#include "Units.hpp"
#include "Worker.hpp"
// -------------------------------------------------------------------------------------
#include <libaio.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
struct BufferFrame; // Forward declaration
}
namespace cr
{
struct Logging; // Forward declaration
// -------------------------------------------------------------------------------------
struct per_worker_log_segment {
   u64 start_off;
   u64 end_off;
   u64 offset;
   u64 last_start_offset;
   LID hardened_gsn;
};
// -------------------------------------------------------------------------------------
struct meta_block {
   u64 number_logs;
   LID min_durable_gsn;
   LID min_all_logs_gsn;
   LID min_all_workers_gsn;
   LID global_sync_to_this_gsn;
   TXID min_all_workers_hardened_commit_ts;
   struct per_worker_log_segment log_segments[0];
};
// -------------------------------------------------------------------------------------
struct LogManager {
   static constexpr u64 LOG_DEV_BLK_SIZE = 4096;
   static constexpr u32 SINK_LOG_ID = 0;
   static constexpr LID NON_PERSISTED_LSN = 0x55555555;
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
   // -------------------------------------------------------------------------------------
   enum class PARTITION_BY : u8 { WORKER, PAGE, RU_EPOCH };
   PARTITION_BY partition_by;
   // -------------------------------------------------------------------------------------
   FILE* fp;

   LogManager(u32 nb_logs, s32 log_dev_fd, u64 log_dev_size);
   ~LogManager();

   static Logging& getLog(storage::BufferFrame *bf);
   static s32 getLogID(ru_epoch_t ru_epoch, PID page_id);
   static Logging& getLog(ru_epoch_t ru_epoch, PID page_id);
   // These variant of get log are to be used only when the log that the page is mapped to
   // won't change concurrently.
   static u32 getLogID(ru_epoch_t ru_epoch);
   static Logging& getLog(ru_epoch_t ru_epoch);
   u32 LSN2LogID(LID lsn);
   bool isSinkLog(u32 log_id) { return log_id < FLAGS_wal_sink_logs; }

   void resetLogSegment(s64 ru_epoch);

   void add_pwrite(u32 log_i, u64 buffer_offset, u64 size, bool block_full);

   void submitAndWait();
   
   void persistMetaBlock();

   bool isPartitionedByWorker() { return partition_by == PARTITION_BY::WORKER; }
   bool isPartitionedByPage() { return partition_by == PARTITION_BY::PAGE; }
   bool isPartitionedByRUepoch() { return partition_by == PARTITION_BY::RU_EPOCH; }
   // -------------------------------------------------------------------------------------
   struct Stats {
      atomic<u64> bytes_used = 0;
   } log_stats;
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
// -------------------------------------------------------------------------------------
