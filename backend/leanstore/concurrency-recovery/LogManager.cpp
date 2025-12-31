#include "LogManager.hpp"

namespace leanstore
{
namespace cr
{

LogManager* LogManager::global = nullptr;

LogManager::LogManager(u32 nb_logs, s32 log_dev_fd, u64 log_dev_size)
    : log_count(nb_logs), log_dev_fd(log_dev_fd), log_dev_size(log_dev_size), batch_max_size(nb_logs * 2 + 2)
{
   all_logs = new Logging[nb_logs];
   ensure(all_logs != nullptr);
   // -------------------------------------------------------------------------------------
   meta_size = log_start_offset = utils::upAlign(sizeof(meta_block) + nb_logs * sizeof(per_worker_log_segment), LOG_DEV_BLK_SIZE);
   u8* meta_block_buffer = (u8*)aligned_alloc(4096, meta_size);
   ensure(meta_block_buffer != nullptr);
   memset(meta_block_buffer, 0, meta_size);
   meta = (struct meta_block*)meta_block_buffer;
   log_segment_size = utils::downAlign((log_dev_size - meta_size) / nb_logs, LOG_DEV_BLK_SIZE);
   // -------------------------------------------------------------------------------------
   // TODO : if we're recovering
   //   read the meta block
   //   assert that the current number of logs is the same as the one before.
   meta->number_logs = nb_logs;
   meta->min_all_workers_gsn = 0;
   meta->min_all_workers_hardened_commit_ts = 0;
   for (u32 log_i = 0; log_i < log_count; ++log_i) {
      auto* seg = &meta->log_segments[log_i];
      seg->start_off = log_start_offset + log_i * log_segment_size;
      seg->end_off = seg->start_off + log_segment_size;
      seg->offset = seg->last_start_offset = 0;
      // -------------------------------------------------------------------------------------
      auto& logging = all_logs[log_i];
      logging.log_segment_start = seg->start_off;
      logging.wal_buffer = reinterpret_cast<u8*>(std::aligned_alloc(4096, FLAGS_wal_buffer_size));
      ensure(logging.wal_buffer != nullptr);
      ensure_equal(u64(logging.wal_buffer) % 4096, 0);
      std::memset(logging.wal_buffer, 0, FLAGS_wal_buffer_size);
   }
   s64 ret = pwrite(log_dev_fd, meta_block_buffer, meta_size, /*offset*/ 0);
   ensure_equal(ret, s64(meta_size));
   // -------------------------------------------------------------------------------------
   // initialize aio context
   iocbs = make_unique<struct iocb[]>(batch_max_size);
   iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
   events = make_unique<struct io_event[]>(batch_max_size);
   {
      memset(&aio_context, 0, sizeof(aio_context));
      const int ret = io_setup(batch_max_size, &aio_context);
      if (ret != 0) {
         throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
      }
   }
}

Logging& LogManager::getLog()
{ 
    return Worker::my().myLog();
}

void LogManager::add_pwrite(u32 log_i, u64 buffer_offset, u64 size, bool block_full)
{
   ensure(size % LOG_DEV_BLK_SIZE == 0);
   if (size == 0)
      return;
   auto& lseg = meta->log_segments[log_i];
   ensure((lseg.start_off + lseg.offset) < log_dev_size);
   ensure_equal((lseg.offset % 4096), 0);
   ensure_equal((lseg.start_off % 4096), 0);
   auto& logging = all_logs[log_i];
   io_prep_pwrite(&iocbs[io_slot], log_dev_fd, logging.wal_buffer + buffer_offset, size, lseg.start_off + lseg.offset);
   iocbs[io_slot].data = logging.wal_buffer + buffer_offset;
   iocbs_ptr[io_slot] = &iocbs[io_slot];
   io_slot++;
   lseg.offset += size;
   if (!block_full) {
      lseg.offset -= LOG_DEV_BLK_SIZE;
   }
   lseg.last_start_offset = buffer_offset;
   ensure(lseg.offset < log_segment_size);
}

void LogManager::submitAndWait()
{
   u32 submitted = 0;
   u32 left = io_slot;
   while (left) {
      s32 ret_code = io_submit(aio_context, left, iocbs_ptr.get() + submitted);
      ensure_equal(ret_code, s32(io_slot));
      posix_check(ret_code >= 0);
      submitted += ret_code;
      left -= ret_code;
   }
   {
      if (io_slot > 0) {
         const s32 done_requests = io_getevents(aio_context, submitted, submitted, events.get(), NULL);
         posix_check(done_requests >= 0);
         for (s32 i = 0; i < done_requests; ++i) {
            ensure(s64(events[i].res) > 0);
            ensure_equal(events[i].res2, 0);
            if ((events[i].res % LOG_DEV_BLK_SIZE) != 0)
               cout << events[i].res << endl;
            ensure_equal((s64(events[i].res) % LOG_DEV_BLK_SIZE), 0);
         }
      }
   }
   if (FLAGS_wal_fsync) {
      fdatasync(log_dev_fd);
   }
}

void LogManager::persistMetaBlock()
{
   s64 ret = pwrite(log_dev_fd, meta, meta_size, 0);
   ensure(ret == meta_size);
   if (FLAGS_wal_fsync) {
      fdatasync(log_dev_fd);
   }
}

}  // namespace cr
}  // namespace leanstore
