#include "Logging.hpp"

#include "LogManager.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"

namespace leanstore
{
namespace cr
{

LogManager* LogManager::global = nullptr;

LogManager::LogManager(u32 nb_logs, s32 log_dev_fd, u64 log_dev_size)
    : log_count(nb_logs), log_dev_fd(log_dev_fd), log_dev_size(log_dev_size), batch_max_size(nb_logs * 2 + 2)
{
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal_partition_by == "worker") {
      partition_by = PARTITION_BY::WORKER;
   } else if (FLAGS_wal_partition_by == "page") {
      partition_by = PARTITION_BY::PAGE;
   } else if (FLAGS_wal_partition_by == "ru_epoch") {
      partition_by = PARTITION_BY::RU_EPOCH;
   } else {
      throw std::invalid_argument("FLAGS_wal_partition_by");
   }
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
   logger = std::make_unique<utils::Logger>("log_manager_journal.txt");
   // -------------------------------------------------------------------------------------
   const bool recover_wal = FLAGS_recover && !FLAGS_clean_recover;
   if (recover_wal) {
      s64 ret = pread(log_dev_fd, meta_block_buffer, meta_size, 0);
      ensure_equal(ret, s64(meta_size));
      ensure_equal(meta->number_logs, nb_logs);
      Logging::global_min_gsn_flushed.store(meta->min_durable_gsn);
      LID sync_point = std::max<LID>(meta->global_sync_to_this_gsn, meta->min_all_workers_gsn);
      Logging::global_sync_to_this_gsn.store(sync_point);
      LOG_INFO(logger, "Recovering min all logs gsn %lu", meta->min_durable_gsn);
      LOG_INFO(logger, "Recovering max all logs gsn %lu", meta->global_sync_to_this_gsn);
      // TX timestamp need not be recovered!
   } else {
      meta->number_logs = nb_logs;
      meta->min_durable_gsn = 0;
      meta->min_all_logs_gsn = 0;
      meta->min_all_workers_gsn = 0;
      meta->global_sync_to_this_gsn = 0;
      meta->min_all_workers_hardened_commit_ts = 0;
   }
   for (u32 log_i = 0; log_i < log_count; ++log_i) {
      auto* seg = &meta->log_segments[log_i];
      if (!recover_wal) {
         seg->start_off = log_start_offset + log_i * log_segment_size;
         seg->end_off = seg->start_off + log_segment_size;
         seg->offset = seg->last_start_offset = 0;
         seg->hardened_gsn = 0;
      } else {
         ensure_equal(seg->start_off, log_start_offset + log_i * log_segment_size);
         ensure_equal(seg->end_off, seg->start_off + log_segment_size);
         // FIXME(mfd) : This is not strictly necessary for experimenting
         // This is mainly for supporting crash recovery or normal recovery with discarding
         // enabled, which we do not support both for now.
         // ensure_equal(seg->offset, 0);
      }
      // -------------------------------------------------------------------------------------
      auto& logging = all_logs[log_i];
      logging.log_id = log_i;
      logging.log_segment_start = seg->start_off;
      logging.log_segment_size = log_segment_size;
      logging.wal_lsn_counter = recover_wal ? seg->offset : 0;
      logging.log_gsn_clock = recover_wal ? (seg->hardened_gsn) : 0;
      logging.wt_to_lw.current_value.last_gsn = logging.hardened_gsn = logging.log_gsn_clock;
      logging.is_sink_log = isSinkLog(log_i);
      logging.wal_buffer = reinterpret_cast<u8*>(std::aligned_alloc(4096, FLAGS_wal_buffer_size));
      ensure(logging.wal_buffer != nullptr);
      ensure_equal(u64(logging.wal_buffer) % 4096, 0);
      std::memset(logging.wal_buffer, 0, FLAGS_wal_buffer_size);
   }
   if (!recover_wal) {
      s64 ret = pwrite(log_dev_fd, meta_block_buffer, meta_size, /*offset*/ 0);
      ensure_equal(ret, s64(meta_size));
   }
   // -------------------------------------------------------------------------------------
   // initialize aio context
   // TODO(mfd) : use io_uring here instead and register the log file descriptor and all log buffers.
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

u32 LogManager::LSN2LogID(LID lsn)
{
   if ((lsn == INVALID_LSN) || (lsn == NON_PERSISTED_LSN)) return -1;
   LID aligned_lsn = utils::downAlign(lsn, LOG_DEV_BLK_SIZE);
   u32 log_id = (aligned_lsn - meta_size) / log_segment_size;
   {
      ensure_lt(log_id, global->log_count);
      ensure_equal(global->all_logs[log_id].log_id, log_id);
   }
   return log_id;
}

u32 LogManager::getLogID(ru_epoch_t ru_epoch)
{
   ensure(global->isPartitionedByRUepoch());
   return (ru_epoch % (global->log_count-FLAGS_wal_sink_logs)) + FLAGS_wal_sink_logs;
}

s32 LogManager::getLogID(ru_epoch_t ru_epoch, PID page_id)
{
   ensure(global->isPartitionedByRUepoch());
   s32 log_id = -1;
   if ((ru_epoch == -1)
      || (ru_epoch <= storage::BMC::global_bf->reclaiming_ru_epoch.load(std::memory_order_acquire))) {
      log_id = page_id % FLAGS_wal_sink_logs;
   } else {
      log_id = getLogID(ru_epoch); 
      if (global->all_logs[log_id].redirect_to_sink_log.load(std::memory_order_acquire)) {
         log_id = page_id % FLAGS_wal_sink_logs;
      }
   }
   return log_id;
}

Logging& LogManager::getLog(s64 ru_epoch)
{
   u32 log_id = getLogID(ru_epoch);
   return global->all_logs[log_id];
}

Logging& LogManager::getLog(s64 ru_epoch, PID page_id)
{
   s32 log_id = getLogID(ru_epoch, page_id);
   ensure(log_id != -1);
   return global->all_logs[log_id];
}

Logging& LogManager::getLog(storage::BufferFrame *bf)
{
   s32 log_id = -1;
   if (global->isPartitionedByWorker()) {
      log_id = Worker::my().worker_id;
   } else if (global->isPartitionedByRUepoch()) {
      if (bf->header.not_yet_persisted || !bf->isDiscardable()) {
         log_id = bf->header.pid % FLAGS_wal_sink_logs;
      } else {
         return getLog(bf->page.ru_epoch, bf->header.pid);
      }
   } else {
      log_id = bf->header.pid % global->log_count;
   }
   ensure(log_id != -1);
   return global->all_logs[log_id];
}

void LogManager::resetLogSegment(s64 ru_epoch)
{
   ensure(global->isPartitionedByRUepoch());
   ensure(ru_epoch >= 0);
   u32 log_id = getLogID(ru_epoch);
   auto& lseg = global->meta->log_segments[log_id];
   LOG_INFO(logger, "Reclaiming log of RU epoch %ld mapped to %u", ru_epoch, log_id);
   LOG_INFO(logger, "Log space consumption was %.1f%% when trimming log %u", lseg.offset * 100.0f/log_segment_size, log_id);
   LOG_INFO(logger, "%.4f GiB Log space", lseg.offset/ 1073741824.0);
   COUNTERS_BLOCK(log_space_usage)
   {
      log_stats.bytes_used.fetch_sub(lseg.offset);
   }
   lseg.offset = 0;
}

void LogManager::add_pwrite(u32 log_i, u64 buffer_offset, u64 size, bool block_full)
{
   if (!FLAGS_wal_pwrite) return;
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
   COUNTERS_BLOCK(log_space_usage)
   {
      //FIXME(mfd) : Do not account for writes to the sink log for now
      // until we implement recovery.
      if (!isSinkLog(log_i)) {
         u64 effective_size = size;
         if (!block_full) effective_size -= LOG_DEV_BLK_SIZE;
         log_stats.bytes_used.fetch_add(effective_size, std::memory_order_release);
      }
   }
   lseg.last_start_offset = buffer_offset;
   if (lseg.offset >= log_segment_size) {
      // FIXME(mfd) : Either proper checkpointing of the sink log or obviate it.
      // For now, no writes go to the sink log.
      ensure(log_i != 0);
      cerr << "Log space is not enough!!!" << endl;
      raise(SIGTRAP);
   }
   if (!block_full) {
      lseg.offset -= LOG_DEV_BLK_SIZE;
   }
   COUNTERS_BLOCK(gct_write_bytes) { CRCounters::myCounters().gct_write_bytes += size; }
}

void LogManager::submitAndWait()
{
   if (!FLAGS_wal_pwrite) return;
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
   if (!FLAGS_wal_pwrite) return;
   s64 ret = pwrite(log_dev_fd, meta, meta_size, 0);
   ensure_equal(ret, s64(meta_size));
   COUNTERS_BLOCK(gct_write_bytes) { CRCounters::myCounters().gct_write_bytes += meta_size; }
   if (FLAGS_wal_fsync) {
      fdatasync(log_dev_fd);
   }
}

LogManager::~LogManager()
{
    // ensure background threads are stopped ?
    persistMetaBlock();
}

}  // namespace cr
}  // namespace leanstore
