#include "RUBackgroundPageFixer.hpp"

#include "BufferManager.hpp"
#include "CustomSlabAllocator.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency-recovery/LogManager.hpp"
#include "leanstore/concurrency-recovery/Logging.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/profiling/counters/GCCounters.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/CrashLogger.hpp"
// -------------------------------------------------------------------------------------
#include <liburing.h>
#include <pthread.h>

#include <thread>
// -------------------------------------------------------------------------------------
/** Used as a sanity check. It traverses the log records of a page from the last LSN applied
 to a page following a prev_lsn pointer. The prev_lsn of the log record that first made
 the page dirty is a special NULL lsn (INVALID_LSN). */
#define DEBUG_BACKWARD_LOG_CHAIN_TRAVERSE 0
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct page_info {
   PID pid;
   LID* lsn_list;
   LID first_lsn;
   LID last_lsn;
   u8 nb_log_records;

   void dump() const
   {
      std::cout << "page_info {\n";
      std::cout << "   pid            = " << pid << "\n";
      std::cout << "   first_lsn      = " << first_lsn << "\n";
      std::cout << "   last_lsn       = " << last_lsn << "\n";
      std::cout << "   nb_log_records = " << +nb_log_records << "\n";

      std::cout << "   lsn_list       = [";
      for (u8 i = 0; i < nb_log_records; i++) {
         std::cout << lsn_list[i];
         if (i + 1 < nb_log_records)
            std::cout << ", ";
      }
      std::cout << "]\n";
      std::cout << "}\n";
   }
};
// -------------------------------------------------------------------------------------
struct RUBackgroundPageFixer::ThreadContext {
   // FIXME(mfd): Temporely set it large enough
   static constexpr u32 batch_size = 256;
   // -------------------------------------------------------------------------------------
   const u32 gc_id;
   BufferFrame buf_pages[batch_size];
   struct io_uring r_ring;
   struct io_uring w_ring;
   std::unique_ptr<struct io_uring_cqe*[]> cqes;
   // Pages of the current batch, indexed like buf_pages.
   std::vector<page_info> to_fix_pids;
   // -------------------------------------------------------------------------------------
   // RU epochs [min_uncollected_ru_epoch, max_collected_ru_epoch) are the ones left to collect.
   ru_epoch_t max_collected_ru_epoch = 0;
   ru_epoch_t min_uncollected_ru_epoch = 0;
   // -------------------------------------------------------------------------------------
   // The RU epoch being collected and its mapped log segment.
   ru_epoch_t current_gc_epoch = -1;
   u32 currently_reclaiming_log_id = 0;
   u8* log_records = nullptr;
   u64 cur_log_segment_start = -1;
   u64 cur_log_segment_size = 0;
   // -------------------------------------------------------------------------------------
   u64 actually_fixed = 0;  // in the current batch
   u64 total_fixed = 0;  // in the current RU epoch
   // -------------------------------------------------------------------------------------
   explicit ThreadContext(u32 gc_id) : gc_id(gc_id)
   {
      u32 flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;

      int rc = io_uring_queue_init(2 * batch_size, &r_ring, flags);
      posix_check(rc == 0);

      if (FLAGS_enable_fdp) {
         flags |= (IORING_SETUP_SQE128 | IORING_SETUP_CQE32);
      }
      rc = io_uring_queue_init(2 * batch_size, &w_ring, flags);
      posix_check(rc == 0);

      cqes = std::make_unique<struct io_uring_cqe*[]>(2 * batch_size);
      to_fix_pids.reserve(batch_size);
   }
};
// -------------------------------------------------------------------------------------
RUBackgroundPageFixer::RUBackgroundPageFixer(BufferManager& bm) : bm(bm) {}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::start()
{
   for (s32 gc_id = 0; gc_id < FLAGS_ru_gc_threads; ++gc_id) {
      std::thread([this, gc_id]() { run(gc_id); }).detach();
   }
}
// -------------------------------------------------------------------------------------
bool RUBackgroundPageFixer::tryEnqueueRUEpoch(ru_epoch_t ru_epoch)
{
   // Give priority to the bg page fixer thread, No need to contend with them.
   if (!queue_m.try_lock()) {
      return false;
   }
   to_gc_epochs.push_back(ru_epoch);
   if (sleeping_threads > 0) {
      queue_m.unlock();
      queue_cv.notify_all();
   } else {
      queue_m.unlock();
   }
   return true;
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::wakeUpAll()
{
   std::lock_guard _l(queue_m);
   queue_cv.notify_all();
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::run(u32 gc_id)
{
   std::string gc_thread_name = "ru_gc_" + std::to_string(gc_id);
   pthread_setname_np(pthread_self(), gc_thread_name.c_str());
   tls_writer_thread_id = FLAGS_pp_threads + gc_id;

   running_threads++;
   bm.bg_threads_counter++;

   ThreadContext ctx(gc_id);
   while (bm.bg_threads_keep_running) {
      waitForRUEpochsToCollect(ctx);
      for (ru_epoch_t gc_ru_epoch = ctx.min_uncollected_ru_epoch; gc_ru_epoch < ctx.max_collected_ru_epoch; gc_ru_epoch++) {
         if (!bm.bg_threads_keep_running) break;
         collectRUEpoch(ctx, gc_ru_epoch);
      }
      ctx.min_uncollected_ru_epoch = ctx.max_collected_ru_epoch;
   }
   bm.bg_threads_counter--;
   running_threads--;
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::waitForRUEpochsToCollect(ThreadContext& ctx)
{
   std::unique_lock _l(queue_m);
   sleeping_threads++;
   queue_cv.wait(_l, [&ctx, this]() {
      return to_gc_epochs.size() > static_cast<u64>(ctx.max_collected_ru_epoch) || !bm.bg_threads_keep_running;
   });
   sleeping_threads--;
   ctx.max_collected_ru_epoch = to_gc_epochs.size();
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::collectRUEpoch(ThreadContext& ctx, ru_epoch_t gc_ru_epoch)
{
   ctx.current_gc_epoch = gc_ru_epoch;
   auto& set = bm.ru_discard_set[DEFAULT_RUH][gc_ru_epoch];
   auto& logging = cr::LogManager::getLog(gc_ru_epoch);
   ctx.currently_reclaiming_log_id = logging.log_id;
   mapRUEpochLog(gc_ru_epoch, logging);
   auto start = std::chrono::system_clock::now();

   u64 offset = 0;  // local offset
   ctx.log_records = static_cast<u8*>(set.mmaped_log);
   ctx.cur_log_segment_start = set.log_segment_start;
   ctx.cur_log_segment_size = set.log_segment_size;

   // The GC threads split the log in batches of 4 KiB.
   while (offset + sizeof(cr::WALEntry) < ctx.cur_log_segment_size) {
      ctx.actually_fixed = 0;
      offset = set.offset_batch.fetch_add(4096);
      const u64 pages_to_fix = scanLogBatch(ctx, offset);
      fixPages(ctx, pages_to_fix);
   }
   // -------------------------------------------------------------------------------------
   auto end = std::chrono::system_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
   set.total_fixed.fetch_add(ctx.total_fixed);
   if (set.done_gc.fetch_sub(1) == 1) {
      finishRUEpoch(ctx, duration.count());
   }
   ctx.total_fixed = 0;
   while (set.done_gc.load() != FLAGS_ru_gc_threads) {
   }
}
// -------------------------------------------------------------------------------------
// The first GC thread to reach the RU epoch seals its log and maps it for all the others.
void RUBackgroundPageFixer::mapRUEpochLog(ru_epoch_t gc_ru_epoch, cr::Logging& logging)
{
   auto& set = bm.ru_discard_set[DEFAULT_RUH][gc_ru_epoch];
   ru_epoch_t prev_gc_ru_epoch = gc_ru_epoch - 1;
   set.m.lock();
   if (set.is_currently_being_garbage_collected.exchange(true, std::memory_order_release)) {
      set.m.unlock();
      return;
   }
   logging.mutex.lock();
   // makes sure not log records or holes will appear in this log in the future.
   bool ok = bm.reclaiming_ru_epoch.compare_exchange_strong(prev_gc_ru_epoch, gc_ru_epoch);
   ensure(ok);

   // wait until all holes in the log buffer are filled.
   logging.drain_holes();

   // wait until the Group Committed thread persists the new log records.
   u64 stuck_counter = 0;
   while (logging.wal_gct_cursor.load(std::memory_order_acquire) != logging.wal_log_cursor) {
      _mm_pause();
      if (++stuck_counter == u64(65536)) {
         LOG_ERROR(bm.logger, "Log buffer was not persisted by the GCT, something is wrong\n");
         print_backtrace();
         ensure(false);
      }
      // We're waiting for a single write IO to the log device.
      std::this_thread::sleep_for(std::chrono::microseconds(100));
   }

   set.mmaped_log = mmap(nullptr, logging.wal_lsn_counter, PROT_READ, MAP_PRIVATE, bm.log_fd, logging.log_segment_start);
   if (set.mmaped_log == MAP_FAILED) {
      perror("mmap");
      raise(SIGTRAP);
   }
   set.log_segment_start = logging.log_segment_start;
   set.log_segment_size = logging.wal_lsn_counter;
   logging.mutex.unlock();

   ensure_equal(set.offset_batch.load(), 0);
   set.m.unlock();
   const double threshold = set.ReclaimUnitUsage() * 100.0 / set.total.load(std::memory_order_relaxed);
   const s32 discarded_pages = set.inserted.load(std::memory_order_relaxed) - set.deleted.load(std::memory_order_relaxed);
   LOG_INFO(bm.logger, "Garbage collecting RU epoch %lu with threshold %.2f: upper bound on pages to fix %d, log size to scan %lu", gc_ru_epoch, threshold, discarded_pages, set.log_segment_size);
}
// -------------------------------------------------------------------------------------
// Called by the last GC thread done with the RU epoch.
void RUBackgroundPageFixer::finishRUEpoch(ThreadContext& ctx, u64 seconds_taken)
{
   auto& set = bm.ru_discard_set[DEFAULT_RUH][ctx.current_gc_epoch];
   const ru_epoch_t old_re = bm.reclaimed_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   const ru_epoch_t new_re = old_re + 1;
   ensure_equal(new_re, ctx.current_gc_epoch);
   auto& logging = cr::LogManager::getLog(ctx.current_gc_epoch);
   const u32 ru_usage = set.ReclaimUnitUsage();
   bm.bm_stats.estimated_gc_writes.fetch_add(set.total.load() - ru_usage);
   const u64 fixed_pages_in_ru = set.total_fixed.load();
   set.m.lock();
   const int rc = munmap(set.mmaped_log, ctx.cur_log_segment_size);
   ensure_equal(rc, 0);
   set.mmaped_log = nullptr;
   set.reset();
   bm.reclaimed_ru_epoch[DEFAULT_RUH].fetch_add(1);
   logging.reset();
   set.m.unlock();
   if (FLAGS_wal && FLAGS_wal_pwrite) {
      cr::LogManager::global->resetLogSegment(ctx.current_gc_epoch);
   }
   LOG_INFO(bm.logger, "GC epoch %lu, time taken %lu seconds : Pages fixed = %lu, RU usage : %u", ctx.current_gc_epoch, seconds_taken, fixed_pages_in_ru, ru_usage);
}
// -------------------------------------------------------------------------------------
u64 RUBackgroundPageFixer::scanLogBatch(ThreadContext& ctx, u64& offset)
{
   u64 pages_to_fix = 0;
   u64 submitted = 0;
   const u64 fix_up_to = std::min<u64>(offset + 4096, ctx.cur_log_segment_size);
   cr::WALEntry* last_entry = nullptr;
   while (offset + sizeof(cr::WALEntry) < fix_up_to) {
      cr::WALEntry& entry = *reinterpret_cast<cr::WALEntry*>(&ctx.log_records[offset]);
      last_entry = &entry;

      if (entry.type == cr::WALDTEntry::TYPE::SKIP) {
         offset = utils::upAlign(offset, 4096);
         continue;
      }

      offset += entry.size;

      if (entry.type == cr::WALDTEntry::TYPE::CARRIAGE_RETURN) {
         continue;
      }

      if (!((entry.type == cr::WALDTEntry::TYPE::DT_SPECIFIC)
             || (entry.type == cr::WALDTEntry::TYPE::PER_PAGE_DT_SPECIFIC))) {
         dumpCorruptedLogEntry(ctx, offset, entry, last_entry);
      }
      ensure((entry.type == cr::WALDTEntry::TYPE::DT_SPECIFIC)
            || (entry.type == cr::WALDTEntry::TYPE::PER_PAGE_DT_SPECIFIC));

      // Hacky to interpret the PPL as a WALDTEntry
      auto& dte = *reinterpret_cast<cr::WALDTEntry*>(&entry);

      if (entry.prev_lsn != INVALID_LSN) {
         // This is not the first entry in the chain, skip.
         continue;
      }

      const PID pid = dte.pid;
      auto& page_state = bm.discard_state[pid];

      // It is safe to continue if the page is clean. In fact, from a clean state the page
      // can only concurrently move to a cached state (in the buffer pool). It cannot get
      // concurrently discarded again because pages that are belonging to an ru_epoch older
      // or equal to the currently reclaiming ru do not get evicted.
      // It is also possible that the page is clean but in a newer RU. In that case the page
      // can get discarded and we will double check for that after acquiring the latching the page state.
      if (page_state.isClean()) {
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().clean++; }
         continue;
      }

      // if state is a buffer frame, Issue the write to that page.
      // Make sure the page in the same RU epoch that we're garbage collecting.
      // Keep this step synchronous for now.
      // Need to acquire a lock on the buffer frame and mark it, is being written back.
      // ATTENTION : There might be a risk of deadlock between the guard and the page status lock.
      // If the page is Dirty in the conventional sense write it back.
      if (page_state.isHot()) {
         // Page is in buffer frame
         // For now just continue, I'll handle that later
         COUNTERS_BLOCK(gc)
         {
            GCCounters::myCounters().hot_fixed++;
         }
         continue;
      }

      // The page is Free when we it was reclaimed but its pid is still not reused.
      // Even if the page id is reused we can later find out by checking whether the RU epoch
      // stored in the page is the same as the RU epoch we're currentlty reclaiming.
      // There is a case when the page is reclaimed and the RU is the same, which is after the
      // pid is allocated and before the page is written to storage, in that case the page is
      // in HOT state and we should handle it separately.
      if (page_state.isFree()) {
         continue;
      }

      if (!registerPageToFix(ctx, entry, pid, pages_to_fix)) {
         continue;
      }

      struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.r_ring);
      ensure(sqe != nullptr);
      // TODO(mfd) : Fixed buffer read
      io_uring_prep_read(sqe, bm.ssd_fd, ctx.buf_pages[pages_to_fix].page, PAGE_SIZE, pid * PAGE_SIZE);
      // TODO(mfd) : store just the index into the array.
      //  we can keep the pid here just for debugging.
      ensure((pid & 0xFFFF000000000000) == 0);
      u64 data = (pages_to_fix << 48) | pid;
      io_uring_sqe_set_data64(sqe, data);
      ensure(ctx.to_fix_pids.back().last_lsn != INVALID_LSN);
      ++pages_to_fix;
      if (pages_to_fix == ThreadContext::batch_size / 2) {
         int s = io_uring_submit(&ctx.r_ring);
         ensure(s > 0);
         submitted += s;
      }
   }
   int s = io_uring_submit(&ctx.r_ring);
   ensure(s >= 0);
   submitted += s;
   ensure_equal(submitted, pages_to_fix);
   ensure_equal(pages_to_fix, ctx.to_fix_pids.size());
   return pages_to_fix;
}
// -------------------------------------------------------------------------------------
// Locks the discarded page and inserts an IO frame for it so that the workers wait for it
// to be fixed. Returns false if the page must not be fixed by us.
bool RUBackgroundPageFixer::registerPageToFix(ThreadContext& ctx, const cr::WALEntry& entry, PID pid, u64 idx)
{
   LID* lsn_list = nullptr;
   LID last_lsn = INVALID_LSN;
   u8 nb_log_records = 0;
   Partition& partition = bm.getPartition(pid);
   std::lock_guard<instrumented_mutex> _m(partition.ht_mutex);
   auto frame_handler = partition.io_ht.lookup(pid);
   if (frame_handler) {
      // someone is reading the page, so he will fix it.
      // FIXME(mfd) : Even if someone is reading and fixing the page. It must still be
      // written back before this log reclaimed.
      // Increment the reader count.
      // Read the BufferFrame address and register it for fixing.
      return false;
   }
   // if (bm.discard_state[pid].isHot()) {}
   if (!bm.discard_state[pid].isDiscarded()) {
      // TODO(mfd) : It could be that the page now contains a buffer frame
      // And I need to handle this case, seperately.
      return false;
   }
   if (bm.discard_state[pid].getLogID() != ctx.currently_reclaiming_log_id) {
      return false;
   }
   IOFrame& io_frame = partition.io_ht.insert(pid);
   io_frame.readers_counter = 1;
   io_frame.state = IOFrame::STATE::READING;
   io_frame.inserted_by_bg_page_fixer = true;

   bool ok = io_frame.mutex.try_lock();
   ensure(ok);

   auto p = bm.discard_state[pid].getLocked();
   ensure(bm.discard_state[pid].isDiscarded());
   nb_log_records = p.second;
   lsn_list = reinterpret_cast<LID*>(p.first);
   if (nb_log_records == 1) {
      last_lsn = p.first;
   } else {
      last_lsn = lsn_list[nb_log_records - 1];
   }
   ensure_equal(ctx.to_fix_pids.size(), idx);
   ctx.to_fix_pids.emplace_back(pid, lsn_list, entry.lsn, last_lsn, nb_log_records);
   if (nb_log_records == 1) {
      ctx.to_fix_pids.back().lsn_list = &ctx.to_fix_pids.back().last_lsn;
   }
   return true;
}
// -------------------------------------------------------------------------------------
// Waits for the reads issued by scanLogBatch(), fixes the pages and waits for their write back.
void RUBackgroundPageFixer::fixPages(ThreadContext& ctx, u64 pages_to_fix)
{
   if (pages_to_fix > 0) {
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().pages_read += pages_to_fix; }
      u32 ready = io_uring_peek_batch_cqe(&ctx.r_ring, ctx.cqes.get(), pages_to_fix);
      for (u32 i = 0; i < ready; ++i) {
         fixPage(ctx, ctx.cqes[i]);
      }
      // Submit to the w_ring
      if (ready > 0)
         io_uring_cq_advance(&ctx.r_ring, ready);
      // Wait for the remaining part, and do the same thing.
      u32 remaining = pages_to_fix - ready;
      if (remaining > 0) {
         // do the same, loop over all finshed. define a callback.
         int rc = io_uring_wait_cqe_nr(&ctx.r_ring, ctx.cqes.get(), remaining);
         posix_check(rc == 0);
         struct io_uring_cqe* cqe;
         u32 head;
         u32 i = 0;
         io_uring_for_each_cqe(&ctx.r_ring, head, cqe)
         {
            fixPage(ctx, cqe);
            ++i;
         }
         ensure_equal(i, remaining);
         io_uring_cq_advance(&ctx.r_ring, remaining);
      }
      // TODO(mfd) : Submit and wait.
      ready = io_uring_peek_batch_cqe(&ctx.w_ring, ctx.cqes.get(), ctx.actually_fixed);

      for (u32 i = 0; i < ready; ++i) {
         ackFixedPage(ctx, ctx.cqes[i]);
      }
      if (ready > 0)
         io_uring_cq_advance(&ctx.w_ring, ready);
      // Wait for the remaining part, and do the same thing.
      remaining = ctx.actually_fixed - ready;
      if (remaining > 0) {
         int rc = io_uring_wait_cqe_nr(&ctx.w_ring, ctx.cqes.get(), remaining);
         // Temporly check that this did not fail
         posix_check(rc == 0);

         struct io_uring_cqe* cqe;
         u32 head;
         u32 i = 0;
         io_uring_for_each_cqe(&ctx.w_ring, head, cqe)
         {
            ackFixedPage(ctx, cqe);
            ++i;
         }
         ensure_equal(i, remaining);
         io_uring_cq_advance(&ctx.w_ring, remaining);
      }
   }
   ctx.to_fix_pids.clear();
}
// -------------------------------------------------------------------------------------
// A discarded page got read: apply its log records and issue its write back.
void RUBackgroundPageFixer::fixPage(ThreadContext& ctx, struct io_uring_cqe* cqe)
{
   ensure_equal(cqe->res, PAGE_SIZE);
   u64 data = io_uring_cqe_get_data64(cqe);
   u64 fixed_pid = data & 0x0000FFFFFFFFFFFF;
   u64 idx = data >> 48;
   assert(idx < ctx.to_fix_pids.size());
   ensure_equal(ctx.to_fix_pids[idx].pid, fixed_pid);
   const LID last_lsn = ctx.to_fix_pids[idx].last_lsn;
   auto& bf = ctx.buf_pages[idx];
   auto& page = bf.page;
   ensure_equal(page.magic_debugging_number, fixed_pid);
   if (page.ru_epoch != ctx.current_gc_epoch) {
      // This means that the page was written back in a new RU epoch
      // and got discarded. Unlock and leave state as is.
      ensure_lt(ctx.current_gc_epoch, page.ru_epoch);
      bm.discard_state[fixed_pid].unlock();
      // Clear up the IO Frame and unlock it so that the workers could retry
      releaseIOFrame(fixed_pid);
      COUNTERS_BLOCK(gc)
      {
         GCCounters::myCounters().dirty_in_other_ru_epoch++;
      }
      return;
   }

   LID lsn = last_lsn;
   std::vector<cr::WALEntry*> to_apply_log_records;
   ensure(lsn != INVALID_LSN);

   const auto& page_to_fix_info = ctx.to_fix_pids[idx];

#if DEBUG_BACKWARD_LOG_CHAIN_TRAVERSE
   ensure(page_to_fix_info.nb_log_records > 0);
   while (lsn != INVALID_LSN) {
      ensure_lte(ctx.cur_log_segment_start, lsn);
      u64 off = lsn - ctx.cur_log_segment_start;
      auto* entry = reinterpret_cast<cr::WALEntry*>(&ctx.log_records[off]);

      bool ok = bm.logRecordSanityCheck(entry, page, lsn);
      ensure(ok);

      to_apply_log_records.push_back(entry);

      if (to_apply_log_records.size() > FLAGS_max_log_records_to_discard) {
         printf("[ERR] Number of logs to apply in the global state : %u\n", page_to_fix_info.nb_log_records);
         printf("[ERR] curr lsn = %lu, prev lsn = %lu, lseg start lsn %lu\n", lsn, entry->prev_lsn, ctx.cur_log_segment_start);
         for (const auto& e : to_apply_log_records) {
            reinterpret_cast<cr::WALDTEntry*>(e)->dump();
         }
         page.dump();
         bm.discard_state[fixed_pid].dump();
         printf("[ERR] Total Fixed %lu \n", ctx.total_fixed);
      }

      ensure(to_apply_log_records.size() <= FLAGS_max_log_records_to_discard);
      lsn = entry->prev_lsn;
   }
   std::reverse(to_apply_log_records.begin(), to_apply_log_records.end());
   // Sanity checks.
   ensure(!to_apply_log_records.empty());
   ensure_equal_goto_fail(to_apply_log_records.size(), ctx.to_fix_pids[idx].nb_log_records);
   for (u8 i = 0; i < to_apply_log_records.size(); ++i) {
      ensure_equal_goto_fail(to_apply_log_records[i]->lsn, page_to_fix_info.lsn_list[i]);
   }

#else
   for (u8 i = 0; i < page_to_fix_info.nb_log_records; ++i) {
      LID lsn = page_to_fix_info.lsn_list[i];
      u64 off = lsn - ctx.cur_log_segment_start;
      auto* entry = reinterpret_cast<cr::WALEntry*>(&ctx.log_records[off]);

      bool ok = bm.logRecordSanityCheck(entry, page, lsn);
      ensure(ok);

      to_apply_log_records.push_back(entry);
   }
#endif

   u32 absorbed_writes = 0;
   for (const auto& log_record: to_apply_log_records) {
      if (log_record->type == cr::WALEntry::TYPE::PER_PAGE_DT_SPECIFIC) {
         auto* ppl = reinterpret_cast<BufferFrame::PPL*>(log_record);
         DTRegistry::global_dt_registry.redo(page.dt_id, page.dt, ppl->log_records, ppl->nb_log_records, ppl->payload_size());
         absorbed_writes += ppl->absorbed_writes;
      } else {
         auto* dte = reinterpret_cast<cr::WALDTEntry*>(log_record);
         DTRegistry::global_dt_registry.redo(page.dt_id, page.dt, dte->payload, 1, dte->size - sizeof(cr::WALDTEntry));
         absorbed_writes += 1;
      }
   }

   COUNTERS_BLOCK(absorbed_writes_histogram)
   {
      absorbed_writes = std::min<u32>(absorbed_writes, 63);
      GCCounters::myCounters().absorbed_writes_histogram[absorbed_writes]++;
   }

   page.PLSN += to_apply_log_records.size();
   page.last_written_lsn = last_lsn;
   page.GSN = reinterpret_cast<cr::WALDTEntry*>(to_apply_log_records.back())->gsn;
   bm.fdp_placement_policy.onFixDiscardedPage(bf);
   bm.pageWriteBackPrologue(bf);
   ++page.bg_page_fixed_count;

   // Just for debugging
   ++ctx.total_fixed;
   ++ctx.actually_fixed;

   if (to_apply_log_records.size() > 1)
      bm.randomAllocator().free(ctx.to_fix_pids[idx].lsn_list, to_apply_log_records.size());

#if DEBUG_BACKWARD_LOG_CHAIN_TRAVERSE
   goto success;

fail:
   printf("[ERR] Number of logs to apply in the global state : %u\n", page_to_fix_info.nb_log_records);
   for (const auto& e : to_apply_log_records) {
      reinterpret_cast<cr::WALDTEntry*>(e)->dump();
   }
   page.dump();
   bm.discard_state[fixed_pid].dump();
   printf("[ERR] Total Fixed %lu \n", ctx.total_fixed);
   page_to_fix_info.dump();
   __asm__ volatile("int3");

success:

#endif
   struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.w_ring);
   ensure(sqe != nullptr);
   io_uring_prep_write(sqe, bm.ssd_fd, (void*)page, PAGE_SIZE, fixed_pid * PAGE_SIZE);
   io_uring_sqe_set_data64(sqe, data);
   // TODO(mfd) : Amortize
   int s = io_uring_submit(&ctx.w_ring);
   ensure(s == 1);
}
// -------------------------------------------------------------------------------------
// A fixed page got written back: mark it clean and let the waiting workers read it.
void RUBackgroundPageFixer::ackFixedPage(ThreadContext& ctx, struct io_uring_cqe* cqe)
{
   ensure(cqe->res == PAGE_SIZE);
   u64 data = io_uring_cqe_get_data64(cqe);
   PID pid = data & 0x0000FFFFFFFFFFFF;
   u64 idx = data >> 48;
   auto& bf = ctx.buf_pages[idx];
   auto& page = bf.page;
   ensure_equal(page.magic_debugging_number, pid);

   bm.discard_state[pid].unlockClean(ctx.to_fix_pids[idx].last_lsn, cr::LogManager::getLogID(page.ru_epoch));

   releaseIOFrame(pid);
   COUNTERS_BLOCK(gc)
   {
      GCCounters::myCounters().total_fixed++;
   }
   bf.reset();
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::releaseIOFrame(PID pid)
{
   Partition& partition = bm.getPartition(pid);
   std::lock_guard g_guard(partition.ht_mutex);
   auto frame_handler = partition.io_ht.lookup(pid);
   ensure(frame_handler);
   IOFrame& frame = frame_handler.frame();
   ensure(frame.state == IOFrame::STATE::READING);
   // After unlocking, workers waiting for the page to be fixed, will
   // jump and retry, When they acquire the partition lock, they won't
   // see the frame.
   frame.mutex.unlock();
   // -------------------------------------------------------------------------------------
   if (frame.readers_counter.fetch_add(-1) == 1) {
      partition.io_ht.remove(pid);
   } else {
      frame.state = IOFrame::STATE::TO_DELETE;
   }
}
// -------------------------------------------------------------------------------------
/******************* CRASH Debugging *********************/
void RUBackgroundPageFixer::dumpCorruptedLogEntry(ThreadContext& ctx,
                                                           u64 offset,
                                                           const cr::WALEntry& entry,
                                                           cr::WALEntry* last_entry)
{
   auto crash_logger = utils::CrashLogger("crash.log." + to_string(ctx.gc_id));
   cerr << "type : " << +static_cast<u8>(entry.type) << endl;
   cerr << "offset : " << offset - entry.size << endl;
   cerr << "next offset : " << offset << endl;
   cerr << "Cur Log ID : " << ctx.currently_reclaiming_log_id << endl;
   cerr << "cureent log segment size : " << ctx.cur_log_segment_size << endl;
   if (last_entry != nullptr) {
      if ((last_entry->type == cr::WALDTEntry::TYPE::DT_SPECIFIC)
          || (last_entry->type == cr::WALDTEntry::TYPE::PER_PAGE_DT_SPECIFIC)) {
         reinterpret_cast<cr::WALDTEntry*>(last_entry)->dump();
      } else {
         last_entry->dump();
      }

   } else {
      cerr << "First Entry" << endl;
   }
   u64 off = offset - (offset % 4096);
   off = std::min(off, off - 4096);
   while (off < offset) {
      cr::WALEntry& dentry = *reinterpret_cast<cr::WALEntry*>(&ctx.log_records[off]);
      dentry.dump();
      if (dentry.type == cr::WALDTEntry::TYPE::SKIP) {
         off = utils::upAlign(off, 4096);
         continue;
      }
      off += dentry.size;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore

