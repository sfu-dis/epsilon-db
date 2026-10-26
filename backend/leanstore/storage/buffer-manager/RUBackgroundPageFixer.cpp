#include "RUBackgroundPageFixer.hpp"

#include "BufferManager.hpp"
#include "CompressedSpace.hpp"
#include "CustomSlabAllocator.hpp"
#include "LBAChain.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency-recovery/LogManager.hpp"
#include "leanstore/concurrency-recovery/Logging.hpp"
#include "leanstore/storage/NvmeDevice.hpp"
#include "leanstore/storage/btree/core/BTreeNode.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/profiling/counters/GCCounters.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/CrashLogger.hpp"
// -------------------------------------------------------------------------------------
#include <liburing.h>
#include <pthread.h>

#include <algorithm>
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
      std::cout << "   first_lsn      = " << lsnToString(first_lsn) << "\n";
      std::cout << "   last_lsn       = " << lsnToString(last_lsn) << "\n";
      std::cout << "   nb_log_records = " << +nb_log_records << "\n";

      std::cout << "   lsn_list       = [";
      for (u8 i = 0; i < nb_log_records; i++) {
         std::cout << lsnToString(lsn_list[i]);
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
   // FLAGS_compress_pages: blocks of the LBA chain read per batch, also the capacity of the packer the moved pages are
   // written with. The blocks a batch frees make the pool of the next one.
   static constexpr u32 compressed_batch_blocks = 64;
   // -------------------------------------------------------------------------------------
   const u32 gc_id;
   BufferFrame buf_pages[batch_size];
   struct io_uring r_ring;
   struct io_uring w_ring;
   std::unique_ptr<struct io_uring_cqe*[]> cqes;
   // Pages of the current batch, indexed like buf_pages.
   std::vector<page_info> to_fix_pids;
   // Hot pages to fix of the current batch
   std::vector<HotPageRef> hot_pids;
   // Hot pages that could not be handled yet, retried before the RU epoch is finished.
   std::vector<HotPageRef> deferred_hot_pids;
   // Frames whose copy is in buf_pages, indexed like buf_pages.
   std::vector<BufferFrame*> hot_bfs;
   // FLAGS_bg_page_fixer_tree_lookup: pending LSNs copied from the parent swips, indexed like buf_pages.
   LID parent_lsns[batch_size][btree::BTreeNode::max_pending_lsn];
   // -------------------------------------------------------------------------------------
   // FLAGS_compress_pages
   struct MovedPage {
      PID pid;
      PID old_location;
      PID new_location;
      ru_epoch_t ru_epoch;  // the one it got written to
      bool fixed;  // a discarded page that got fixed, otherwise a clean copy
      LID last_lsn;
   };
   // compressed_batch_blocks blocks each, the blocks read from the LBA chain and the blocks the packer fills.
   std::unique_ptr<u8, decltype(&std::free)> read_blocks{nullptr, &std::free};
   std::unique_ptr<u8, decltype(&std::free)> packer_blocks{nullptr, &std::free};
   std::unique_ptr<BlockPacker> packer;
   // Pages added to the packer, their IO frame is held until the packer is flushed.
   std::vector<MovedPage> moved;
   // Set by lookUpPageInTree() when it returns HOT.
   BufferFrame* lookup_hot_bf = nullptr;
   // Pages of the current batch movePage() could not move yet, src points into read_blocks.
   struct DeferredPage {
      PID pid;
      PID location;
      const u8* src;
      ru_epoch_t ru_epoch;
   };
   std::vector<DeferredPage> deferred_pages;
   std::vector<DeferredPage> retried_pages;
   u64 total_clean_copied = 0;  // in the current RU epoch
   // -------------------------------------------------------------------------------------
   // Jobs of to_gc_epochs already picked up, and those left to collect.
   u64 picked_up_jobs = 0;
   std::vector<GCJob> jobs;
   // -------------------------------------------------------------------------------------
   // The RU epoch being collected and its mapped log segment.
   ru_epoch_t current_gc_epoch = -1;
   bool log_only = false;  // see tryEnqueueRUEpoch()
   u32 currently_reclaiming_log_id = 0;
   u8* log_records = nullptr;
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
      hot_bfs.reserve(batch_size);
   }
};
// -------------------------------------------------------------------------------------
// Inserts the IO frame of a page about to be fixed, the workers that want to load the page wait on it until
// releaseIOFrame(). Pre: partition.ht_mutex is held and the page has no IO frame.
static void insertFixerIOFrame(Partition& partition, PID pid, std::atomic<s64>& io_frames_held)
{
   io_frames_held.fetch_add(1, std::memory_order_relaxed);
   IOFrame& io_frame = partition.io_ht.insert(pid);
   io_frame.readers_counter = 1;
   io_frame.state = IOFrame::STATE::READING;
   io_frame.inserted_by_bg_page_fixer = true;
   bool ok = io_frame.mutex.try_lock();
   ensure(ok);
}
// -------------------------------------------------------------------------------------
RUBackgroundPageFixer::RUBackgroundPageFixer(BufferManager& bm) : bm(bm)
{
   stats.threads = std::make_unique<ThreadStats[]>(std::max<s32>(FLAGS_ru_gc_threads, 1));
}
// -------------------------------------------------------------------------------------
u64 RUBackgroundPageFixer::nowNs()
{
   return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::setPhase(ThreadContext& ctx, Phase phase)
{
   ThreadStats& t = stats.threads[ctx.gc_id];
   t.phase_since_ns.store(nowNs(), std::memory_order_relaxed);
   t.phase.store(static_cast<u8>(phase), std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::start()
{
   for (s32 gc_id = 0; gc_id < FLAGS_ru_gc_threads; ++gc_id) {
      std::thread([this, gc_id]() { run(gc_id); }).detach();
   }
}
// -------------------------------------------------------------------------------------
bool RUBackgroundPageFixer::tryEnqueueRUEpoch(ru_epoch_t ru_epoch, bool log_only)
{
   // Give priority to the bg page fixer thread, No need to contend with them.
   if (!queue_m.try_lock()) {
      stats.enqueue_failures.fetch_add(1, std::memory_order_relaxed);
      return false;
   }
   to_gc_epochs.push_back({ru_epoch, log_only});
   stats.jobs_enqueued.fetch_add(1, std::memory_order_relaxed);
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
   if (FLAGS_compress_pages) {
      const u64 bytes = u64(ThreadContext::compressed_batch_blocks) * PAGE_SIZE;
      ctx.read_blocks.reset(static_cast<u8*>(std::aligned_alloc(PAGE_SIZE, bytes)));
      ctx.packer_blocks.reset(static_cast<u8*>(std::aligned_alloc(PAGE_SIZE, bytes)));
      ensure(ctx.read_blocks != nullptr && ctx.packer_blocks != nullptr);
      ctx.packer = std::make_unique<BlockPacker>(*bm.compressed_space, ctx.packer_blocks.get(), ThreadContext::compressed_batch_blocks,
                                                 bm.lba_chain.get());
   }
   while (bm.bg_threads_keep_running) {
      setPhase(ctx, Phase::IDLE);
      waitForRUEpochsToCollect(ctx);
      for (const GCJob& job : ctx.jobs) {
         if (!bm.bg_threads_keep_running) break;
         collectRUEpoch(ctx, job);
      }
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
      return to_gc_epochs.size() > ctx.picked_up_jobs || !bm.bg_threads_keep_running;
   });
   sleeping_threads--;
   ctx.jobs.assign(to_gc_epochs.begin() + ctx.picked_up_jobs, to_gc_epochs.end());
   ctx.picked_up_jobs = to_gc_epochs.size();
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::collectRUEpoch(ThreadContext& ctx, GCJob job)
{
   const ru_epoch_t gc_ru_epoch = job.ru_epoch;
   ctx.current_gc_epoch = gc_ru_epoch;
   ctx.log_only = job.log_only;
   auto& set = bm.ru_discard_set[DEFAULT_RUH][gc_ru_epoch];
   auto& logging = cr::LogManager::getLog(gc_ru_epoch);
   ctx.currently_reclaiming_log_id = logging.log_id;
   // The log of an RU epoch is reclaimed only once. When it was reclaimed early, by a log_only
   // job (which always precedes in the queue), its pages are already fixed and its log is empty.
   const bool scan_log = job.log_only || !set.log_reclaimed.load(std::memory_order_acquire);
   setPhase(ctx, Phase::MAP_LOG);
   mapRUEpochLog(ctx, logging, scan_log);
   auto start = std::chrono::system_clock::now();

   u64 offset = 0;  // local offset
   ctx.log_records = static_cast<u8*>(set.mmaped_log);
   ctx.cur_log_segment_size = scan_log ? set.log_segment_size : 0;

   if (FLAGS_compress_pages) {
      // The GC threads split the blocks of the LBA chain in batches. The pool of blocks the moved pages are written to
      // is the free list of the thread: the blocks a batch frees are the first ones reused, which invalidates their
      // copy in this RU epoch on the SSD. What is left of the pool carries over to the next RU epoch.
      bm.compressed_space->reserveBlocks(ThreadContext::compressed_batch_blocks);
      const u64 entries = set.lba_chain_entries.size();
      while (true) {
         const u64 begin = set.offset_batch.fetch_add(ThreadContext::compressed_batch_blocks);
         if (begin >= entries) {
            break;
         }
         const u64 count = std::min<u64>(entries - begin, ThreadContext::compressed_batch_blocks);
         moveBlocksBatch(ctx, set.lba_chain_entries.data() + begin, count);
         setPhase(ctx, Phase::HOT);
         fixHotPages(ctx);
         stats.chain_done.fetch_add(count, std::memory_order_relaxed);
      }
   } else if (FLAGS_bg_page_fixer_variant == 2) {
      // The GC threads split the PIDs of the LBA chain in batches.
      const u64 entries = set.lba_chain_entries.size();
      while (true) {
         const u64 begin = set.offset_batch.fetch_add(ThreadContext::batch_size);
         if (begin >= entries) {
            break;
         }
         ctx.actually_fixed = 0;
         const PID* pids = set.lba_chain_entries.data() + begin;
         const u64 count = std::min<u64>(entries - begin, ThreadContext::batch_size);
         if (FLAGS_bg_page_fixer_tree_lookup) {
            fixLBAChainBatchInTree(ctx, pids, count);
         } else {
            setPhase(ctx, Phase::READ);
            const u64 pages_to_fix = scanLBAChainBatch(ctx, pids, count);
            setPhase(ctx, Phase::FIX);
            fixPages(ctx, pages_to_fix);
         }
         setPhase(ctx, Phase::HOT);
         fixHotPages(ctx);
         stats.chain_done.fetch_add(count, std::memory_order_relaxed);
      }
   } else {
      // The GC threads split the log in batches of 4 KiB.
      while (offset + sizeof(cr::WALEntry) < ctx.cur_log_segment_size) {
         ctx.actually_fixed = 0;
         offset = set.offset_batch.fetch_add(4096);
         setPhase(ctx, Phase::READ);
         const u64 pages_to_fix = scanLogBatch(ctx, offset);
         setPhase(ctx, Phase::FIX);
         fixPages(ctx, pages_to_fix);
         // At this point the bg page fixer thread no longer holds any IOFrame, nor any PageState locks.
         setPhase(ctx, Phase::HOT);
         fixHotPages(ctx);
      }
   }
   setPhase(ctx, Phase::DRAIN);
   drainDeferredHotPages(ctx);
   // -------------------------------------------------------------------------------------
   auto end = std::chrono::system_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
   set.total_fixed.fetch_add(ctx.total_fixed);
   set.total_clean_copied.fetch_add(ctx.total_clean_copied);
   if (set.done_gc.fetch_sub(1) == 1) {
      setPhase(ctx, Phase::FINISH);
      if (ctx.log_only) {
         finishRUEpochLogReclaim(ctx, duration.count());
      } else {
         finishRUEpoch(ctx, duration.count());
      }
   }
   ctx.total_fixed = 0;
   ctx.total_clean_copied = 0;
   setPhase(ctx, Phase::BARRIER);
   while (set.done_gc.load() != FLAGS_ru_gc_threads) {
   }
}
// -------------------------------------------------------------------------------------
// The first GC thread to reach the RU epoch seals its log and maps it for all the others.
void RUBackgroundPageFixer::mapRUEpochLog(ThreadContext& ctx, cr::Logging& logging, bool scan_log)
{
   const ru_epoch_t gc_ru_epoch = ctx.current_gc_epoch;
   auto& set = bm.ru_discard_set[DEFAULT_RUH][gc_ru_epoch];
   ru_epoch_t prev_gc_ru_epoch = gc_ru_epoch - 1;
   set.m.lock();
   if (set.is_currently_being_garbage_collected.exchange(true, std::memory_order_release)) {
      set.m.unlock();
      return;
   }
   stats.cur_epoch.store(gc_ru_epoch, std::memory_order_relaxed);
   stats.cur_epoch_start_ns.store(nowNs(), std::memory_order_relaxed);
   stats.chain_entries.store(0, std::memory_order_relaxed);
   stats.chain_done.store(0, std::memory_order_relaxed);
   logging.mutex.lock();
   if (ctx.log_only) {
      // A full log is already redirected to the sink logs (see Logging::reserveLSN()), which
      // keeps it sealed. The RU epoch is not reclaimed, so reclaiming_ru_epoch stays as is.
      ensure(logging.redirect_to_sink_log.load(std::memory_order_acquire));
   } else {
      // makes sure not log records or holes will appear in this log in the future.
      bool ok = bm.reclaiming_ru_epoch.compare_exchange_strong(prev_gc_ru_epoch, gc_ru_epoch);
      ensure(ok);
   }
   if (!scan_log) {
      // The log was reclaimed early and stayed redirected since, nothing to scan.
      logging.mutex.unlock();
      if (FLAGS_bg_page_fixer_variant == 2) {
         // Only the blocks of the chain, they are recycled once the RU epoch is reclaimed. With compression, the
         // blocks of the RU epoch are moved by this pass.
         loadLBAChain(set, gc_ru_epoch, /* with_entries */ FLAGS_compress_pages);
         stats.chain_entries.store(set.lba_chain_entries.size(), std::memory_order_relaxed);
      }
      set.m.unlock();
      LOG_INFO(bm.logger, "Garbage collecting RU epoch %lu whose log was already reclaimed", gc_ru_epoch);
      return;
   }

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
   if (FLAGS_bg_page_fixer_variant == 2) {
      loadLBAChain(set, gc_ru_epoch, /* with_entries */ true);
      stats.chain_entries.store(set.lba_chain_entries.size(), std::memory_order_relaxed);
   }

   ensure_equal(set.offset_batch.load(), 0);
   set.m.unlock();
   const double threshold = set.ReclaimUnitUsage() * 100.0 / set.total.load(std::memory_order_relaxed);
   const s32 discarded_pages = set.inserted.load(std::memory_order_relaxed) - set.deleted.load(std::memory_order_relaxed);
   LOG_INFO(bm.logger, "Garbage collecting %sRU epoch %lu with threshold %.2f: upper bound on pages to fix %d, log size %lu, LBA chain of %lu blocks, %lu entries",
            ctx.log_only ? "the full log of " : "", gc_ru_epoch, threshold, discarded_pages, set.log_segment_size, set.lba_chain_blocks.size(),
            set.lba_chain_entries.size());
}
// -------------------------------------------------------------------------------------
// Caller holds set.m, the other GC threads wait on it before scanning.
void RUBackgroundPageFixer::loadLBAChain(RUEpochDiscardSet& set, ru_epoch_t ru_epoch, bool with_entries)
{
   ensure(set.lba_chain_entries.empty() && set.lba_chain_blocks.empty());
   const PID head = bm.persistant_ru_state->chainHead(ru_epoch);
   LBAChain::readChain(bm.ssd_fd, head, ru_epoch, with_entries ? &set.lba_chain_entries : nullptr, set.lba_chain_blocks);
   if (FLAGS_compress_pages) {
      // A block freed and reused within the RU epoch is listed several times, it is moved once. In LBA order, and
      // without the tombstones.
      auto& entries = set.lba_chain_entries;
      std::sort(entries.begin(), entries.end());
      entries.erase(std::unique(entries.begin(), entries.end()), entries.end());
      if (!entries.empty() && entries.back() == INVALID_PID) {
         entries.pop_back();
      }
   }
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
   const bool log_already_reclaimed = set.log_reclaimed.load();
   set.m.lock();
   if (!log_already_reclaimed) {
      const int rc = munmap(set.mmaped_log, ctx.cur_log_segment_size);
      ensure_equal(rc, 0);
      set.mmaped_log = nullptr;
   }
   const u64 lba_chain_blocks = set.lba_chain_blocks.size();
   if (FLAGS_compress_pages) {
      reportBlocks(ctx, set);
   }
   if (FLAGS_bg_page_fixer_variant == 2) {
      // The chain lives as long as the RU epoch. Its head slot is reused by the RU epoch opened in this slot, which
      // only happens once the set is reset.
      for (const auto& [pid, written_ru_epoch] : set.lba_chain_blocks) {
         LBAChain::recycleBlock(bm, pid, written_ru_epoch);
      }
      bm.persistant_ru_state->chainHead(ctx.current_gc_epoch) = INVALID_PID;
   }
   set.reset();
   stats.cur_epoch.store(-1, std::memory_order_relaxed);
   bm.reclaimed_ru_epoch[DEFAULT_RUH].fetch_add(1);
   // Also stops redirecting an early reclaimed log, so that the next RU epoch mapped to it can use it.
   logging.reset();
   set.m.unlock();
   if (!log_already_reclaimed && FLAGS_wal && FLAGS_wal_pwrite) {
      cr::LogManager::global->resetLogSegment(ctx.current_gc_epoch);
   }
   LOG_INFO(bm.logger, "GC epoch %lu, time taken %lu seconds : Pages fixed = %lu, RU usage : %u, LBA chain blocks recycled : %lu", ctx.current_gc_epoch,
            seconds_taken, fixed_pages_in_ru, ru_usage, lba_chain_blocks);
}
// -------------------------------------------------------------------------------------
// Called by the last GC thread done with a log_only job: the discarded pages are fixed, so the log
// space is reclaimed. The RU epoch itself is not, it stays undiscardable until its in-order pass.
void RUBackgroundPageFixer::finishRUEpochLogReclaim(ThreadContext& ctx, u64 seconds_taken)
{
   auto& set = bm.ru_discard_set[DEFAULT_RUH][ctx.current_gc_epoch];
   auto& logging = cr::LogManager::getLog(ctx.current_gc_epoch);
   const u64 fixed_pages_in_ru = set.total_fixed.load();
   set.m.lock();
   const int rc = munmap(set.mmaped_log, ctx.cur_log_segment_size);
   ensure_equal(rc, 0);
   set.mmaped_log = nullptr;
   set.afterLogReclaim();
   stats.cur_epoch.store(-1, std::memory_order_relaxed);
   logging.reset(/* keep_redirecting */ true);
   set.m.unlock();
   if (FLAGS_wal && FLAGS_wal_pwrite) {
      cr::LogManager::global->resetLogSegment(ctx.current_gc_epoch);
   }
   LOG_INFO(bm.logger, "GC full log of epoch %lu, time taken %lu seconds : Pages fixed = %lu", ctx.current_gc_epoch, seconds_taken, fixed_pages_in_ru);
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
      if (!needsFix(ctx, pid)) {
         continue;
      }
      if (!registerPageToFix(ctx, entry.lsn, pid, pages_to_fix)) {
         continue;
      }
      issueFixRead(ctx, pid, pages_to_fix, submitted);
   }
   int s = io_uring_submit(&ctx.r_ring);
   ensure(s >= 0);
   submitted += s;
   ensure_equal(submitted, pages_to_fix);
   ensure_equal(pages_to_fix, ctx.to_fix_pids.size());
   return pages_to_fix;
}
// -------------------------------------------------------------------------------------
// The PIDs of a batch of the LBA chain, see scanLogBatch(). A PID written several times to the RU epoch is listed as
// many times, it is fixed once since its state is clean afterwards.
u64 RUBackgroundPageFixer::scanLBAChainBatch(ThreadContext& ctx, const PID* pids, u64 count)
{
   ensure_lte(count, ThreadContext::batch_size);
   u64 pages_to_fix = 0;
   u64 submitted = 0;
   for (u64 i = 0; i < count; ++i) {
      const PID pid = pids[i];
      // A tombstone, see LBAChain::record().
      if (pid == INVALID_PID) {
         continue;
      }
      ensure_lt(pid, bm.max_pid);
      if (!needsFix(ctx, pid)) {
         continue;
      }
      // The LBA chain does not tell the first log record of the page in this log.
      if (!registerPageToFix(ctx, INVALID_LSN, pid, pages_to_fix)) {
         continue;
      }
      issueFixRead(ctx, pid, pages_to_fix, submitted);
   }
   int s = io_uring_submit(&ctx.r_ring);
   ensure(s >= 0);
   submitted += s;
   ensure_equal(submitted, pages_to_fix);
   ensure_equal(pages_to_fix, ctx.to_fix_pids.size());
   return pages_to_fix;
}
// -------------------------------------------------------------------------------------
// FLAGS_bg_page_fixer_tree_lookup: the page state is not used to find the pages to fix. Every page of the batch is
// read, then looked up in its B-Tree (lookUpPageInTree()), the discarded ones are fixed and written back.
// Every read page gets an entry in to_fix_pids, so that to_fix_pids, buf_pages and the IO user data share the index.
void RUBackgroundPageFixer::fixLBAChainBatchInTree(ThreadContext& ctx, const PID* pids, u64 count)
{
   ensure_lte(count, ThreadContext::batch_size);
   ensure(ctx.to_fix_pids.empty());
   for (u64 i = 0; i < count; ++i) {
      const PID pid = pids[i];
      // A tombstone, see LBAChain::record().
      if (pid == INVALID_PID) {
         continue;
      }
      ensure_lt(pid, bm.max_pid);
      if (FLAGS_bg_page_fixer_tree_lookup_state_filter && !mayBeDiscardedInLog(ctx, pid)) {
         continue;
      }
      const u64 idx = ctx.to_fix_pids.size();
      ctx.to_fix_pids.emplace_back(pid, nullptr, INVALID_LSN, INVALID_LSN, 0);
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.r_ring);
      ensure(sqe != nullptr);
      io_uring_prep_read(sqe, bm.ssd_fd, ctx.buf_pages[idx].page, PAGE_SIZE, pid * PAGE_SIZE);
      io_uring_sqe_set_data64(sqe, idx);
   }
   const u32 to_read = ctx.to_fix_pids.size();
   if (to_read == 0) {
      return;
   }
   setPhase(ctx, Phase::READ);
   int s = io_uring_submit_and_wait(&ctx.r_ring, to_read);
   ensure_equal(s, static_cast<int>(to_read));
   struct io_uring_cqe* cqe;
   u32 head;
   u32 completed = 0;
   io_uring_for_each_cqe(&ctx.r_ring, head, cqe)
   {
      ensure_equal(cqe->res, PAGE_SIZE);
      ++completed;
   }
   ensure_equal(completed, to_read);
   io_uring_cq_advance(&ctx.r_ring, to_read);
   COUNTERS_BLOCK(gc) { GCCounters::myCounters().pages_read += to_read; }
   // -------------------------------------------------------------------------------------
   setPhase(ctx, Phase::LOOKUP);
   for (u64 idx = 0; idx < to_read; ++idx) {
      if (lookUpPageInTree(ctx, idx) == TreeLookup::FIX) {
         redoAndWriteBack(ctx, idx, (idx << 48) | ctx.to_fix_pids[idx].pid);
      }
   }
   waitForFixedPagesWriteBack(ctx);
   ctx.to_fix_pids.clear();
}
// -------------------------------------------------------------------------------------
// FLAGS_bg_page_fixer_tree_lookup. Pre: buf_pages[idx] holds the copy of the page read from the SSD.
// Returns FIX if the page is discarded into the log being reclaimed. It is then registered as registerPageToFix()
// does: IO frame inserted, page state latched, and its pending LSNs copied from the parent into to_fix_pids[idx].
// The parent swip is made clean right away: the IO frame keeps the workers away until the fixed page is written.
// A cached page is queued in hot_pids, like needsFix() does.
//
// Why the copy, read without an IO frame, is the one to fix: a page is only discarded into the log of its RU epoch
// once its write back to that RU epoch has completed, and it is not written again while its swip stays dirty (any
// later write back maps it to a newer RU epoch). So a dirty swip of the reclaimed RU epoch, seen after the read, means
// the copy is that write back. Since mapRUEpochLog() set reclaiming_ru_epoch, the pages of the RU epoch are no longer
// discarded, except for the reclaiming_ru_epoch race of the page providers (TODO.md B4).
//
// FLAGS_compress_pages (location != 0): the page leaves its block, so it must not be read from there anymore. Whoever
// moves a page on the SSD holds its IO frame (see the read path), and the page map only changes for a cached page or
// under an IO frame. So once the IO frame is inserted, with the parent latched exclusively so that the swip stays
// evicted, the copy is still the current one iff the page map still points to it. With claim_clean, a clean evicted
// page is claimed as well (MOVE). A cached page, inner and meta nodes included, is returned as HOT for the caller to
// move. An IO frame inserted by a worker means the page is being loaded (RETRY), the caller looks it up again once the
// page is cached. One inserted by another bg page fixer means that fixer moves it.
RUBackgroundPageFixer::TreeLookup RUBackgroundPageFixer::lookUpPageInTree(ThreadContext& ctx, u64 idx, PID location, bool claim_clean)
{
   ensure(!claim_clean || location != 0);
   page_info& info = ctx.to_fix_pids[idx];
   const PID pid = info.pid;
   BufferFrame::Page& page = ctx.buf_pages[idx].page;
   ensure_equal(page.magic_debugging_number, pid);
   COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_lookups++; }
   if (page.ru_epoch != ctx.current_gc_epoch) {
      // Newer: written back again since. Older: the write back to this RU epoch is still in flight, the page is cached
      // and not discarded. Either way not discarded into this log.
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_other_ru_epoch++; }
      return TreeLookup::SKIP;
   }
   const bool is_leaf = reinterpret_cast<btree::BTreeNode*>(page.dt)->is_leaf;
   if (!is_leaf && location == 0) {
      // Inner and meta nodes are pinned, they are never discarded. With compression they are moved like the other
      // cached pages, to free their block.
      COUNTERS_BLOCK(gc)
      {
         GCCounters::myCounters().clean++;
         GCCounters::myCounters().tree_inner++;
      }
      return TreeLookup::SKIP;
   }
   u64 retries = 0;
   // Records the retries of this lookup on every exit.
   auto record_retries = [&retries]() {
      COUNTERS_BLOCK(gc)
      {
         auto& max_retries = GCCounters::myCounters().tree_max_retries;
         if (retries > max_retries.load(std::memory_order_relaxed)) {
            max_retries.store(retries, std::memory_order_relaxed);
         }
      }
   };
   while (true) {
      jumpmuTry()
      {
         auto parent_handler = DTRegistry::global_dt_registry.findParentOfPage(page.dt_id, page, pid);
         if (!parent_handler) {
            // Deleted or merged away since, or its pid got reused.
            COUNTERS_BLOCK(gc)
            {
               GCCounters::myCounters().clean++;
               GCCounters::myCounters().tree_parent_not_found++;
            }
            record_retries();
            jumpmu_return TreeLookup::SKIP;
         }
         Swip<BufferFrame>& swip = parent_handler->swip;
         if (!swip.isEVICTED()) {
            BufferFrame* bf = location ? &swip.asBufferFrameMasked() : nullptr;
            parent_handler->parent_guard.recheck();
            COUNTERS_BLOCK(gc)
            {
               GCCounters::myCounters().hot_fixed++;
               GCCounters::myCounters().tree_swip_cached++;
            }
            record_retries();
            if (location != 0) {
               ctx.lookup_hot_bf = bf;
               jumpmu_return TreeLookup::HOT;
            }
            ctx.hot_pids.push_back({pid, bf});
            jumpmu_return TreeLookup::SKIP;
         }
         if (!is_leaf) {
            // The inner and meta nodes are pinned: the copy is stale, its pid got reused for a leaf since.
            parent_handler->parent_guard.recheck();
            COUNTERS_BLOCK(gc) { GCCounters::myCounters().dead_pages++; }
            record_retries();
            jumpmu_return TreeLookup::SKIP;
         }
         const bool dirty = swip.isDIRTY();
         if (!dirty && !claim_clean) {
            parent_handler->parent_guard.recheck();
            COUNTERS_BLOCK(gc)
            {
               GCCounters::myCounters().clean++;
               GCCounters::myCounters().tree_swip_clean++;
            }
            record_retries();
            jumpmu_return TreeLookup::SKIP;
         }
         // Jumps if the parent changed since the swip was read.
         BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler->parent_guard);
         if (dirty) {
            ensure_equal(static_cast<ru_epoch_t>(swip.ru_epoch()), ctx.current_gc_epoch);
         }
         Partition& partition = bm.getPartition(pid);
         {
            std::lock_guard<instrumented_mutex> _m(partition.ht_mutex);
            auto frame_handler = partition.io_ht.lookup(pid);
            if (frame_handler && location != 0) {
               if (frame_handler.frame().inserted_by_bg_page_fixer) {
                  // Another bg page fixer moves it, its block got listed twice. Waiting for it could deadlock.
                  COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_io_frame_busy++; }
                  record_retries();
                  jumpmu_return TreeLookup::SKIP;
               }
               // Retried once the page is cached, the worker needs the parent latch to swizzle it.
               COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_io_frame_busy++; }
               record_retries();
               jumpmu_return TreeLookup::RETRY;
            } else if (frame_handler) {
               // A worker is loading the page and applies its log records, the page is cached afterwards.
               COUNTERS_BLOCK(gc)
               {
                  GCCounters::myCounters().hot_fixed++;
                  GCCounters::myCounters().tree_io_frame_busy++;
               }
               ctx.hot_pids.push_back({pid, nullptr});
               record_retries();
               jumpmu_return TreeLookup::SKIP;
            } else if (location != 0 && bm.compressed_space->location(pid) != location) {
               // Moved since it was read: loaded, written back elsewhere and evicted again.
               ensure(!dirty);
               COUNTERS_BLOCK(gc) { GCCounters::myCounters().dead_pages++; }
               record_retries();
               jumpmu_return TreeLookup::SKIP;
            } else {
               insertFixerIOFrame(partition, pid, stats.io_frames_held);
            }
         }
         if (!dirty) {
            COUNTERS_BLOCK(gc)
            {
               GCCounters::myCounters().clean++;
               GCCounters::myCounters().tree_swip_clean++;
            }
            record_retries();
            jumpmu_return TreeLookup::MOVE;
         }
         const u8 nb_log_records = swip.pendingLSNCount();
         ensure(nb_log_records > 0 && nb_log_records <= btree::BTreeNode::max_pending_lsn);
         LID* lsn_list = ctx.parent_lsns[idx];
         std::memcpy(lsn_list, swip.pendingLSNs(), nb_log_records * sizeof(LID));
         if (bm.track_discard_state) {
            // The page providers still maintain the page state, it must agree with the parent. It is latched as
            // registerPageToFix() does, ackFixedPage() unlocks it clean.
            PageState& page_state = bm.discard_state[pid];
            ensure(page_state.isDiscarded());
            ensure_equal(page_state.getLogID(), ctx.currently_reclaiming_log_id);
            const auto [state_last_lsn, state_nb_log_records] = page_state.getLocked();
            ensure_equal(state_nb_log_records, nb_log_records);
            ensure_equal(state_last_lsn, lsn_list[nb_log_records - 1]);
         }
         // From now on the workers find a clean swip, they read the fixed page once the IO frame is released.
         // The persisted parent holds the stale dirty swip, as on a discard (see the page provider).
         swip.evict(pid);
         parent_handler->parent_bf->markUnDiscardable(INNER_NODE);
         parent_handler->parent_bf->page.PLSN++;
         info.lsn_list = lsn_list;
         info.last_lsn = lsn_list[nb_log_records - 1];
         info.nb_log_records = nb_log_records;
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_claimed++; }
         record_retries();
         jumpmu_return TreeLookup::FIX;
      }
      jumpmuCatch()
      {
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_retries++; }
         if ((++retries % (u64(1) << 20)) == 0) {
            LOG_WARN(bm.logger, "ru_gc_%u: %lu retries looking up pid %lu (dt %u) of RU epoch %lu in its tree, holding %ld IO frames", ctx.gc_id,
                     retries, pid, static_cast<u32>(page.dt_id), ctx.current_gc_epoch, stats.io_frames_held.load());
         }
      }
   }
}
// -------------------------------------------------------------------------------------
// FLAGS_compress_pages. Every live page of the blocks is decompressed and looked up in its B-Tree: the discarded ones are
// fixed and recompressed, the clean ones copied as is, both to blocks of the open RU epoch (out of place), the cached
// ones are moved from their frame (tryMoveHotPage()). The pages that cannot be moved right away are retried, blocking,
// before the batch ends. The blocks are freed as their last live page leaves them, into the free list of this thread,
// so the next batch reuses them first.
void RUBackgroundPageFixer::moveBlocksBatch(ThreadContext& ctx, const PID* blocks, u64 count)
{
   ensure_lte(count, ThreadContext::compressed_batch_blocks);
   ensure(ctx.to_fix_pids.empty() && ctx.moved.empty() && ctx.deferred_pages.empty());
   CompressedSpace& space = *bm.compressed_space;
   const ru_epoch_t gc_ru_epoch = ctx.current_gc_epoch;
   u64 read_blocks[ThreadContext::compressed_batch_blocks];
   u32 to_read = 0;
   for (u64 i = 0; i < count; ++i) {
      const u64 block = blocks[i];
      ensure_lt(block, bm.max_pid);
      if (space.blockRUEpoch(block) != gc_ru_epoch) {
         // Overwritten since, its copy in this RU epoch is already invalid.
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().blocks_overwritten++; }
         continue;
      }
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.r_ring);
      ensure(sqe != nullptr);
      io_uring_prep_read(sqe, bm.ssd_fd, ctx.read_blocks.get() + u64(to_read) * PAGE_SIZE, PAGE_SIZE, block * PAGE_SIZE);
      io_uring_sqe_set_data64(sqe, to_read);
      read_blocks[to_read++] = block;
   }
   if (to_read == 0) {
      return;
   }
   setPhase(ctx, Phase::READ);
   int s = io_uring_submit_and_wait(&ctx.r_ring, to_read);
   ensure_equal(s, static_cast<int>(to_read));
   struct io_uring_cqe* cqe;
   u32 head;
   u32 completed = 0;
   io_uring_for_each_cqe(&ctx.r_ring, head, cqe)
   {
      ensure_equal(cqe->res, PAGE_SIZE);
      ++completed;
   }
   ensure_equal(completed, to_read);
   io_uring_cq_advance(&ctx.r_ring, to_read);
   COUNTERS_BLOCK(gc)
   {
      GCCounters::myCounters().blocks_read += to_read;
      GCCounters::myCounters().pages_read += to_read;
   }
   // -------------------------------------------------------------------------------------
   setPhase(ctx, Phase::LOOKUP);
   for (u32 r = 0; r < to_read; ++r) {
      const u64 block = read_blocks[r];
      // The writers set the RU epoch of a block before writing it: unchanged, the block was not written during the read.
      if (space.blockRUEpoch(block) != gc_ru_epoch) {
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().blocks_overwritten++; }
         continue;
      }
      const u8* buffer = ctx.read_blocks.get() + u64(r) * PAGE_SIZE;
      const u64 address = block * PAGE_SIZE;
      // An uncompressed page fills its block and has no header, it is recognized by the page map pointing to it.
      const auto& raw = *reinterpret_cast<const BufferFrame::Page*>(buffer);
      const PID raw_pid = raw.magic_debugging_number;
      if (raw_pid < bm.pid_space && space.location(raw_pid) == cpid::make(address, PAGE_SIZE)) {
         movePage(ctx, raw_pid, cpid::make(address, PAGE_SIZE), buffer, raw.ru_epoch);
         continue;
      }
      // The compressed pages are packed from the start of the block, the tail is zeroed.
      u32 offset = 0;
      while (offset + sizeof(compression::CompressedPageHeader) <= PAGE_SIZE) {
         const auto header = compression::header(buffer + offset);
         const u32 length = header.length();
         if (length == 0) {
            break;
         }
         ensure(length > sizeof(compression::CompressedPageHeader) && length < PAGE_SIZE && offset + length <= PAGE_SIZE);
         movePage(ctx, header.pid(), cpid::make(address + offset, length), buffer + offset, header.ru_epoch);
         offset += length;
      }
   }
   flushPacker(ctx);
   retryDeferredPages(ctx);
}
// -------------------------------------------------------------------------------------
// The copy of pid at location (src, as read from the SSD), written to ru_epoch.
void RUBackgroundPageFixer::movePage(ThreadContext& ctx, PID pid, PID location, const u8* src, ru_epoch_t ru_epoch)
{
   CompressedSpace& space = *bm.compressed_space;
   if (ru_epoch != ctx.current_gc_epoch) {
      // An uncompressed page rewritten in place to a newer RU epoch since.
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().tree_other_ru_epoch++; }
      return;
   }
   if (pid >= bm.pid_space || space.location(pid) != location) {
      // Deleted or rewritten elsewhere since.
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().dead_pages++; }
      return;
   }
   const u32 length = cpid::length(location);
   BufferFrame& bf = ctx.buf_pages[0];
   compression::decompressPage(src, length, bf.page);
   ensure(ctx.to_fix_pids.empty());
   ctx.to_fix_pids.emplace_back(pid, nullptr, INVALID_LSN, INVALID_LSN, 0);
   // A log_only pass does not reclaim the RU epoch, there is no point in moving its clean pages.
   TreeLookup outcome = lookUpPageInTree(ctx, 0, location, /* claim_clean */ !ctx.log_only);
   if (outcome == TreeLookup::HOT && !FLAGS_bg_fixer_write_back_hot_pages) {
      // Left in place, it pins its block.
      outcome = TreeLookup::SKIP;
   }
   if (outcome != TreeLookup::SKIP && outcome != TreeLookup::RETRY && ctx.packer->full()) {
      // The IO frame of this page is held, but it is not part of the flush.
      flushPacker(ctx);
   }
   ru_epoch_t written_ru_epoch;
   if (outcome == TreeLookup::HOT) {
      if (tryMoveHotPage(ctx, *ctx.lookup_hot_bf, pid, location, src) == HotPage::RETRY) {
         outcome = TreeLookup::RETRY;
      }
      ctx.lookup_hot_bf = nullptr;
   }
   if (outcome == TreeLookup::RETRY) {
      ctx.deferred_pages.push_back({pid, location, src, ru_epoch});
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().batch_deferred++; }
   } else if (outcome == TreeLookup::FIX) {
      setPhase(ctx, Phase::FIX);
      const LID last_lsn = ctx.to_fix_pids[0].last_lsn;
      redoPage(ctx, 0);
      if (FLAGS_compression_zero_free_space) {
         reinterpret_cast<btree::BTreeNode*>(bf.page.dt)->zeroFreeSpace();
      }
      const PID new_location = ctx.packer->add(bf.page, /* compress */ true, location, written_ru_epoch);
      bf.page.ru_epoch = written_ru_epoch;
      bm.countPackedPage(bf.page);
      ctx.moved.push_back({pid, location, new_location, written_ru_epoch, true, last_lsn});
      setPhase(ctx, Phase::LOOKUP);
   } else if (outcome == TreeLookup::MOVE) {
      const PID new_location = ctx.packer->addVerbatim(src, length, written_ru_epoch);
      // As a write back of the page would: its copy in the reclaimed RU epoch gets invalid, the new one counts to the
      // RU epoch it is written to.
      bm.ru_discard_set[DEFAULT_RUH].at(ctx.current_gc_epoch).invalid.fetch_add(1);
      bf.page.ru_epoch = written_ru_epoch;
      bm.countPackedPage(bf.page);
      ctx.moved.push_back({pid, location, new_location, written_ru_epoch, false, INVALID_LSN});
      ++ctx.total_clean_copied;
      space.clean_copied_pages.fetch_add(1, std::memory_order_relaxed);
      COUNTERS_BLOCK(gc)
      {
         GCCounters::myCounters().clean_copied++;
         GCCounters::myCounters().clean_copied_bytes += length;
      }
   }
   ctx.to_fix_pids.clear();
   bf.reset();
}
// -------------------------------------------------------------------------------------
// Pre: bf was found through the parent swip of pid, src is the copy of pid at location read from the reclaimed RU epoch.
// The clean page is copied from src, it is the same page. Holding is_being_written_back keeps the page providers from
// writing the page back or evicting it, so the page map points to the new copy right away, as the page providers do.
// Whoever changes the location of a cached page (or rewrites an uncompressed one in place) holds is_being_written_back:
// once it is held, the copy at location is still the current one iff the page map points to it and its block is still
// in the reclaimed RU epoch.
RUBackgroundPageFixer::HotPage RUBackgroundPageFixer::tryMoveHotPage(ThreadContext& ctx, BufferFrame& bf, PID pid, PID location, const u8* src)
{
   CompressedSpace& space = *bm.compressed_space;
   jumpmuTry()
   {
      BMOptimisticGuard o_guard(bf.header.latch);
      if (bf.header.pid != pid || bf.isFree()) {
         // Evicted since, the lookup decides again.
         o_guard.recheck();
         jumpmu_return HotPage::RETRY;
      }
      if (bf.header.is_being_written_back.load(std::memory_order_acquire)) {
         o_guard.recheck();
         jumpmu_return HotPage::RETRY;
      }
      BMExclusiveGuard x_guard(o_guard);
      if (bf.header.is_being_written_back.load(std::memory_order_acquire)) {
         jumpmu_return HotPage::RETRY;
      }
      if (space.location(pid) != location || space.blockRUEpoch(cpid::block(location)) != ctx.current_gc_epoch) {
         // Written back since, it left the block.
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().dead_pages++; }
         jumpmu_return HotPage::DONE;
      }
      bf.header.is_being_written_back.store(true, std::memory_order_release);
      const u32 length = cpid::length(location);
      ru_epoch_t written_ru_epoch;
      PID new_location;
      if (bf.isDirty()) {
         // As tryWriteBackHotPage() does, the copy replaces the one read from the block in buf_pages[0].
         bm.prepareDirtyWriteBack(bf);
         ensure_equal(bf.page.magic_debugging_number, pid);
         ++bf.page.write_back_count;
         BufferFrame::Page& copy = ctx.buf_pages[0].page;
         DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, copy);
         auto& node = *reinterpret_cast<btree::BTreeNode*>(copy.dt);
         if (FLAGS_compression_zero_free_space) {
            node.zeroFreeSpace();
         }
         new_location = ctx.packer->add(copy, !bf.header.keep_in_memory && node.is_leaf, location, written_ru_epoch);
         ++ctx.total_fixed;
         COUNTERS_BLOCK(gc) { GCCounters::myCounters().hot_written++; }
      } else {
         // An uncompressed page is rewritten in place, as its write back would.
         if (length == PAGE_SIZE) {
            new_location = ctx.packer->add(src, /* compress */ false, location, written_ru_epoch);
         } else {
            new_location = ctx.packer->addVerbatim(src, length, written_ru_epoch);
         }
         // Like a clean copy (MOVE): its copy in the reclaimed RU epoch gets invalid.
         bm.ru_discard_set[DEFAULT_RUH].at(ctx.current_gc_epoch).invalid.fetch_add(1);
         ++ctx.total_clean_copied;
         space.clean_copied_pages.fetch_add(1, std::memory_order_relaxed);
         COUNTERS_BLOCK(gc)
         {
            GCCounters::myCounters().clean_copied++;
            GCCounters::myCounters().clean_copied_bytes += length;
            GCCounters::myCounters().hot_moved++;
         }
      }
      bf.page.ru_epoch = written_ru_epoch;
      bm.countPackedPage(bf.page);
      const PID previous = space.exchangeLocation(pid, new_location);
      ensure_equal(previous, location);
      if (previous != new_location) {
         space.free(previous);
      }
      ctx.hot_bfs.push_back(&bf);
      jumpmu_return HotPage::WRITING;
   }
   jumpmuCatch()
   {
      return HotPage::RETRY;
   }
}
// -------------------------------------------------------------------------------------
// The deferred pages are looked up again from their copy in read_blocks, they may have been cached, evicted or moved
// since. Nothing is held while waiting: the packer is flushed first, so no worker waits on an IO frame of this thread.
void RUBackgroundPageFixer::retryDeferredPages(ThreadContext& ctx)
{
   if (ctx.deferred_pages.empty()) {
      return;
   }
   const Phase phase = static_cast<Phase>(stats.threads[ctx.gc_id].phase.load(std::memory_order_relaxed));
   setPhase(ctx, Phase::RETRY);
   u64 rounds = 0;
   while (!ctx.deferred_pages.empty()) {
      ensure(ctx.retried_pages.empty());
      ctx.retried_pages.swap(ctx.deferred_pages);
      for (const auto& page : ctx.retried_pages) {
         waitForWorkerIOFrame(page.pid);
         movePage(ctx, page.pid, page.location, page.src, page.ru_epoch);
      }
      ctx.retried_pages.clear();
      setPhase(ctx, Phase::RETRY);
      flushPacker(ctx);
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().batch_retry_rounds++; }
      if (!ctx.deferred_pages.empty()) {
         if ((++rounds % (u64(1) << 20)) == 0) {
            LOG_WARN(bm.logger, "ru_gc_%u: %lu rounds retrying %lu pages of a batch of RU epoch %lu (first pid %lu)", ctx.gc_id, rounds,
                     ctx.deferred_pages.size(), ctx.current_gc_epoch, ctx.deferred_pages.front().pid);
         }
         std::this_thread::yield();
      }
   }
   setPhase(ctx, phase);
}
// -------------------------------------------------------------------------------------
// Same protocol as a worker waiting for a page another worker reads (see BufferManager::resolveSwip()).
void RUBackgroundPageFixer::waitForWorkerIOFrame(PID pid)
{
   Partition& partition = bm.getPartition(pid);
   std::unique_lock<instrumented_mutex> g_guard(partition.ht_mutex);
   auto frame_handler = partition.io_ht.lookup(pid);
   if (!frame_handler) {
      return;
   }
   IOFrame& io_frame = frame_handler.frame();
   if (io_frame.state != IOFrame::STATE::READING || io_frame.inserted_by_bg_page_fixer) {
      // READY: loaded, the next worker resolving the swip swizzles it.
      return;
   }
   io_frame.readers_counter++;
   g_guard.unlock();
   io_frame.mutex.lock();
   io_frame.mutex.unlock();
   if (io_frame.readers_counter.fetch_add(-1) == 1) {
      g_guard.lock();
      if (io_frame.readers_counter == 0) {
         partition.io_ht.remove(pid);
      }
   }
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::flushPacker(ThreadContext& ctx)
{
   BlockPacker& packer = *ctx.packer;
   const u64 blocks = packer.blocksCount();
   if (blocks == 0) {
      ensure(ctx.moved.empty() && ctx.hot_bfs.empty());
      return;
   }
   const Phase phase = static_cast<Phase>(stats.threads[ctx.gc_id].phase.load(std::memory_order_relaxed));
   setPhase(ctx, Phase::WRITE_BACK);
   packer.seal();
   // Before the writes, see moveBlocksBatch().
   bm.countPackedBlocks(packer);
   for (u64 b_i = 0; b_i < blocks; b_i++) {
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.w_ring);
      ensure(sqe != nullptr);
      nvme_io_uring_prep_write(sqe, bm.ssd_fd, packer.block(b_i), PAGE_SIZE, packer.blockOffset(b_i), DEFAULT_RUH);
      io_uring_sqe_set_data64(sqe, b_i);
   }
   int s = io_uring_submit_and_wait(&ctx.w_ring, blocks);
   ensure_equal(s, static_cast<int>(blocks));
   struct io_uring_cqe* cqe;
   u32 head;
   u32 completed = 0;
   io_uring_for_each_cqe(&ctx.w_ring, head, cqe)
   {
      ensure_equal(cqe->res, FLAGS_enable_fdp ? 0 : static_cast<s32>(PAGE_SIZE));
      ++completed;
   }
   ensure_equal(completed, static_cast<u32>(blocks));
   io_uring_cq_advance(&ctx.w_ring, blocks);
   packer.reset();
   // -------------------------------------------------------------------------------------
   // The moved pages: the workers waiting on their IO frame read the new copy. The old one is freed, its block goes
   // to the free list of this thread once its last live page left it.
   CompressedSpace& space = *bm.compressed_space;
   for (const auto& moved : ctx.moved) {
      const PID previous = space.exchangeLocation(moved.pid, moved.new_location);
      ensure_equal(previous, moved.old_location);
      space.free(previous);
      if (moved.fixed && bm.track_discard_state) {
         bm.discard_state[moved.pid].unlockClean(moved.last_lsn, cr::LogManager::getLogID(moved.ru_epoch), 0);
      }
      releaseIOFrame(moved.pid);
      COUNTERS_BLOCK(gc)
      {
         if (moved.fixed) {
            GCCounters::myCounters().total_fixed++;
         }
         GCCounters::myCounters().pages_written++;
      }
   }
   ctx.moved.clear();
   for (BufferFrame* bf : ctx.hot_bfs) {
      ackHotPage(*bf);
   }
   ctx.hot_bfs.clear();
   setPhase(ctx, phase);
}
// -------------------------------------------------------------------------------------
// The blocks of the chain that are still valid on the SSD hold live pages left in place (hot pages without
// FLAGS_bg_fixer_write_back_hot_pages, pages whose parent was not found, a block listed in the chain that the fixers
// skipped), or are free but not overwritten yet (the leftover of the pools, the blocks freed into the free lists of
// other threads). The LBA chain blocks are counted in block_total only.
void RUBackgroundPageFixer::reportBlocks(ThreadContext& ctx, RUEpochDiscardSet& set)
{
   CompressedSpace& space = *bm.compressed_space;
   const ru_epoch_t gc_ru_epoch = ctx.current_gc_epoch;
   u64 valid_live = 0, valid_free = 0;
   for (const PID block : set.lba_chain_entries) {
      if (space.blockRUEpoch(block) == gc_ru_epoch) {
         if (space.isFreeBlock(block)) {
            valid_free++;
         } else {
            valid_live++;
         }
      }
   }
   const s32 block_total = set.block_total.load();
   const s32 block_invalid = set.block_invalid.load();
   LOG_INFO(bm.logger,
            "GC epoch %lu blocks : %d written, %d invalidated (%.1f%%), still valid %lu with live pages and %lu free not yet "
            "overwritten, %ld others (LBA chain blocks), %lu blocks listed in the chain, %d clean pages copied, %d fixed",
            gc_ru_epoch, block_total, block_invalid, block_total ? block_invalid * 100.0 / block_total : 0.0, valid_live, valid_free,
            static_cast<s64>(block_total) - block_invalid - static_cast<s64>(valid_live + valid_free), set.lba_chain_entries.size(),
            set.total_clean_copied.load(), set.total_fixed.load());
}
// -------------------------------------------------------------------------------------
// FLAGS_bg_page_fixer_tree_lookup_state_filter: returns false if the page state rules out a discard into the log being
// reclaimed, so the page is not read. Same reasoning as needsFix(): a clean page only moves to cached, a page
// discarded into another log was written back to another RU epoch. Cached and NOT_SURE pages are still read and looked
// up. The state is loaded once, it may change under us (unlike getLogID() this does not assert the state).
bool RUBackgroundPageFixer::mayBeDiscardedInLog(ThreadContext& ctx, PID pid)
{
   const u64 v = bm.discard_state[pid].raw.load(std::memory_order_acquire);
   if (v == 0) {
      // Free
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().filter_free++; }
      return false;
   }
   const u64 state = (v & PageState::state_mask) >> PageState::state_shift;
   if (state == PageState::CLEAN) {
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().filter_clean++; }
      return false;
   }
   if (state == PageState::DISCARDED && ((v & PageState::log_id_mask) >> PageState::log_id_shift) != ctx.currently_reclaiming_log_id) {
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().filter_other_log++; }
      return false;
   }
   COUNTERS_BLOCK(gc) { GCCounters::myCounters().filter_passed++; }
   return true;
}
// -------------------------------------------------------------------------------------
// Returns true if the page may be discarded with log records in the log being reclaimed. A cached page is queued in
// hot_pids instead.
bool RUBackgroundPageFixer::needsFix(ThreadContext& ctx, PID pid)
{
   auto& page_state = bm.discard_state[pid];

   // It is safe to continue if the page is clean. In fact, from a clean state the page
   // can only concurrently move to a cached state (in the buffer pool). It cannot get
   // concurrently discarded again because pages that are belonging to an ru_epoch older
   // or equal to the currently reclaiming ru do not get evicted.
   // It is also possible that the page is clean but in a newer RU. In that case the page
   // can get discarded and we will double check for that after acquiring the latching the page state.
   if (page_state.isClean()) {
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().clean++; }
      return false;
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
      ctx.hot_pids.push_back({pid, nullptr});
      return false;
   }

   // The page is Free when we it was reclaimed but its pid is still not reused.
   // Even if the page id is reused we can later find out by checking whether the RU epoch
   // stored in the page is the same as the RU epoch we're currentlty reclaiming.
   // There is a case when the page is reclaimed and the RU is the same, which is after the
   // pid is allocated and before the page is written to storage, in that case the page is
   // in HOT state and we should handle it separately.
   if (page_state.isFree()) {
      return false;
   }
   return true;
}
// -------------------------------------------------------------------------------------
// Issues the read of the page registered by registerPageToFix() at index pages_to_fix, submits every half batch.
void RUBackgroundPageFixer::issueFixRead(ThreadContext& ctx, PID pid, u64& pages_to_fix, u64& submitted)
{
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
// -------------------------------------------------------------------------------------
// Locks the discarded page and inserts an IO frame for it so that the workers wait for it
// to be fixed. Returns false if the page must not be fixed by us.
bool RUBackgroundPageFixer::registerPageToFix(ThreadContext& ctx, LID first_lsn, PID pid, u64 idx)
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
      ctx.hot_pids.push_back({pid, nullptr});
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
   insertFixerIOFrame(partition, pid, stats.io_frames_held);

   auto p = bm.discard_state[pid].getLocked();
   ensure(bm.discard_state[pid].isDiscarded());
   nb_log_records = p.second;
   if (PageState::holdsLSNArray(nb_log_records)) {
      lsn_list = reinterpret_cast<LID*>(p.first);
      last_lsn = lsn_list[nb_log_records - 1];
   } else {
      // With FLAGS_discard_lsns_in_parent and more than one log record lsn_list stays null,
      // fixPage() walks the log chain backwards from last_lsn.
      last_lsn = p.first;
   }
   ensure_equal(ctx.to_fix_pids.size(), idx);
   ctx.to_fix_pids.emplace_back(pid, lsn_list, first_lsn, last_lsn, nb_log_records);
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
      waitForFixedPagesWriteBack(ctx);
   }
   ctx.to_fix_pids.clear();
}
// -------------------------------------------------------------------------------------
// Waits for the write backs issued by redoAndWriteBack() (ctx.actually_fixed of them) and acknowledges them.
void RUBackgroundPageFixer::waitForFixedPagesWriteBack(ThreadContext& ctx)
{
   setPhase(ctx, Phase::WRITE_BACK);
   // TODO(mfd) : Submit and wait.
   u32 ready = io_uring_peek_batch_cqe(&ctx.w_ring, ctx.cqes.get(), ctx.actually_fixed);

   for (u32 i = 0; i < ready; ++i) {
      ackFixedPage(ctx, ctx.cqes[i]);
   }
   if (ready > 0)
      io_uring_cq_advance(&ctx.w_ring, ready);
   // Wait for the remaining part, and do the same thing.
   u32 remaining = ctx.actually_fixed - ready;
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
   auto& page = ctx.buf_pages[idx].page;
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
   redoAndWriteBack(ctx, idx, data);
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::redoAndWriteBack(ThreadContext& ctx, u64 idx, u64 data)
{
   const PID fixed_pid = data & 0x0000FFFFFFFFFFFF;
   ensure_equal(ctx.to_fix_pids[idx].pid, fixed_pid);
   redoPage(ctx, idx);
   auto& page = ctx.buf_pages[idx].page;
   struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.w_ring);
   ensure(sqe != nullptr);
   nvme_io_uring_prep_write(sqe, bm.ssd_fd, (void*)page, PAGE_SIZE, fixed_pid * PAGE_SIZE, page.ruh);
   io_uring_sqe_set_data64(sqe, data);
   // TODO(mfd) : Amortize
   int s = io_uring_submit(&ctx.w_ring);
   ensure(s == 1);
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::redoPage(ThreadContext& ctx, u64 idx)
{
   [[maybe_unused]] const PID fixed_pid = ctx.to_fix_pids[idx].pid;
   const LID last_lsn = ctx.to_fix_pids[idx].last_lsn;
   auto& bf = ctx.buf_pages[idx];
   auto& page = bf.page;

   LID lsn = last_lsn;
   std::vector<cr::WALEntry*> to_apply_log_records;
   ensure(lsn != INVALID_LSN);

   const auto& page_to_fix_info = ctx.to_fix_pids[idx];

#if DEBUG_BACKWARD_LOG_CHAIN_TRAVERSE
   ensure(page_to_fix_info.nb_log_records > 0);
   while (lsn != INVALID_LSN) {
      ensure_equal(lsnLogID(lsn), ctx.currently_reclaiming_log_id);
      u64 off = lsnOffset(lsn);
      auto* entry = reinterpret_cast<cr::WALEntry*>(&ctx.log_records[off]);

      bool ok = bm.logRecordSanityCheck(entry, page, lsn);
      ensure(ok);

      to_apply_log_records.push_back(entry);

      if (to_apply_log_records.size() > FLAGS_max_log_records_to_discard) {
         printf("[ERR] Number of logs to apply in the global state : %u\n", page_to_fix_info.nb_log_records);
         printf("[ERR] curr lsn = %s, prev lsn = %s\n", lsnToString(lsn).c_str(), lsnToString(entry->prev_lsn).c_str());
         for (const auto& e : to_apply_log_records) {
            reinterpret_cast<cr::WALDTEntry*>(e)->dump();
         }
         page.dump();
         if (bm.track_discard_state) bm.discard_state[fixed_pid].dump();
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
   if (page_to_fix_info.lsn_list == nullptr) {
      // Only the last LSN is known, the others are in the parent.
      for (u8 i = 0; i < page_to_fix_info.nb_log_records; ++i) {
         ensure(lsn != INVALID_LSN);
         ensure_equal(lsnLogID(lsn), ctx.currently_reclaiming_log_id);
         u64 off = lsnOffset(lsn);
         auto* entry = reinterpret_cast<cr::WALEntry*>(&ctx.log_records[off]);

         bool ok = bm.logRecordSanityCheck(entry, page, lsn);
         ensure(ok);

         to_apply_log_records.push_back(entry);
         lsn = entry->prev_lsn;
      }
      std::reverse(to_apply_log_records.begin(), to_apply_log_records.end());
   } else {
      for (u8 i = 0; i < page_to_fix_info.nb_log_records; ++i) {
         LID lsn = page_to_fix_info.lsn_list[i];
         ensure_equal(lsnLogID(lsn), ctx.currently_reclaiming_log_id);
         u64 off = lsnOffset(lsn);
         auto* entry = reinterpret_cast<cr::WALEntry*>(&ctx.log_records[off]);

         bool ok = bm.logRecordSanityCheck(entry, page, lsn);
         ensure(ok);

         to_apply_log_records.push_back(entry);
      }
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

   if (PageState::holdsLSNArray(to_apply_log_records.size()))
      bm.randomAllocator().free(ctx.to_fix_pids[idx].lsn_list, to_apply_log_records.size());

#if DEBUG_BACKWARD_LOG_CHAIN_TRAVERSE
   goto success;

fail:
   printf("[ERR] Number of logs to apply in the global state : %u\n", page_to_fix_info.nb_log_records);
   for (const auto& e : to_apply_log_records) {
      reinterpret_cast<cr::WALDTEntry*>(e)->dump();
   }
   page.dump();
   if (bm.track_discard_state) bm.discard_state[fixed_pid].dump();
   printf("[ERR] Total Fixed %lu \n", ctx.total_fixed);
   page_to_fix_info.dump();
   __asm__ volatile("int3");

success:

#endif
}
// -------------------------------------------------------------------------------------
// Pages of the reclaimed log that were cached when scanned. Their latest version depends on the log
// records of the RU epoch, so they must be written back before the log gets reset.
// Pre: fixPages() drained both rings and reset buf_pages, so they are free to hold the page copies.
// We hold neither an IO frame nor a PageState latch, so waiting on a frame latch cannot deadlock.
void RUBackgroundPageFixer::fixHotPages(ThreadContext& ctx)
{
   ensure(ctx.to_fix_pids.empty());
   ensure(ctx.hot_bfs.empty());
   if (!FLAGS_bg_fixer_write_back_hot_pages) {
      ctx.hot_pids.clear();
      return;
   }
   for (const HotPageRef& hot : ctx.hot_pids) {
      if (FLAGS_compress_pages && ctx.packer->full()) {
         writeBackHotPages(ctx);
      }
      switch (tryWriteBackHotPage(ctx, hot)) {
         case HotPage::DONE:
            break;
         case HotPage::RETRY:
            ctx.deferred_hot_pids.push_back(hot);
            COUNTERS_BLOCK(gc) { GCCounters::myCounters().hot_deferred++; }
            break;
         case HotPage::WRITING:
            if (ctx.hot_bfs.size() == ThreadContext::batch_size) {
               writeBackHotPages(ctx);
            }
            break;
      }
   }
   writeBackHotPages(ctx);
   ctx.hot_pids.clear();
   stats.threads[ctx.gc_id].deferred_hot.store(ctx.deferred_hot_pids.size(), std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
// Never blocks: the guards jump when the frame is latched, and the page is retried later.
// With compression the page goes through the packer, like a write back of the page providers.
RUBackgroundPageFixer::HotPage RUBackgroundPageFixer::tryWriteBackHotPage(ThreadContext& ctx, const HotPageRef& hot)
{
   const PID pid = hot.pid;
   BufferFrame* bf = hot.bf;
   if (bf == nullptr) {
      bf = bm.discard_state[pid].getBufferFrame();
   }
   if (bf == nullptr) {
      PageState& page_state = bm.discard_state[pid];
      // bf == nullptr means the page is no longer hot.
      // 2 possible cases:
      // 1. Page is already written back in a clean state  (Most likely).
      // 2. The page is discarded: This happens when the page provider threads reads
      //    the reclaiming_ru_epoch just before it got incremented by the bg_fixer_thread.
      //    The pp threads then discarded the page successfully.
      //    In this case we just need to try again. We're guarentted that this case will never
      //    happen again for this page.
      // Read the state once, it may change concurrently.
      const u64 v = page_state.raw.load(std::memory_order_acquire);
      const bool discarded = (v & PageState::state_mask) == PageState::state_discarded_mask;
      const u64 log_id = (v & PageState::log_id_mask) >> PageState::log_id_shift;
      if (discarded && log_id == ctx.currently_reclaiming_log_id) {
         // A worker is reading the page (registerPageToFix() saw its IO frame), it becomes hot soon.
         // TODO(mfd) : Or the PPT discarded it after we saw it hot (see the reclaiming_ru_epoch
         // recheck under the log mutex). In that case, it should be fixed like a discarded page.
         return HotPage::RETRY;
      }
      // CLEAN: written back or evicted clean. Free: deleted. DISCARDED in another log: written back
      // in a newer RU epoch and discarded since.
      return HotPage::DONE;
   }
   jumpmuTry()
   {
      BMOptimisticGuard o_guard(bf->header.latch);
      if (bf->header.pid != pid || bf->isFree()) {
         o_guard.recheck();
         if (hot.bf != nullptr) {
            // Found through its parent: the page got evicted since, after its write back if it was dirty (the pages of
            // the reclaimed RU epoch are not discarded anymore).
            jumpmu_return HotPage::DONE;
         }
         // The frame got evicted and reused meanwhile, look at the page state again.
         // Why I need to retry here ?
         jumpmu_return HotPage::RETRY;
      }
      // Writing back a page that did not need it is only an extra write, never incorrect. This covers
      // a pid freed and reused within the same RU epoch.
      // An UNMAPPED page was never written back, its log records are in the sink logs.
      const ru_epoch_t ru_epoch = bf->page.ru_epoch;
      if (ru_epoch == UNMAPPED_RU_EPOCH || ru_epoch > ctx.current_gc_epoch || !bf->isDirty()) {
         o_guard.recheck();
         jumpmu_return HotPage::DONE;
      }
      if (bf->header.is_being_written_back.load(std::memory_order_acquire)) {
         // The prologue already mapped the copy being written to a newer RU epoch. Retry until the
         // write returns, the page must be persisted before the log is reset.
         o_guard.recheck();
         jumpmu_return HotPage::RETRY;
      }
      BMExclusiveGuard x_guard(o_guard);
      // Not just store. It may have changed meawhile after the first check returned false
      // and before I acquire the latch.
      if (bf->header.is_being_written_back.load(std::memory_order_acquire)) {
         jumpmu_return HotPage::RETRY;
      }
      bf->header.is_being_written_back.store(true, std::memory_order_release);
      // Maps the page to a newer RU epoch, so the log records of the updates following the copy
      // go to the log of that RU epoch.
      bm.prepareDirtyWriteBack(*bf);
      ensure_equal(bf->page.magic_debugging_number, pid);
      ++bf->page.write_back_count;
      if (FLAGS_compress_pages) {
         // The copy is packed right away, buf_pages[0] is only a scratch page. As the page providers do, the page map
         // points to the new copy before it is written: the page is cached, nobody reads it from the SSD.
         BufferFrame::Page& copy = ctx.buf_pages[0].page;
         DTRegistry::global_dt_registry.checkpoint(bf->page.dt_id, *bf, copy);
         auto& node = *reinterpret_cast<btree::BTreeNode*>(copy.dt);
         if (FLAGS_compression_zero_free_space) {
            node.zeroFreeSpace();
         }
         CompressedSpace& space = *bm.compressed_space;
         ru_epoch_t written_ru_epoch;
         const PID location = ctx.packer->add(copy, !bf->header.keep_in_memory && node.is_leaf, space.location(pid), written_ru_epoch);
         bf->page.ru_epoch = written_ru_epoch;
         bm.countPackedPage(bf->page);
         const PID previous = space.exchangeLocation(pid, location);
         if (previous != location) {
            space.free(previous);
         }
         ++ctx.total_fixed;
      } else {
         // Unswizzles the children of inner pages in the copy.
         DTRegistry::global_dt_registry.checkpoint(bf->page.dt_id, *bf, ctx.buf_pages[ctx.hot_bfs.size()].page);
      }
      ctx.hot_bfs.push_back(bf);
      jumpmu_return HotPage::WRITING;
   }
   jumpmuCatch()
   {
      // Acquiring Exclusive lock failed
      return HotPage::RETRY;
   }
}
// -------------------------------------------------------------------------------------
// Writes the copies made by tryWriteBackHotPage() and waits for them.
void RUBackgroundPageFixer::writeBackHotPages(ThreadContext& ctx)
{
   if (FLAGS_compress_pages) {
      flushPacker(ctx);
      return;
   }
   const u32 n = ctx.hot_bfs.size();
   if (n == 0) {
      return;
   }
   for (u32 i = 0; i < n; ++i) {
      auto& page = ctx.buf_pages[i].page;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx.w_ring);
      ensure(sqe != nullptr);
      nvme_io_uring_prep_write(sqe, bm.ssd_fd, page, PAGE_SIZE, page.magic_debugging_number * PAGE_SIZE, page.ruh);
      io_uring_sqe_set_data64(sqe, i);
   }
   // TODO(mfd) : submit and wait, single syscall.
   int s = io_uring_submit(&ctx.w_ring);
   ensure_equal(s, static_cast<int>(n));
   int rc = io_uring_wait_cqe_nr(&ctx.w_ring, ctx.cqes.get(), n);
   posix_check(rc == 0);
   struct io_uring_cqe* cqe;
   u32 head;
   u32 i = 0;
   io_uring_for_each_cqe(&ctx.w_ring, head, cqe)
   {
      ackHotPage(ctx, cqe);
      ++i;
   }
   ensure_equal(i, n);
   io_uring_cq_advance(&ctx.w_ring, n);
   ctx.total_fixed += n;
   ctx.hot_bfs.clear();
}
// -------------------------------------------------------------------------------------
// Same as the PPT when its write back returns. Not ackFixedPage(): a hot page has neither an IO frame
// nor a latched PageState.
void RUBackgroundPageFixer::ackHotPage(ThreadContext& ctx, struct io_uring_cqe* cqe)
{
   if (FLAGS_enable_fdp) {
      ensure_equal(cqe->res, 0);
   } else {
      ensure_equal(cqe->res, PAGE_SIZE);
   }
   const u64 idx = io_uring_cqe_get_data64(cqe);
   ackHotPage(*ctx.hot_bfs[idx]);
}
// -------------------------------------------------------------------------------------
void RUBackgroundPageFixer::ackHotPage(BufferFrame& bf)
{
   COUNTERS_BLOCK(gc)
   {
      GCCounters::myCounters().pages_written++;
   }
   Partition* reclaimed_in = nullptr;
   while (true) {
      jumpmuTry()
      {
         BMOptimisticGuard o_guard(bf.header.latch);
         BMExclusiveGuard x_guard(o_guard);
         ensure(bf.header.is_being_written_back);
         bf.header.is_being_written_back.store(false, std::memory_order_release);
         if (FLAGS_compress_pages && bf.isFree()) {
            // Deleted while being written back (see reclaimPage), as the page providers do: nothing references this copy.
            const PID pid = bf.header.pid;
            bm.compressed_space->free(bm.compressed_space->exchangeLocation(pid, 0));
            bm.recyclePID(pid, bf.page.ru_epoch, bf.page.ruh);
            bf.reset();
            reclaimed_in = &bm.getPartition(pid);
         }
         jumpmu_break;
      }
      jumpmuCatch() {}
   }
   if (reclaimed_in != nullptr) {
      reclaimed_in->dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
// The log of the RU epoch must not be reset while one of its hot pages is not yet written back.
void RUBackgroundPageFixer::drainDeferredHotPages(ThreadContext& ctx)
{
   u64 rounds = 0;
   while (!ctx.deferred_hot_pids.empty()) {
      ensure(ctx.hot_pids.empty());
      ctx.hot_pids.swap(ctx.deferred_hot_pids);
      fixHotPages(ctx);
      COUNTERS_BLOCK(gc) { GCCounters::myCounters().drain_rounds++; }
      if (!ctx.deferred_hot_pids.empty()) {
         if (++rounds % 65536 == 0) {
            LOG_INFO(bm.logger, "ru_gc_%u still waiting for %lu hot pages of RU epoch %lu", ctx.gc_id, ctx.deferred_hot_pids.size(), ctx.current_gc_epoch);
         }
         std::this_thread::yield();
      }
   }
}
// -------------------------------------------------------------------------------------
// A fixed page got written back: mark it clean and let the waiting workers read it.
void RUBackgroundPageFixer::ackFixedPage(ThreadContext& ctx, struct io_uring_cqe* cqe)
{
   if (FLAGS_enable_fdp) {
      ensure_equal(cqe->res, 0);
   } else {
      ensure_equal(cqe->res, PAGE_SIZE);
   }
   u64 data = io_uring_cqe_get_data64(cqe);
   PID pid = data & 0x0000FFFFFFFFFFFF;
   u64 idx = data >> 48;
   auto& bf = ctx.buf_pages[idx];
   auto& page = bf.page;
   ensure_equal(page.magic_debugging_number, pid);

   // The fixed page got written back, reset its discard count.
   if (bm.track_discard_state) {
      bm.discard_state[pid].unlockClean(ctx.to_fix_pids[idx].last_lsn, cr::LogManager::getLogID(page.ru_epoch), 0);
   }

   releaseIOFrame(pid);
   COUNTERS_BLOCK(gc)
   {
      GCCounters::myCounters().total_fixed++;
      GCCounters::myCounters().pages_written++;
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
   stats.io_frames_held.fetch_sub(1, std::memory_order_relaxed);
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

