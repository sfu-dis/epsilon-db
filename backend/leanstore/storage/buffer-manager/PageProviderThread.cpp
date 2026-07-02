#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "CustomSlabAllocator.hpp"
#include "Exceptions.hpp"
#include "Tracing.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct ppl_info {
   u32 log_id;
   PID pid;
   LID gsn;
   LID lsn;
};
// -------------------------------------------------------------------------------------
void BufferManager::pageProviderThread(u64 pp_id, u64 p_begin, u64 p_end)  // [p_begin, p_end)
{
   std::string thread_name("pp_" + std::to_string(p_begin) + "_" + std::to_string(p_end));
   pthread_setname_np(pthread_self(), thread_name.c_str());
   using Time = decltype(std::chrono::high_resolution_clock::now());
   // -------------------------------------------------------------------------------------
   leanstore::cr::CRManager::global->registerMeAsSpecialWorker();
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size);
   std::vector<BufferFrame*> cool_candidate_bfs, evict_candidate_bfs;
   // -------------------------------------------------------------------------------------
   std::vector<ppl_info> to_discard_queue;
   // -------------------------------------------------------------------------------------
   auto next_bf_range = [&]() {
      const u64 BATCH_SIZE = FLAGS_replacement_chunk_size;
      cool_candidate_bfs.clear();
      for (u64 i = 0; i < BATCH_SIZE; i++) {
         BufferFrame* r_bf = &randomBufferFrame();
         DO_NOT_OPTIMIZE(r_bf->header.state);
         cool_candidate_bfs.push_back(r_bf);
      }
      return;
   };
   // -------------------------------------------------------------------------------------
   auto unlock_discarded_pages_with_persisted_ppl = [&]() {
      if (FLAGS_per_page_logging) {
         std::vector<ppl_info> unpersisted_ppl;
         for (const auto& entry: to_discard_queue) {
            if (cr::LogManager::global->all_logs[entry.log_id].hardened_gsn.load(std::memory_order_acquire) >= entry.gsn) {
               discard_state[entry.pid].unlock();
            } else {
               unpersisted_ppl.push_back(entry);
            }
         }
         to_discard_queue = unpersisted_ppl;
      }
   };
   // -------------------------------------------------------------------------------------
   while (bg_threads_keep_running) {
      // Phase 1: unswizzle pages (put in the cooling stage)
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      volatile u64 failed_attempts =
          0;  // [corner cases]: prevent starving when free list is empty and cooling to the required level can not be achieved
#define repickIf(cond)                       \
   if (cond) {                               \
      failed_attempts = failed_attempts + 1; \
      jumpmu_continue;                       \
   }
      auto& current_partition = randomPartition();
      if ((current_partition.dram_free_list.counter < current_partition.free_bfs_limit) && failed_attempts < 10) {
         next_bf_range();
         while (cool_candidate_bfs.size()) {
            jumpmuTry()
            {
               BufferFrame* r_buffer = cool_candidate_bfs.back();
               cool_candidate_bfs.pop_back();
               COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
               // -------------------------------------------------------------------------------------
               BMOptimisticGuard r_guard(r_buffer->header.latch);
               repickIf(r_buffer->header.keep_in_memory || r_buffer->header.is_being_written_back || r_buffer->header.latch.isExclusivelyLatched());
               if (FLAGS_wal && FLAGS_wal_pwrite && (r_buffer->header.logging != nullptr)) {
                  // FIXME(mfd) : Account for the page that has changed from an active RU epoch to the collected RU epoch.
                  // TODO(mfd) : Monitor failures because of this.
                  repickIf(r_buffer->page.GSN > r_buffer->header.logging->hardened_gsn.load(std::memory_order_acquire));
               }
               // FIXME(mfd) : Temporarly avoiding evicting inner nodes.
               if (FLAGS_enable_discarding) {
                  auto node = reinterpret_cast<btree::BTreeNode*>(r_buffer->page.dt);
                  repickIf(!node->is_leaf);
               }
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               if (r_buffer->header.state == BufferFrame::STATE::COOL) {
                  evict_candidate_bfs.push_back(reinterpret_cast<BufferFrame*>(r_buffer));
                  repickIf(true);  // TODO: maybe without failed_attempts
               }
               repickIf(r_buffer->header.state != BufferFrame::STATE::HOT);
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
               // -------------------------------------------------------------------------------------
               bool all_children_evicted = true;
               bool picked_a_child_instead = false;
               [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
               COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
               getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
                  all_children_evicted &= swip.isEVICTED();  // Ignore when it has a child in the cooling stage
                  if (swip.isHOT()) {
                     BufferFrame* picked_child_bf = &swip.asBufferFrame();
                     r_guard.recheck();
                     picked_a_child_instead = true;
                     cool_candidate_bfs.push_back(picked_child_bf);
                     return false;
                  }
                  r_guard.recheck();
                  return true;
               });
               COUNTERS_BLOCK()
               {
                  iterate_children_begin = std::chrono::high_resolution_clock::now();
                  PPCounters::myCounters().iterate_children_ms +=
                      (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
               }
               repickIf(!all_children_evicted || picked_a_child_instead);
               // -------------------------------------------------------------------------------------
               [[maybe_unused]] Time find_parent_begin, find_parent_end;
               COUNTERS_BLOCK() { find_parent_begin = std::chrono::high_resolution_clock::now(); }
               DTID dt_id = r_buffer->page.dt_id;
               r_guard.recheck();
               ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, *r_buffer);
               // -------------------------------------------------------------------------------------
               if (FLAGS_optimistic_parent_pointer) {
                  if (parent_handler.is_bf_updated) {
                     r_guard.guard.version += 2;
                  }
               }
               // -------------------------------------------------------------------------------------
               paranoid(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
               paranoid(parent_handler.parent_guard.latch != reinterpret_cast<HybridLatch*>(0x99));
               COUNTERS_BLOCK()
               {
                  find_parent_end = std::chrono::high_resolution_clock::now();
                  PPCounters::myCounters().find_parent_ms +=
                      (std::chrono::duration_cast<std::chrono::microseconds>(find_parent_end - find_parent_begin).count());
               }
               // -------------------------------------------------------------------------------------
               r_guard.recheck();
               const SpaceCheckResult space_check_res = getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer);
               if (space_check_res == SpaceCheckResult::RESTART_SAME_BF || space_check_res == SpaceCheckResult::PICK_ANOTHER_BF) {
                  jumpmu_continue;
               }
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               // Suitable page founds, lets cool
               {
                  const PID pid = r_buffer->header.pid;
                  // r_x_guard can only be acquired and released while the partition mutex is locked
                  {
                     BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
                     BMExclusiveGuard r_x_guard(r_guard);
                     // -------------------------------------------------------------------------------------
                     paranoid(r_buffer->header.pid == pid);
                     paranoid(r_buffer->header.state == BufferFrame::STATE::HOT);
                     paranoid(r_buffer->header.is_being_written_back == false);
                     paranoid(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
                     paranoid(&parent_handler.swip.asBufferFrame() == r_buffer);
                     // -------------------------------------------------------------------------------------
                     r_buffer->header.state = BufferFrame::STATE::COOL;
                     parent_handler.swip.cool();  // Cool the pointing swip before unlocking the current bf
                  }
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter++; }
               }
               failed_attempts = 0;
            }
            jumpmuCatch() {}
         }
      }
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
      }
      // -------------------------------------------------------------------------------------
      // Phase 2:
      FreedBfsBatch freed_bfs_batch;
      auto evict_bf = [&](BufferFrame& bf, BMOptimisticGuard& c_guard, bool discard) {
         DTID dt_id = bf.page.dt_id;
         c_guard.recheck();
         ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, bf);
         // -------------------------------------------------------------------------------------
         if (FLAGS_optimistic_parent_pointer) {
            if (parent_handler.is_bf_updated) {
               c_guard.guard.version += 2;
            }
         }
         // -------------------------------------------------------------------------------------
         paranoid(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
         BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
         // The page must remain exclusively latched if the function return through
         // normal path and must release the latch if it jumps().
         // TODO(mfd) : Define a Guard with this behaviour if necessaray.
         // For now, make sure to manually release the latch before each non-local jump.
         c_guard.guard.toExclusive();
         ensure(&parent_handler.swip.asBufferFrameMasked() == &bf);
         // -------------------------------------------------------------------------------------
         if (FLAGS_crc_check && bf.header.crc) {
            ensure(utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE) == bf.header.crc);
         }
         // -------------------------------------------------------------------------------------
         ensure(!bf.isDirty() || discard);
         ensure(!bf.header.is_being_written_back);
         ensure(bf.header.state == BufferFrame::STATE::COOL);
         ensure(parent_handler.swip.isCOOL());
         ensure(bf.page.ru_epoch >= 0);
         // -------------------------------------------------------------------------------------
         const PID evicted_pid = bf.header.pid;
         const LID last_write_lsn = bf.page.last_written_lsn;
         if (discard) {
            if (!bf.isDiscardable()) {
               c_guard.guard.unlock();
               jumpmu::jump();
            }
            // TODO(mfd) : PARANOID_BLOCK()
            if (FLAGS_wal && FLAGS_wal_pwrite && !FLAGS_fake_log_reapply) {
               // ensure(last_write_lsn != INVALID_LSN);
               if (last_write_lsn == INVALID_LSN) {
                  bf.dump();
                  __asm__ volatile("int3");
               }
               u32 log_id = cr::LogManager::global->LSN2LogID(last_write_lsn);
               if (log_id != cr::LogManager::getLogID(bf.page.ru_epoch)) {
                  bf.dump();
                  raise(SIGTRAP);
               }
            }
            if (bf.page.ru_epoch <= reclaiming_ru_epoch.load(std::memory_order_acquire)) {
               c_guard.guard.unlock();
               jumpmu::jump();
            }
            // tryDiscard will fail only in those two cases.
            //  1. The GC thread is currently fixing the page (entry locked).
            //  2. The GC thread has already fixed the page (marked gc_fixed).
            // I think in both cases it is fine to just evict the page. Especially
            // In the second case since the page should be mapped now to a new RU epoch.
            // Be aware of deadlock between page latch and page state latch
            ensure(bf.header.pending_lsn_count > 0);
            ensure(bf.header.pending_lsn_count <= FLAGS_max_log_records_to_discard);
            ensure_equal(bf.header.pending_lsn[bf.header.pending_lsn_count - 1], last_write_lsn);
            ensure_equal(bf.header.pending_lsn_count, bf.page.PLSN - bf.header.last_written_plsn);
            LID *pending_lsn = nullptr;
            bool submitted_ppl = false;
            if (bf.header.pending_lsn_count == 1) {
               pending_lsn = bf.header.pending_lsn;
            } else {
               if (FLAGS_per_page_logging && bf.header.pending_lsn_count >= FLAGS_ppl_merge_threshold) {
                  bool ok = bf.submitPPLEntry();
                  if (!ok) {
                     // only fails when the log that is mapped to this page changes.
                     // just retry and next time I will write the page without trying to discard it.
                     assert(!bf.isDiscardable());
                     c_guard.guard.unlock();
                     jumpmu::jump();
                  }
                  ensure(bf.header.pending_lsn_count == 1);
                  pending_lsn = bf.header.pending_lsn;
                  submitted_ppl = true;
               } else {
                  pending_lsn = per_pp_allocator[pp_id].allocate(bf.header.pending_lsn_count);
                  ensure(pending_lsn != nullptr);
                  std::memcpy(pending_lsn, bf.header.pending_lsn, bf.header.pending_lsn_count * sizeof(LID));
               }
            }
            bool success = false;
            const u32 log_id = cr::LogManager::getLogID(bf.page.ru_epoch);
            if (submitted_ppl) {
               to_discard_queue.emplace_back(bf.header.logging->log_id, bf.header.pid, bf.ppl.header.gsn, bf.page.last_written_lsn);
               success = discard_state[evicted_pid].tryDiscard<true>(pending_lsn, bf.header.pending_lsn_count, log_id);
            } else {
               success = discard_state[evicted_pid].tryDiscard<false>(pending_lsn, bf.header.pending_lsn_count, log_id);
            }
            ensure(success); // because still I haven't implemeneted the HOT page reclaiming.
            if (!success) {
               // THINK(mfd) : How may this affect the PPL?
               to_discard_queue.pop_back();
               c_guard.guard.unlock();
               jumpmu::jump();
            }
            // The RU discard set identity may change meanwhile.
            // I think I should simply allow this, this is a very rare event, and these are 
            //  stats to approximate thresolhold of GC in RU epoch.
            ru_discard_set.data[bf.page.ru_epoch % max_open_ru_epochs].inserted.fetch_add(1);
            parent_handler.swip.evictAndMarkDirty(evicted_pid, bf.page.ru_epoch);
            COUNTERS_BLOCK(discarded_pages) { PPCounters::myCounters().discarded_pages++; }
         } else {
            parent_handler.swip.evict(evicted_pid);
            if (FLAGS_enable_discarding) {
               const u32 log_id = cr::LogManager::getLogID(bf.page.ru_epoch);
               discard_state[evicted_pid].getLocked();
               ensure(discard_state[evicted_pid].isHot());
               discard_state[evicted_pid].unlockClean(last_write_lsn, log_id);
            }
         }
         // -------------------------------------------------------------------------------------
         // Reclaim buffer frame
         bf.reset();
         bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
         bf.header.latch.mutex.unlock();
         // -------------------------------------------------------------------------------------
         freed_bfs_batch.add(bf);
         if (freed_bfs_batch.size() <= std::min<u64>(FLAGS_worker_threads, 128)) {
            freed_bfs_batch.push(current_partition);
         }
         // -------------------------------------------------------------------------------------
         if (FLAGS_pid_tracing) {
            Tracing::mutex.lock();
            if (Tracing::ht.contains(evicted_pid)) {
               std::get<1>(Tracing::ht[evicted_pid])++;
            } else {
               Tracing::ht[evicted_pid] = {dt_id, 1};
            }
            Tracing::mutex.unlock();
         }
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; }
      };
      // -------------------------------------------------------------------------------------
      for (volatile const auto& cooled_bf : evict_candidate_bfs) {
         jumpmuTry()
         {
            BMOptimisticGuard o_guard(cooled_bf->header.latch);
            // Check if the BF got swizzled in or unswizzle another time in another partition
            if (cooled_bf->header.state != BufferFrame::STATE::COOL ||
                cooled_bf->header.is_being_written_back) {  //  || getPartitionID(bf.header.pid) != p_i
               jumpmu_continue;
            }
            const PID cooled_bf_pid = cooled_bf->header.pid;
            const u64 p_i = getPartitionID(cooled_bf_pid);
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               Partition& partition = getPartition(p_i);
               bool success = partition.ht_mutex.try_lock();
               if (!success || partition.io_ht.lookup(cooled_bf_pid)) {
                  if (success) {
                     partition.ht_mutex.unlock();
                  }
                  jumpmu_continue;
               }
               assert(success);
               partition.ht_mutex.unlock();
            }
            // -------------------------------------------------------------------------------------
            if (cooled_bf->isDirty()) {
               if (FLAGS_enable_discarding
                  && cooled_bf->isDiscardable()
                  && reinterpret_cast<btree::BTreeNode*>(cooled_bf->page.dt)->is_leaf
                  && cooled_bf->page.ru_epoch > reclaiming_ru_epoch.load(std::memory_order_acquire)) {
                  evict_bf(*cooled_bf, o_guard, true);
               } else if (!async_write_buffer.full()) {
                  {
                     BMExclusiveGuard ex_guard(o_guard);
                     paranoid(!cooled_bf->header.is_being_written_back);
                     cooled_bf->header.is_being_written_back.store(true, std::memory_order_release);
                     COUNTERS_BLOCK(absorbed_writes_histogram)
                     {
                        u8 absorbed_writes = std::min<u8>(cooled_bf->header.absorbed_writes, 63);
                        PPCounters::myCounters().absorbed_writes_histogram[absorbed_writes]++;
                     }
                     /// We directly update the header information because we need this information
                     /// to determnine which log we will map to. Therefore, we need to wait until the write succeeds.
                     cooled_bf->header.not_yet_persisted = false;
                     cooled_bf->header.logging = nullptr;
                     cooled_bf->header.absorbed_writes = 0;
                     cooled_bf->header.pending_lsn_count = 0;
                     cooled_bf->header.last_written_plsn = cooled_bf->page.PLSN;
                     cooled_bf->page.prev_ru_epoch = cooled_bf->page.ru_epoch;
                     cooled_bf->page.ru_epoch = ru_epoch.load(std::memory_order_acquire);
                     if (!FLAGS_wal) { cooled_bf->page.last_written_lsn = cr::LogManager::NON_PERSISTED_LSN; }
                     if (FLAGS_per_page_logging) { cooled_bf->ppl.reset(); }
                     if (FLAGS_crc_check) {
                        cooled_bf->header.crc = utils::CRC(cooled_bf->page.dt, EFFECTIVE_PAGE_SIZE);
                     }
                     // TODO: preEviction callback according to DTID
                     PID wb_pid = cooled_bf_pid;
                     async_write_buffer.add(*cooled_bf, wb_pid);
                  }
               } else {
                  jumpmu_break;
               }
            } else {
               evict_bf(*cooled_bf, o_guard, false);
            }
         }
         jumpmuCatch() {}
      }
      evict_candidate_bfs.clear();
      // -------------------------------------------------------------------------------------
      unlock_discarded_pages_with_persisted_ppl();
      // -------------------------------------------------------------------------------------
      // Phase 3:
      auto start = std::chrono::high_resolution_clock::now();
      if (async_write_buffer.submit()) {
         const u32 polled_events = async_write_buffer.pollEventsSync();
         per_pp_iostats[pp_id].io_counter.fetch_add(polled_events, std::memory_order::release);
         PPCounters::myCounters().flushed_pages_counter += polled_events;
         COUNTERS_BLOCK() {
            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            if (WorkerCounters::myCounters().ioWriteHistLock.try_lock()) {
               WorkerCounters::myCounters().ioWriteHist.increaseSlot(elapsed);
               WorkerCounters::myCounters().ioWriteHistLock.unlock();
            }
         }

         async_write_buffer.getWrittenBfs(
             [&](BufferFrame& written_bf, u64 written_plsn, ru_epoch_t written_ru_epoch) {
                while (true) {
                   jumpmuTry()
                   {
                      // TODO(mfd): With per page logging, the page provider thread needs to unlock
                      // the global page state after the ppl is persisted, stayed locked here may
                      // delay that (no risk of deadlock).
                      BMOptimisticGuard o_guard(written_bf.header.latch);
                      // BMExclusiveGuard ex_guard(o_guard);
                      o_guard.guard.toExclusive(); 

                      ensure(written_bf.header.is_being_written_back);
                      ensure_lte(written_bf.header.last_written_plsn, written_plsn);
                      // -------------------------------------------------------------------------------------
                      written_bf.header.is_being_written_back = false;
                      s64 previous_ru_epoch = written_bf.page.prev_ru_epoch;
                      if (previous_ru_epoch != -1 && previous_ru_epoch > reclaimed_ru_epoch.load(std::memory_order_acquire)) {
                         ru_discard_set.data[previous_ru_epoch % max_open_ru_epochs].invalid.fetch_add(1);
                      }
                      ru_discard_set[written_ru_epoch].total.fetch_add(1);
                      o_guard.guard.unlock();
                      jumpmu_break;
                   }
                   jumpmuCatch() { return false; }
                }
                // -------------------------------------------------------------------------------------
                {
                   jumpmuTry()
                   {
                      BMOptimisticGuard o_guard(written_bf.header.latch);
                      if (written_bf.header.state == BufferFrame::STATE::COOL && !written_bf.header.is_being_written_back && !written_bf.isDirty()) {
                         evict_bf(written_bf, o_guard, false);
                      }
                   }
                   jumpmuCatch() {}
                }
                return true;
             },
             polled_events);
      }
      if (freed_bfs_batch.size()) {
         freed_bfs_batch.push(current_partition);
      }
      unlock_discarded_pages_with_persisted_ppl();
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bg_threads_counter--;
   pp_threads_counter--;
   //   delete cr::Worker::tls_ptr;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
