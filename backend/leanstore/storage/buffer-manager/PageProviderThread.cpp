#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "CompressedSpace.hpp"
#include "CustomSlabAllocator.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
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
void BufferManager::pageProviderThread(u64 pp_id, u64 p_begin, u64 p_end)  // [p_begin, p_end)
{
   std::string thread_name("pp_" + std::to_string(p_begin) + "_" + std::to_string(p_end));
   pthread_setname_np(pthread_self(), thread_name.c_str());
   tls_writer_thread_id = pp_id;
   using Time = decltype(std::chrono::high_resolution_clock::now());
   // -------------------------------------------------------------------------------------
   leanstore::cr::CRManager::global->registerMeAsSpecialWorker();
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size);
   std::vector<BufferFrame*> cool_candidate_bfs, evict_candidate_bfs;
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
   while (bg_threads_keep_running) {
      // Phase 1: unswizzle pages (put in the cooling stage)
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK(pp_phases) { phase_1_begin = std::chrono::high_resolution_clock::now(); }
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
               repickIf(FLAGS_btree_pin_inner_nodes && !reinterpret_cast<btree::BTreeNode*>(r_buffer->page.dt)->is_leaf);
               if (FLAGS_enable_discarding && FLAGS_wal && FLAGS_wal_pwrite && (r_buffer->header.logging != nullptr)) {
                  // FIXME(mfd) : Account for the page that has changed from an active RU epoch to the collected RU epoch.
                  // TODO(mfd) : Monitor failures because of this.
                  repickIf(r_buffer->page.GSN > r_buffer->header.logging->hardened_gsn.load(std::memory_order_acquire));
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
                  [[maybe_unused]] const PID pid = r_buffer->header.pid;
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
      COUNTERS_BLOCK(pp_phases)
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
         PID evicted_pid;
         {
            BMBlockingExclusiveGuard c_x_guard(c_guard);
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
            evicted_pid = bf.header.pid;
            const LID last_write_lsn = bf.page.last_written_lsn;
            if (discard) {
               if (!bf.isDiscardable()) {
                  jumpmu::jump();
               }
               PARANOID_BLOCK()
               {
                  ensure(FLAGS_wal && FLAGS_wal_pwrite);
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
                  jumpmu::jump();
               }
               cr::Logging& ru_epoch_log = cr::LogManager::getLog(bf.page.ru_epoch);
               if (ru_epoch_log.redirect_to_sink_log.load(std::memory_order_acquire)) {
                  bf.markUnDiscardable(OTHER);
                  jumpmu::jump();
               }
               ensure(bf.header.pending_lsn_count > 0);
               if (!bf.header.will_flush_ppl) {
                  ensure(bf.header.pending_lsn_count <= FLAGS_max_log_records_to_discard);
                  ensure_equal(bf.header.pending_lsn[bf.header.pending_lsn_count - 1], last_write_lsn);
                  ensure_equal(bf.header.pending_lsn_count, bf.page.PLSN - bf.header.last_written_plsn);
               }
               LID *pending_lsn = nullptr;
               if (bf.header.pending_lsn_count == 1) {
                  pending_lsn = bf.header.pending_lsn;
               } else {
                  if (FLAGS_per_page_logging && bf.header.pending_lsn_count >= FLAGS_ppl_merge_threshold) {
                     bool ok = bf.submitPPLEntry();
                     if (!ok) {
                        // only fails when the log that is mapped to this page changes.
                        // just retry and next time I will write the page without trying to discard it.
                        assert(!bf.isDiscardable());
                        jumpmu::jump();
                     }
                     ensure_equal(bf.header.pending_lsn_count, 1);
                     pending_lsn = bf.header.pending_lsn;
                  } else if (FLAGS_discard_lsns_in_parent) {
                     pending_lsn = bf.header.pending_lsn;
                  } else {
                     pending_lsn = per_pp_allocator[pp_id].allocate(bf.header.pending_lsn_count);
                     ensure(pending_lsn != nullptr);
                     std::memcpy(pending_lsn, bf.header.pending_lsn, bf.header.pending_lsn_count * sizeof(LID));
                  }
               }
               bool success = false;
               const u32 log_id = cr::LogManager::getLogID(bf.page.ru_epoch);
               const u8 discard_count = std::min<u8>(bf.header.discard_count + 1, PageState::max_discard_count);
               // A not yet persisted PPL entry is still in the WAL buffer (the GCT only lets it get
               // overwritten once written to the log device), so a buffer miss can copy it from there.
               // The log may have got full since the check above.
               if (ru_epoch_log.redirect_to_sink_log.load(std::memory_order_acquire)) {
                  if (pending_lsn != bf.header.pending_lsn) {
                     per_pp_allocator[pp_id].free(pending_lsn, bf.header.pending_lsn_count);
                  }
                  bf.markUnDiscardable(OTHER);
                  jumpmu::jump();
               }
               if (FLAGS_discard_lsns_in_parent) {
                  // The parent is exclusively latched, the LSNs are stored next to the swip and the swip is evicted.
                  auto& parent_node = *reinterpret_cast<btree::BTreeNode*>(parent_handler.parent_bf->page.dt);
                  if (!parent_node.evictAndStorePendingLSNs(parent_handler.swip.cast<btree::BTreeNode>(), parent_handler.pos, evicted_pid, bf.page.ru_epoch, pending_lsn, bf.header.pending_lsn_count)) {
                     // No space left in the parent for the pending LSNs, write the page back instead.
                     // TODO(mfd) : split the parent instead.
                     bf.markUnDiscardable(PARENT_FULL);
                     jumpmu::jump();
                  }
               }
               success = discard_state[evicted_pid].tryDiscard(pending_lsn, bf.header.pending_lsn_count, log_id, discard_count);
               ensure(success); // because still I haven't implemeneted the HOT page reclaiming.
               // The RU discard set identity may change meanwhile.
               // I simply allow this, this is a very rare event, and these are
               // stats just to approximate thresolhold of GC in RU epoch.
               ru_discard_set[DEFAULT_RUH].at(bf.page.ru_epoch).inserted.fetch_add(1);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
               if (auto* t = ru_discard_set[DEFAULT_RUH].at(bf.page.ru_epoch).tableStats(bf.page.dt_id)) {
                  t->inserted.fetch_add(1, std::memory_order_relaxed);
               }
#endif
               if (!FLAGS_discard_lsns_in_parent) {
                  parent_handler.swip.evictAndMarkDirty(evicted_pid, bf.page.ru_epoch);
               }
               COUNTERS_BLOCK(discarded_pages) { PPCounters::myCounters().discarded_pages++; }
               COUNTERS_BLOCK(discarded_pages_per_dtid) { PPCounters::myCounters().discarded_pages_per_dtid[bf.page.dt_id]++; }
            } else {
               parent_handler.swip.evict(evicted_pid);
               onEvictedPageSwip(bf, *parent_handler.parent_bf);
               if (FLAGS_enable_discarding) {
                  const u32 log_id = cr::LogManager::getLogID(bf.page.ru_epoch);
                  discard_state[evicted_pid].getLocked();
                  ensure(discard_state[evicted_pid].isHot());
                  discard_state[evicted_pid].unlockClean(last_write_lsn, log_id, bf.header.discard_count);
               }
            }
            // The parent only needs to be persisted again when its on-disk swip becomes stale :
            // 1. Discarding : the swip now carries the pending LSNs.
            // 2. Clean eviction of a page loaded through a dirty swip : the persisted parent may still carry
            //    the old pending LSNs. (A parent can't be written back while the child is swizzled, so otherwise
            //    its persisted swip is already the clean one.)
            if (FLAGS_discard_lsns_in_parent && (discard || bf.header.loaded_from_dirty_swip)) {
               parent_handler.parent_bf->markUnDiscardable(INNER_NODE);
               parent_handler.parent_bf->page.PLSN++;
            }
            // -------------------------------------------------------------------------------------
            // Reclaim buffer frame, the latch is released when c_x_guard goes out of scope
            bf.reset();
         }
         // -------------------------------------------------------------------------------------
         freed_bfs_batch.add(bf);
         if (freed_bfs_batch.size() <= std::min<u64>(FLAGS_worker_threads, 128)) {
            freed_bfs_batch.push(current_partition);
         }
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; }
      };
      // -------------------------------------------------------------------------------------
      for (volatile const auto& cooled_bf : evict_candidate_bfs) {
         jumpmuTry()
         {
            auto& header = cooled_bf->header;
            auto& page = cooled_bf->page;
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
               Partition& partition = *partitions[p_i];
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
            // With compression, a page that has never been written has no location on the SSD yet.
            if (cooled_bf->isDirty() || (FLAGS_compress_pages && !cpid::onSSD(cooled_bf_pid))) {
               std::optional<bool> page_in_live_ru_epoch;
               if (FLAGS_enable_discarding
                  && cooled_bf->isDiscardable()
                  && reinterpret_cast<btree::BTreeNode*>(page.dt)->is_leaf
                  && (page_in_live_ru_epoch = (page.ru_epoch > reclaiming_ru_epoch.load(std::memory_order_acquire)))) {
                  /// TODO(mfd) : Ensure, if possible, that the page belongs to the appropriate ruh, which is the DEFAULT_RUH
                  evict_bf(*cooled_bf, o_guard, true);
               } else if (!async_write_buffer.full()
                          && (rate.load(std::memory_order_acquire) == 100 || (utils::RandomGenerator::getRandU64(0, 100) < rate.load(std::memory_order_acquire)))) {
                  {
                     if (page_in_live_ru_epoch.has_value() && !page_in_live_ru_epoch.value()) {
                       // I will waste a memory fence here just for uniformity.
                       cooled_bf->markUnDiscardable(RECLAIMED_RU_EPOCH);
                     }
                     BMExclusiveGuard ex_guard(o_guard);
                     paranoid(!header.is_being_written_back);
                     header.is_being_written_back.store(true, std::memory_order_release);
                     // XXX(mfd) : Probably the better check is > reclaiming not > reclaimed.
                     // TODO(mfd) : I will put the default RUH here, But it should be the RUH that the page will be written into
                     /// This part makes sense only for pages belonging to the discardable ruh.
                     /// Or at least previously belonging to the discardable ruh.
                     /// The open question is how to identify this upfront.
                     const ruh_id_t ruh = DEFAULT_RUH;
                     if (page.ru_epoch != UNMAPPED_RU_EPOCH && page.ru_epoch > reclaimed_ru_epoch[ruh].load(std::memory_order_acquire)) {
                         ru_discard_set[ruh].at(page.ru_epoch).undiscardable_cause_distribution[header.undiscardable_cause].fetch_add(1);
                     }
                     COUNTERS_BLOCK(absorbed_writes_histogram)
                     {
                        u8 absorbed_writes = std::min<u8>(cooled_bf->ppl.absorbed_writes, 63);
                        PPCounters::myCounters().absorbed_writes_histogram[absorbed_writes]++;
                     }
                     COUNTERS_BLOCK(undiscardable_cause)
                     {
                        switch(header.undiscardable_cause) {
                           case (JUST_SPLITTED) : {
                              PPCounters::myCounters().write_just_splitted++;
                              break;
                           }
                           case (NEWLY_ALLOCATED) : {
                              PPCounters::myCounters().write_new_page++;
                              break;
                           }
                           case (PPL_BUFFER_FULL) : {
                              PPCounters::myCounters().ppl_buffer_full++;
                              PPCounters::myCounters().undiscardable_wb_discard_count[std::min<u8>(header.discard_count, 2)]++;
                              break;
                           }
                           case (MAX_DISCARDING_DEPTH_EXCEEDED) : {
                              PPCounters::myCounters().hot++;
                              PPCounters::myCounters().undiscardable_wb_discard_count[std::min<u8>(header.discard_count, 2)]++;
                              break;
                           }
                           case (PARENT_FULL) : {
                              PPCounters::myCounters().parent_full++;
                              break;
                           }
                        }
                     }
                     ++PPCounters::myCounters().written_back_pages_per_dtid[page.dt_id];
                     prepareDirtyWriteBack(*cooled_bf);
                     // TODO: preEviction callback according to DTID
                     if (FLAGS_compress_pages && FLAGS_compression_zero_free_space) {
                        reinterpret_cast<btree::BTreeNode*>(page.dt)->zeroFreeSpace();
                     }
                     const PID written_pid = async_write_buffer.add(*cooled_bf, cooled_bf_pid);
                     if (FLAGS_compress_pages) {
                        // Out-of-place write, the page is cached so nobody reads its previous copy anymore.
                        compressed_space->free(cooled_bf_pid);
                        header.pid = written_pid;
                        header.pid_changed = true;
                     }
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
      // Phase 3:
      auto start = std::chrono::high_resolution_clock::now();
      if (async_write_buffer.submit()) {
         const u32 polled_events = async_write_buffer.pollEventsSync();
         PPCounters::myCounters().flushed_pages_counter += polled_events;
         {
            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            COUNTERS_BLOCK(pp_phases) {
               PPCounters::myCounters().io_phase_us += elapsed;
            }
            COUNTERS_BLOCK(ioWriteHist) {
               if (WorkerCounters::myCounters().ioWriteHistLock.try_lock()) {
                 WorkerCounters::myCounters().ioWriteHist.increaseSlot(elapsed);
                 WorkerCounters::myCounters().ioWriteHistLock.unlock();
               }
            }
         }

         async_write_buffer.getWrittenBfs(
             [&](BufferFrame& written_bf, u64 written_plsn, ru_epoch_t written_ru_epoch) {
                while (true) {
                   jumpmuTry()
                   {
                      BMOptimisticGuard o_guard(written_bf.header.latch);
                      BMExclusiveGuard ex_guard(o_guard);

                      ensure(written_bf.header.is_being_written_back);
                      ensure_lte(written_bf.header.last_written_plsn, written_plsn);
                      // -------------------------------------------------------------------------------------
                      written_bf.header.is_being_written_back = false;
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
      COUNTERS_BLOCK(pp_thread_rounds) { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bg_threads_counter--;
   pp_threads_counter--;
   //   delete cr::Worker::tls_ptr;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
