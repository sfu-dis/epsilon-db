#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
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

#include <chrono>
#include <fstream>
#include <iomanip>
#include <set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
thread_local BufferFrame* BufferManager::last_read_bf = nullptr;
u64 BufferManager::RU_SIZE = 3193344UL; // Hardcoded for now, we will read from the device later.
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(s32 ssd_fd, u64 total_blocks_in_ssd) :
  ssd_fd(ssd_fd), max_open_ru_epochs(total_blocks_in_ssd / RU_SIZE),
  persistant_ru_state_offset(utils::upAlign(FLAGS_ssd_gib * 1073741824, 4096)),
  ru_discard_set(max_open_ru_epochs)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      BMC::global_bf = this;
      dram_pool_size = FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame);
      const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
      void* big_memory_chunk = mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (big_memory_chunk == MAP_FAILED) {
         perror("Failed to allocate memory for the buffer pool");
         SetupFailed("Check the buffer pool size");
      } else {
         bfs = reinterpret_cast<BufferFrame*>(big_memory_chunk);
      }
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      madvise(bfs, dram_total_size,
              MADV_DONTFORK);  // O_DIRECT does not work with forking.
      // -------------------------------------------------------------------------------------
      if (FLAGS_enable_discarding) {
         void *entries_p = mmap(nullptr, total_blocks_in_ssd * sizeof(PageState), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
         if (entries_p == MAP_FAILED) {
            perror("Failed to allocated memory for the Discard Set\n");
            SetupFailed("Do you have enough memory ?");
         }
         discard_state = static_cast<PageState*>(entries_p);
         madvise(entries_p, total_blocks_in_ssd * sizeof(PageState), MADV_HUGEPAGE);
         int rc = mlock(entries_p, total_blocks_in_ssd * sizeof(PageState));
         if (rc == -1) {
            perror("mlock");
            raise(SIGTRAP);
         }
         if (FLAGS_max_log_records_to_discard > 1) {
            per_pp_allocator = std::make_unique<CustomSlabAllocator<LID>[]>(FLAGS_pp_threads);
         }
         if (FLAGS_recover) {
            // TODO(mfd) : Get all workers to reconstruct the discard state.
            // For now we just assume we recover from a clean state.
         }
      } else {
         discard_state = nullptr;
      }
      // -------------------------------------------------------------------------------------
      // Initialize partitions
      partitions_count = (1 << FLAGS_partition_bits);
      partitions_mask = partitions_count - 1;
      const u64 free_bfs_limit = std::ceil((FLAGS_free_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
      for (u64 p_i = 0; p_i < partitions_count; p_i++) {
         partitions.push_back(std::make_unique<Partition>(p_i, partitions_count, free_bfs_limit));
      }
      // -------------------------------------------------------------------------------------
      utils::Parallelize::parallelRange(dram_total_size, [&](u64 begin, u64 end) { memset(reinterpret_cast<u8*>(bfs) + begin, 0, end - begin); });
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         u64 p_i = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            getPartition(p_i).dram_free_list.push(*new (bfs + bf_i) BufferFrame());
            p_i = (p_i + 1) % partitions_count;
         }
      });
      // -------------------------------------------------------------------------------------
      fp = fopen("buffer_manager_journal.txt", "w");
      ensure(fp != nullptr);
      // -------------------------------------------------------------------------------------
      per_pp_iostats = std::make_unique<padded_iostat[]>(FLAGS_pp_threads);
      u64 aligned_size = utils::upAlign(sizeof(PersistantRUState) + max_open_ru_epochs*sizeof(u32), 4096);
      persistant_ru_state = reinterpret_cast<PersistantRUState*>(std::aligned_alloc(4096, aligned_size));
      ensure(persistant_ru_state != nullptr);
      new (persistant_ru_state) PersistantRUState(max_open_ru_epochs);
      if (FLAGS_recover) {
         persistant_ru_state->loadFromPersistantStorage();
         ensure_equal(persistant_ru_state->max_open_ru_epochs, max_open_ru_epochs);
         s64 prev_ru_epoch = persistant_ru_state->ru_epoch;
         fprintf(fp, "[INFO] Recovering, RU epoch is %lu\n", prev_ru_epoch);
         ensure(prev_ru_epoch < max_open_ru_epochs);
         ru_epoch.store(prev_ru_epoch);
         ensure_equal(persistant_ru_state->oldest_active_ru_epoch, 0);
         ensure_equal(persistant_ru_state->reclaimed_ru_epoch, -1);
         oldest_uncollected_ru_epoch.store(persistant_ru_state->oldest_active_ru_epoch);
         reclaimed_ru_epoch.store(persistant_ru_state->reclaimed_ru_epoch);
         // u64 rmb = fdp_get_remaining_bytes_in_ru(ssd_fd, 0);
         u32 last_total = persistant_ru_state->totals[prev_ru_epoch];
         per_pp_iostats[0].io_counter = last_total;
         fprintf(fp, "[INFO] Recovering, written in RU is %u\n", last_total);
         for (s64 e = oldest_uncollected_ru_epoch; e <= prev_ru_epoch; ++e) {
            ru_discard_set[e].total.store(persistant_ru_state->totals[e - oldest_uncollected_ru_epoch]);
            // FIXME(mfd) : recover real invalid counter
            ru_discard_set[e].invalid.store(0);
            // TODO(mfd) : add an atomic flag to mark active RU epochs set, use for debugging.
         }
      } else {
         persistant_ru_state->ru_epoch = 0;
         persistant_ru_state->oldest_active_ru_epoch = 0;
         persistant_ru_state->reclaimed_ru_epoch = -1;
      }
      if (FLAGS_wal && FLAGS_wal_pwrite) {
         ensure(!FLAGS_redo_log_file.empty());
         log_fd = open(FLAGS_redo_log_file.c_str(), O_DIRECT | O_RDONLY);
         ensure(log_fd > 0);
      }
      // -------------------------------------------------------------------------------------
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::startBackgroundThreads()
{
   // Page Provider threads
   if (FLAGS_pp_threads) {  // make it optional for pure in-memory experiments
      std::vector<std::thread> pp_threads;
      const u64 partitions_per_thread = partitions_count / FLAGS_pp_threads;
      ensure(FLAGS_pp_threads <= partitions_count);
      const u64 extra_partitions_for_last_thread = partitions_count % FLAGS_pp_threads;
      // -------------------------------------------------------------------------------------
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
         pp_threads.emplace_back(
             [&, t_i](u64 p_begin, u64 p_end) {
                if (FLAGS_pin_threads) {
                   utils::pinThisThread(FLAGS_worker_threads + FLAGS_wal + t_i);
                } else {
                   // utils::pinThisThread(FLAGS_wal + t_i);
                }
                CPUCounters::registerThread("pp_" + std::to_string(t_i));
                // https://linux.die.net/man/2/setpriority
                if (FLAGS_root) {
                   posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
                }
                pageProviderThread(t_i, p_begin, p_end);
             },
             t_i * partitions_per_thread,
             ((t_i + 1) * partitions_per_thread) + ((t_i == FLAGS_pp_threads - 1) ? extra_partitions_for_last_thread : 0));
         pp_threads_counter++;
         bg_threads_counter++;
      }
      for (auto& thread : pp_threads) {
         thread.detach();
      }

      std::thread ru_epoch_mgr = std::thread([&]() {
         pthread_setname_np(pthread_self(), "ru_epoch_mgr");
         bg_threads_counter++;
         std::vector<u64> last_seen(FLAGS_pp_threads, 0);
         u64 last_seen_tot_gc_writes = 0;
         u64 tot_page_written = 0;
         u64 prev_rmb, rmb;
         if (FLAGS_use_fdp_rumaw) {
            prev_rmb = fdp_get_remaining_bytes_in_ru(ssd_fd, 0);
            rmb = prev_rmb;
            if (rmb != RU_SIZE) {
               fdp_reset_free_ru(ssd_fd, /* default plid*/ 0);
            }
            ensure(fdp_get_remaining_bytes_in_ru(ssd_fd, 0) == s64(RU_SIZE));
            prev_rmb = RU_SIZE;
         }
         auto check_for_new_ru_epoch = [&]() {
            bool open_new_ru_epoch = false;
            if (FLAGS_use_fdp_rumaw) {
               rmb = fdp_get_remaining_bytes_in_ru(ssd_fd, 0);
               // XXX(mfd) Ugly heuristic to avoid fluctuations
               if (prev_rmb < rmb && rmb > (RU_SIZE - 200000)) {
                  open_new_ru_epoch = true;
               }
               prev_rmb = rmb;
            } else {
               u64 local_tot = 0;
               for (u64 pp_id = 0; pp_id < FLAGS_pp_threads; ++pp_id) {
                  u64 new_value = per_pp_iostats[pp_id].io_counter.load(std::memory_order::acquire);
                  ensure(new_value >= last_seen[pp_id]);
                  u64 diff = new_value - last_seen[pp_id];
                  local_tot += diff;
                  tot_page_written += diff;
                  last_seen[pp_id] = new_value;
               }
               u64 seen = tot_gc_writes.load(std::memory_order_acquire);
               tot_page_written += (seen - last_seen_tot_gc_writes);
               local_tot += (seen - last_seen_tot_gc_writes);
               last_seen_tot_gc_writes = seen;
               if (tot_page_written >= RU_SIZE) {
                  open_new_ru_epoch = true;
                  tot_page_written = tot_page_written - RU_SIZE;
               }
            }
            if (open_new_ru_epoch) {
               u64 new_epoch = ru_epoch.load(std::memory_order_relaxed) + 1;
               // TODO(mfd) : handle with care.
               if (FLAGS_enable_discarding) {
                  if ((new_epoch - reclaimed_ru_epoch) >= (max_open_ru_epochs - FLAGS_overprovisioning_ru_epochs)) {
                     ru_discard_set[oldest_uncollected_ru_epoch].force_gc = true;
                  }
               }
               ru_epoch.store(new_epoch, std::memory_order_release);
               fprintf(fp, "[INFO] Opened up a new RU Epoch %lu!!!\n", new_epoch);
            }
         };
         while (bg_threads_keep_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            check_for_new_ru_epoch();
            if (FLAGS_enable_discarding
                && oldest_uncollected_ru_epoch < ru_epoch.load(std::memory_order_relaxed)
                && ru_discard_set[oldest_uncollected_ru_epoch].shouldGC()) {
               if (gc_m.try_lock()) {
                  to_gc_epochs.push_back(oldest_uncollected_ru_epoch);
                  oldest_uncollected_ru_epoch++;
                  if (is_gc_sleeping > 0) {
                     gc_m.unlock();
                     gc_cv.notify_all();
                  } else {
                     gc_m.unlock();
                  }
               }
            }
         }
         // Wake up all grabage collection threads so that they could exit normally
         if (FLAGS_enable_discarding) {
            std::lock_guard _l(gc_m);
            gc_cv.notify_all(); 
         }
         while(pp_threads_counter) {}
         while(gc_threads_counter) {}
         // No thread is able to write now
         check_for_new_ru_epoch();
         // sanity check block
         {
            for (u64 pp_id = 0; pp_id < FLAGS_pp_threads; ++pp_id) {
               ensure_equal(per_pp_iostats[pp_id].io_counter, last_seen[pp_id]);
            }
         }
         bg_threads_counter--;
      });
      ru_epoch_mgr.detach();

      auto garbage_collector_routine = [&](u32 gc_id) {
         ruGarbageCollectorThread(gc_id);
      };
      std::vector<std::thread> garbage_collectors;
      if (FLAGS_enable_discarding) {
         for (s32 gc_id = 0; gc_id < FLAGS_ru_gc_threads; ++gc_id) {
            garbage_collectors.emplace_back(garbage_collector_routine, gc_id);
         }
         for (auto& t : garbage_collectors) {
            t.detach();
         }
      }
   }
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> BufferManager::serialize()
{
   // TODO: correctly serialize ranges of used pages
   std::unordered_map<std::string, std::string> map;
   PID max_pid = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      max_pid = std::max<PID>(getPartition(p_i).next_pid, max_pid);
   }
   map["max_pid"] = std::to_string(max_pid);
   return map;
}
// -------------------------------------------------------------------------------------
void BufferManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   PID max_pid = std::stol(map["max_pid"]);
   max_pid = (max_pid + (partitions_count - 1)) & ~(partitions_count - 1);
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      getPartition(p_i).next_pid = max_pid + p_i;
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::writeAllBufferFrames()
{
   stopBackgroundThreads();
   ensure(!FLAGS_out_of_place);

   std::atomic<u64> total_writes = ru_discard_set[ru_epoch.load()].total.load();
   ensure(total_writes < RU_SIZE);
   
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree() && bf.isDirty()) {
            page.dt_id = bf.page.dt_id;
            page.magic_debugging_number = bf.header.pid;
            s64 previous_ru_epoch = bf.page.ru_epoch;
            u64 cur_ru_epoch = this->ru_epoch.load(std::memory_order_acquire);
            page.ru_epoch = cur_ru_epoch;
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, page.dt);
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure_equal(ret, PAGE_SIZE);
            if (previous_ru_epoch != -1 && previous_ru_epoch > reclaimed_ru_epoch) {
               s32 invalid = ru_discard_set[previous_ru_epoch].invalid.fetch_add(1);
               ensure(invalid <= ru_discard_set[previous_ru_epoch].total.load(std::memory_order_acquire));
            }
            if ((total_writes.fetch_add(1) % RU_SIZE) == 0) {
               bool ok = ru_epoch.compare_exchange_strong(cur_ru_epoch, cur_ru_epoch + 1);
               ensure(ok);
               ru_discard_set[cur_ru_epoch].total.store(RU_SIZE);
               fprintf(fp, "[INFO] Opened up a new RU epoch %lu\n", cur_ru_epoch + 1);
               // FIXME(mfd) : should force garbage collection if this event is 
               // close to happen.
               ensure((cur_ru_epoch + 1 - reclaimed_ru_epoch) <= max_open_ru_epochs);
            }
         }
         bf.header.latch.mutex.unlock();
      }
   });
   ru_discard_set[ru_epoch.load()].total.store(total_writes % RU_SIZE);
   fprintf(fp, "[INFO] newest RU epoch is left with %lu\n", total_writes  % RU_SIZE);
   u64 e = ru_epoch.load(std::memory_order_acquire);
   fprintf(stdout, "%lu\n", e);
   ensure_equal(oldest_uncollected_ru_epoch, 0);
   ensure_equal(reclaimed_ru_epoch, -1);
   persistant_ru_state->ru_epoch = e;
   persistant_ru_state->oldest_active_ru_epoch = 0;
   persistant_ru_state->reclaimed_ru_epoch = -1;
   for (u32 i = 0; i <= e; ++i) {
      auto &set = ru_discard_set[i];
      fprintf(stdout, "(%u,%u,%u),", set.size(), set.invalid.load(), set.total.load());
      persistant_ru_state->totals[i] = set.total.load();
   }
   fprintf(stdout, "\n");
   persistant_ru_state->writetoPersistantStorage();
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   u64 total_used_pages = 0, total_freed_pages = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      total_freed_pages += getPartition(p_i).freedPages();
      total_used_pages += getPartition(p_i).allocatedPages();
   }
   return total_used_pages - total_freed_pages;
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::getContainingBufferFrame(const u8* ptr)
{
   u64 index = (ptr - reinterpret_cast<u8*>(bfs)) / (sizeof(BufferFrame));
   return bfs[index];
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
Partition& BufferManager::randomPartition()
{
   auto rand_partition_i = utils::RandomGenerator::getRand<u64>(0, partitions_count);
   return getPartition(rand_partition_i);
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, dram_pool_size);
   return bfs[rand_buffer_i];
}
CustomSlabAllocator<LID>& BufferManager::randomAllocator()
{
   auto allocator_idx = utils::RandomGenerator::getRand<u64>(0, FLAGS_pp_threads);
   return per_pp_allocator[allocator_idx];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
   // Pick a pratition randomly
   Partition& partition = randomPartition();
   BufferFrame& free_bf = partition.dram_free_list.tryPop();
   PID free_pid = partition.nextPID();
   if (FLAGS_enable_discarding) discard_state[free_pid].unlockBF(&free_bf);
   assert(free_bf.header.state == BufferFrame::STATE::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf.header.latch.assertNotExclusivelyLatched();
   free_bf.header.latch.mutex.lock();  // Exclusive lock before changing to HOT
   free_bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::STATE::HOT;
   free_bf.header.last_written_plsn = free_bf.page.PLSN = free_bf.page.GSN = 0;
   free_bf.page.ru_epoch = s64(-1);
   free_bf.page.last_written_lsn = INVALID_LSN;
   free_bf.header.latch.assertExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().allocate_operations_counter++;
   }
   // -------------------------------------------------------------------------------------
   return free_bf;
}
// -------------------------------------------------------------------------------------
void BufferManager::evictLastPage()
{
   if (FLAGS_worker_page_eviction && last_read_bf) {
      jumpmuTry()
      {
         BMOptimisticGuard o_guard(last_read_bf->header.latch);
         const bool is_cooling_candidate = (!last_read_bf->header.keep_in_memory && !last_read_bf->header.is_being_written_back &&
                                            !(last_read_bf->header.latch.isExclusivelyLatched()) &&
                                            !last_read_bf->isDirty()
                                            // && (partition_i) >= p_begin && (partition_i) <= p_end
                                            && last_read_bf->header.state == BufferFrame::STATE::HOT);
         if (!is_cooling_candidate) {
            jumpmu::jump();
         }
         o_guard.recheck();
         // -------------------------------------------------------------------------------------
         bool picked_a_child_instead = false;
         DTID dt_id = last_read_bf->page.dt_id;
         PID last_pid = last_read_bf->header.pid;
         o_guard.recheck();
         getDTRegistry().iterateChildrenSwips(dt_id, *last_read_bf, [&](Swip<BufferFrame>&) {
            picked_a_child_instead = true;
            return false;
         });
         if (picked_a_child_instead) {
            jumpmu::jump();
         }
         // assert(!partition.io_ht.lookup(last_read_bf->header.pid));
         // assert(!partition.io_ht.lookup(pid));
         ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, *last_read_bf);
         // -------------------------------------------------------------------------------------
         if (FLAGS_optimistic_parent_pointer) {
            if (parent_handler.is_bf_updated) {
               o_guard.guard.version += 2;
            }
         }
         // -------------------------------------------------------------------------------------
         assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
         o_guard.recheck();
         BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
         o_guard.guard.toExclusive();
         // -------------------------------------------------------------------------------------
         assert(!last_read_bf->header.is_being_written_back);
         assert(last_read_bf->header.state != BufferFrame::STATE::FREE);
         parent_handler.swip.evict(last_pid);
         // -------------------------------------------------------------------------------------
         // Reclaim buffer frame
         last_read_bf->reset();
         last_read_bf->header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
         last_read_bf->header.latch.mutex.unlock();
         FreedBfsBatch freed_bfs_batch;
         freed_bfs_batch.add(*last_read_bf);
         freed_bfs_batch.push(getPartition(last_pid));
      }
      jumpmuCatch()
      {
         last_read_bf = nullptr;
      }
   }
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame& bf)
{
   Partition& partition = getPartition(bf.header.pid);
   if (FLAGS_recycle_pages) {
      partition.freePage(bf.header.pid);
   }
   // -------------------------------------------------------------------------------------
   if (bf.header.is_being_written_back) {
      // DO NOTHING ! we have a garbage collector ;-)
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
   } else {
      bf.reset();
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      partition.dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
// Used for recovery. Only resolve meta node swip
BufferFrame& BufferManager::resolveMetaSwip(Swip<BufferFrame>& meta_swip)
{
   ensure(FLAGS_recover);
   ensure(meta_swip.isEVICTED());
   ensure(!meta_swip.isDIRTY());
   PID meta_pid = meta_swip.asPageID();
   // printf("[INFO] Meta node in ru_epoch %d\n", ru_epoch);
   BufferFrame& bf = randomPartition().dram_free_list.tryPop();
   readPageSync(meta_pid, bf.page);
   if (FLAGS_enable_discarding) discard_state[meta_pid].unlockBF(&bf);
   bf.header.last_written_plsn = bf.page.PLSN;
   bf.header.pid = meta_pid;
   meta_swip.warm(&bf);
   bf.header.state = BufferFrame::STATE::HOT;
   jumpmu_return bf;
}
// -------------------------------------------------------------------------------------
bool BufferManager::logRecordSanityCheck(cr::WALEntry *entry, BufferFrame::Page& page, LID lsn)
{
   auto* dte = (cr::WALDTEntry*)entry;
   auto* btree_entry = (btree::WALEntry*) dte->payload;

   if (entry->type != cr::WALEntry::TYPE::DT_SPECIFIC
      || entry->lsn != lsn) {
      goto fail;
   }
   ensure_equal(entry->type, cr::WALEntry::TYPE::DT_SPECIFIC);
   ensure_equal(entry->lsn, lsn);
   if (dte->pid != page.magic_debugging_number || dte->gsn < page.GSN) {
      goto fail;
   }
   ensure_equal(dte->pid, page.magic_debugging_number);
   ensure(dte->gsn >= page.GSN);
   // Make sure that log does not cross device block boundary
   ensure_equal(utils::downAlign(lsn, 4096), utils::downAlign(lsn + entry->size, 4096));
   return true;

fail:
   cout << "LSN = " << lsn << endl;
   entry->dump();
   auto& log = cr::LogManager::getLog(page.ru_epoch, page.magic_debugging_number);
   cout << "Page ID : " << page.magic_debugging_number << endl;
   cout << "Page ID on Log record : " << dte->pid << endl;
   cout << "GSN on Log record : " << dte->gsn << endl;
   cout << "RU epoch on Log record : " << dte->ru_epoch << endl;
   cout << "Hardened GSN : " <<  log.hardened_gsn << endl;
   cout << "Page GSN : " << page.GSN << endl;
   cout << "Last written LSN to page : " << page.last_written_lsn << endl;
   cout << "wal lsn counter : " << log.wal_lsn_counter << endl;
   cout << "log_gsn_clock : " << log.log_gsn_clock << endl;
   cout << "log_segment_start : " << log.log_segment_start << endl;
   cout << "RU epoch : " << page.ru_epoch << endl;
   cout << "Fixed ? " << page.nbfixed << endl;
   cout << "TYPE = "  << (int)btree_entry->type << endl;
   cout << "Log ID = " << cr::LogManager::global->LSN2LogID(lsn) << endl;
   leanstore::print_backtrace();
   // raise(SIGTRAP);
   return false;
}
// -------------------------------------------------------------------------------------
// Returns a non-latched BufferFrame, called by worker threads
BufferFrame& BufferManager::resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
{
   if (swip_value.isHOT()) {
      BufferFrame& bf = swip_value.asBufferFrame();
      swip_guard.recheck();
      return bf;
   } else if (swip_value.isCOOL()) {
      LIVELOCK_DEBUG_BLOCK()
      {
         WorkerCounters::myCounters().cool_success++;
      }
      BufferFrame* bf = &swip_value.asBufferFrameMasked();
      swip_guard.recheck();
      BMOptimisticGuard bf_guard(bf->header.latch);
      BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);  // parent
      BMExclusiveGuard bf_x_guard(bf_guard);                // child
      bf->header.state = BufferFrame::STATE::HOT;
      swip_value.warm();
      return *bf;
   }
   // -------------------------------------------------------------------------------------
   swip_guard.unlock();  // Otherwise we would get a deadlock, P->G, G->P
   const PID pid = swip_value.asPageID();
   const s64 ru_epoch = swip_value.ru_epoch();
   const bool page_need_fixing = swip_value.isDIRTY();
   bool gc_fixed = false;
   Partition& partition = getPartition(pid);
   JMUW<std::unique_lock<instrumented_mutex>> g_guard(partition.ht_mutex);
   swip_guard.recheck();
   paranoid(!swip_value.isHOT());
   LIVELOCK_DEBUG_BLOCK()
   {
      WorkerCounters::myCounters().swizzled++;
   }
   // -------------------------------------------------------------------------------------
   auto fix_dirty_page = [&](BufferFrame& bf, LID* lsn_list, u8 nb_log_records) {
      ensure(bf.page.ru_epoch >= 0);
      COUNTERS_BLOCK(dirty_read_operations_counter)
      {
         WorkerCounters::myCounters().dirty_read_operations_counter++;
      }
      if (FLAGS_fake_log_reapply) {
         bf.page.PLSN += nb_log_records;
         return;
      }
      cr::WALEntry* entry = nullptr;
      cr::WALDTEntry* dte = nullptr;
      LID lsn = INVALID_LSN;
      // TODO(mfd) : Sanity check that prev lsn field in the log recods agrees with the list.
      for (u8 i = 0; i < nb_log_records; ++i) {
         lsn = lsn_list[i];
         ensure(lsn != INVALID_LSN);
         u64 off = (lsn % 4096) + 4096 * i;
         entry = reinterpret_cast<cr::WALEntry*>(&cr::Worker::my().log_record_buf[off]);
         dte = reinterpret_cast<cr::WALDTEntry*>(entry);

         bool ok = logRecordSanityCheck(entry, bf.page, lsn);

         if (!ok) {
            cout << "Page need fixing ? " << page_need_fixing << endl;
            cout << "RU epoch in swizzled pointer " << ru_epoch << endl;
            cout << "GC Fixed ? " << gc_fixed << endl;
            raise(SIGTRAP);
         }

         DTRegistry::global_dt_registry.redo(bf.page.dt_id, bf.page.dt, dte->payload);
      }
      bf.page.GSN = dte->gsn;
      bf.page.PLSN += nb_log_records;
      bf.page.last_written_lsn = lsn;

      return;
   };
   // -------------------------------------------------------------------------------------
   auto frame_handler = partition.io_ht.lookup(pid);
   if (!frame_handler) {
      BufferFrame& bf = randomPartition().dram_free_list.tryPop();
      IOFrame& io_frame = partition.io_ht.insert(pid);
      bf.header.latch.assertNotExclusivelyLatched();
      // -------------------------------------------------------------------------------------
      io_frame.state = IOFrame::STATE::READING;
      io_frame.readers_counter = 1;
      io_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      gc_fixed = false;
      // -------------------------------------------------------------------------------------
      auto& page_state = discard_state[pid];
      auto [lsn, nb_log_records] = page_state.getLocked();
      LID* lsn_list = (nb_log_records == 1) ? &lsn : reinterpret_cast<LID*>(lsn);
      if (page_need_fixing) {
         gc_fixed = page_state.isClean();
      }
      if (page_need_fixing && !gc_fixed) {
         ensure_lt(0, nb_log_records);
         lsn = lsn_list[nb_log_records - 1];
      } else {
         // ensure it is clean ?
      }
      // lsn = state;
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      // -------------------------------------------------------------------------------------
      u32 wait_for_io = 1;
      if (page_need_fixing && !gc_fixed && !FLAGS_fake_log_reapply) {
         // issue the asynchronus log record read.
         ensure(page_state.isDiscarded());
         for (u8 i = 0; i < nb_log_records; ++i) {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&cr::Worker::my().ring);
            ensure(sqe != nullptr);
            u64 off = utils::downAlign(lsn_list[i], 4096);
            // io_uring_prep_read(sqe, 1 /*log_fd*/, cr::Worker::my().log_record_buf, 4096, off);
            io_uring_prep_read_fixed(sqe, 1 /*log_fd*/, cr::Worker::my().log_record_buf + 4096 * i, 4096, off, 0 /*buf_idx*/);
            // io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, lsn_list[i]);
            wait_for_io++;
         }
      }
      // readPageSync(pid, bf.page);
      struct io_uring_sqe* sqe = io_uring_get_sqe(&cr::Worker::my().ring);
      ensure(sqe != nullptr);
      io_uring_prep_read(sqe, 0 /*ssd_fd*/, bf.page, PAGE_SIZE, pid * PAGE_SIZE);
      io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
      io_uring_sqe_set_data64(sqe, pid | (1UL << 63));
      s32 s = io_uring_submit_and_wait(&cr::Worker::my().ring, wait_for_io);
      ensure_equal(s, wait_for_io);
      // TODO(mfd) : Add the io Read Latency Histogram
      COUNTERS_BLOCK(read_operations_counter)
      {
         WorkerCounters::myCounters().read_operations_counter++;
      }
      // -------------------------------------------------------------------------------------
      struct io_uring_cqe* cqes[1 + FLAGS_max_log_records_to_discard];
      u32 ready = io_uring_peek_batch_cqe(&cr::Worker::my().ring, cqes, wait_for_io);
      ensure_equal(ready, wait_for_io);
      bool seen_page = false;
      for (int i = 0; i < wait_for_io; ++i) {
         auto* cqe = cqes[i];
         u64 data = io_uring_cqe_get_data64(cqe);
         if (data & (1ul << 63)) {
            ensure(!seen_page);
            ensure_equal(data & ~(1ul << 63), pid);
            seen_page = true;
            ensure_equal(cqe->res, PAGE_SIZE);
         } else {
            // TODO: check the lsn lists.
            // ensure_equal(data, lsn);
            ensure_equal(cqe->res, 4096);
         }
      }
      io_uring_cq_advance(&cr::Worker::my().ring, wait_for_io);
      // -------------------------------------------------------------------------------------
      if (page_need_fixing && !gc_fixed) {
         ru_discard_set.data[bf.page.ru_epoch % max_open_ru_epochs].deleted.fetch_add(1);
         ensure_equal(bf.page.ru_epoch, ru_epoch);
      }
      paranoid(bf.header.state == BufferFrame::STATE::FREE);
      COUNTERS_BLOCK()
      {
         WorkerCounters::myCounters().dt_page_reads[bf.page.dt_id]++;
         if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == FLAGS_trace_dt_id &&
             utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
            utils::printBackTrace();
         }
      }
      ensure_equal(bf.page.magic_debugging_number, pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      paranoid(!bf.header.is_being_written_back);
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      bf.header.last_written_plsn = bf.page.PLSN;
      if (FLAGS_crc_check) {
         bf.header.crc = utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE);
      }
      // -------------------------------------------------------------------------------------
      if (page_need_fixing && !gc_fixed) {
         // THINK of this: Is the last written lsn of a dirty page represents it's ru_epoch ?
         // TODO(mfd) : Do this sanity check for all log records.
         u32 log_id = cr::LogManager::global->LSN2LogID(lsn);
         if (log_id != 1 + (bf.page.ru_epoch % max_open_ru_epochs)) {
            cerr << "RU epoch is swizzled pointer " << ru_epoch << endl;
            bf.page.dump();
         }
         ensure_equal(log_id, 1 + (bf.page.ru_epoch % max_open_ru_epochs));
         ensure(log_id != 0);
         bf.header.logging = &cr::LogManager::global->all_logs[log_id];
         ensure_lte(nb_log_records, FLAGS_max_log_records_to_discard);
         fix_dirty_page(bf, lsn_list, nb_log_records);
         ensure_equal(bf.header.pending_lsn_count, 0);
         std::memcpy(bf.header.pending_lsn, lsn_list ,nb_log_records * sizeof(LID));
         bf.header.pending_lsn_count = nb_log_records;
         if (nb_log_records > 1) randomAllocator().free(lsn_list, nb_log_records);
      } else {
         ensure(bf.header.logging == nullptr);
      }
      page_state.unlockBF(&bf);
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         swip_guard.recheck();
         JMUW<std::unique_lock<instrumented_mutex>> g_guard(partition.ht_mutex);
         BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         io_frame.mutex.unlock();
         swip_value.warm(&bf);
         bf.header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                     // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         }
         // -------------------------------------------------------------------------------------
         last_read_bf = &bf;
         jumpmu_return bf;
      }
      jumpmuCatch()
      {
         // Change state to ready
         g_guard->lock();
         io_frame.bf = &bf;
         io_frame.state = IOFrame::STATE::READY;
         // -------------------------------------------------------------------------------------
         g_guard->unlock();
         io_frame.mutex.unlock();
         // -------------------------------------------------------------------------------------
         jumpmu::jump(IO_FRAME_PARENT_CHANGE);
      }
   }
   // -------------------------------------------------------------------------------------
   IOFrame& io_frame = frame_handler.frame();
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READING) {
      io_frame.readers_counter++;  // incremented while holding partition lock
      g_guard->unlock();
      io_frame.mutex.lock();
      io_frame.mutex.unlock();
      if (io_frame.readers_counter.fetch_add(-1) == 1) {
         g_guard->lock();
         if (io_frame.readers_counter == 0) {
            partition.io_ht.remove(pid);
         }
         g_guard->unlock();
      }
      // -------------------------------------------------------------------------------------
      LIVELOCK_DEBUG_BLOCK()
      {
         WorkerCounters::myCounters().reading_retry_debug_counter++;
      }
      jumpmu::jump(JumpMURetryCause::IO_FRAME_READING);
   }
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READY) {
      // -------------------------------------------------------------------------------------
      BufferFrame* bf = io_frame.bf;
      {
         // We have to exclusively lock the bf because the page provider thread will
         // try to evict them when its IO is done
         bf->header.latch.assertNotExclusivelyLatched();
         paranoid(bf->header.state == BufferFrame::STATE::LOADED);
         BMOptimisticGuard bf_guard(bf->header.latch);
         BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         BMExclusiveGuard bf_x_guard(bf_guard);
         // -------------------------------------------------------------------------------------
         io_frame.bf = nullptr;
         ensure_equal(bf->header.pid, pid);
         swip_value.warm(bf);
         paranoid(swip_value.isHOT());
         ensure_equal(bf->header.state, BufferFrame::STATE::LOADED);
         bf->header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                      // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         } else {
            io_frame.state = IOFrame::STATE::TO_DELETE;
         }
         g_guard->unlock();
         // -------------------------------------------------------------------------------------
         last_read_bf = bf;
         return *bf;
      }
   }
   if (io_frame.state == IOFrame::STATE::TO_DELETE) {
      if (io_frame.readers_counter == 0) {
         partition.io_ht.remove(pid);
      }
      g_guard->unlock();
      LIVELOCK_DEBUG_BLOCK()
      {
         WorkerCounters::myCounters().to_delete_retry_debug_counter++;
      }
      jumpmu::jump(JumpMURetryCause::IO_FRAME_TO_DELETE);
   }
   ensure(false);
}  // namespace storage
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
   paranoid(u64(destination) % 512 == 0);
   s64 bytes_left = PAGE_SIZE;
   auto start = std::chrono::high_resolution_clock::now();
   do {
      const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
      assert(bytes_read > 0);  // call was successfull?
      bytes_left -= bytes_read;
   } while (bytes_left > 0);
   COUNTERS_BLOCK(ioReadHist)
   {
      auto end = std::chrono::high_resolution_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      if (WorkerCounters::myCounters().ioReadHistLock.try_lock()) {
         WorkerCounters::myCounters().ioReadHist.increaseSlot(elapsed);
         WorkerCounters::myCounters().ioReadHistLock.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK(read_operations_counter)
   {
      WorkerCounters::myCounters().read_operations_counter++;
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
u64 BufferManager::getPartitionID(PID pid)
{
   return pid & partitions_mask;
}
// -------------------------------------------------------------------------------------
Partition& BufferManager::getPartition(PID pid)
{
   const u64 partition_i = getPartitionID(pid);
   assert(partition_i < partitions_count);
   return *partitions[partition_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   stopBackgroundThreads();
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   munmap(bfs, dram_total_size);
   fclose(fp);
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
