#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "CustomSlabAllocator.hpp"
#include "Exceptions.hpp"
#include "PageState.hpp"
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
BufferManager::BufferManager(s32 ssd_fd, u64 total_blocks_in_ssd, u32 max_open_ru_epochs) :
  ssd_fd(ssd_fd), max_open_ru_epochs(max_open_ru_epochs),
  persistant_ru_state_offset(total_blocks_in_ssd * PAGE_SIZE),
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
      if (mlock(big_memory_chunk, dram_total_size) == -1) {
         perror("mlock");
         SetupFailed("Cannot prefault the buffer pool, do you have enough memory?");
      }
      // -------------------------------------------------------------------------------------
      if (FLAGS_enable_discarding) {
         void *entries_p = mmap(nullptr, total_blocks_in_ssd * sizeof(PageState), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
         if (entries_p == MAP_FAILED) {
            perror("Failed to allocated memory for the Discard Set\n");
            SetupFailed("Do you have enough memory ?");
         }
         discard_state = static_cast<PageState*>(entries_p);
         PageState::global_discard_state = discard_state;
         madvise(entries_p, total_blocks_in_ssd * sizeof(PageState), MADV_HUGEPAGE);
         int rc = mlock(entries_p, total_blocks_in_ssd * sizeof(PageState));
         if (rc == -1) {
            perror("mlock");
            raise(SIGTRAP);
         }
         COUNTERS_BLOCK(discard_state_peak_mem_usage)
         {
            bm_stats.discard_state_peak_mem_usage.store(total_blocks_in_ssd * sizeof(PageState));
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
      logger = std::make_unique<utils::Logger>("buffer_manager_journal.txt");
      // -------------------------------------------------------------------------------------
      per_pp_iostats = std::make_unique<padded_iostat[]>(FLAGS_pp_threads);
      u64 aligned_size = utils::upAlign(sizeof(PersistantRUState) + max_open_ru_epochs*sizeof(u32), 4096);
      persistant_ru_state = reinterpret_cast<PersistantRUState*>(std::aligned_alloc(4096, aligned_size));
      ensure(persistant_ru_state != nullptr);
      new (persistant_ru_state) PersistantRUState(max_open_ru_epochs);
      if (FLAGS_recover) {
         persistant_ru_state->loadFromPersistantStorage();
         ensure_equal(persistant_ru_state->max_open_ru_epochs, max_open_ru_epochs);
         ru_epoch_t newest_active_ru_epoch = persistant_ru_state->ru_epoch;
         LOG_INFO(logger, "Recovering, Newest Active RU epoch is %lu", newest_active_ru_epoch);
         ensure(newest_active_ru_epoch < max_open_ru_epochs);
         ru_epoch.store(newest_active_ru_epoch);
         if (persistant_ru_state->oldest_active_ru_epoch != 0) {
            // We do not fully support recovering from discarded state for now.
            TODOException();
         }
         ensure_equal(persistant_ru_state->reclaimed_ru_epoch, -1);
         oldest_uncollected_ru_epoch.store(persistant_ru_state->oldest_active_ru_epoch);
         reclaimed_ru_epoch.store(persistant_ru_state->reclaimed_ru_epoch);
         u32 last_total = persistant_ru_state->totals[newest_active_ru_epoch];
         per_pp_iostats[0].io_counter = last_total;
         LOG_INFO(logger, "Recovering, pages used in the newest RU %u", last_total);
         for (ru_epoch_t e = oldest_uncollected_ru_epoch; e <= newest_active_ru_epoch; ++e) {
            auto& set = ru_discard_set.data[e % max_open_ru_epochs];
            set.total.store(persistant_ru_state->totals[e - oldest_uncollected_ru_epoch]);
            // FIXME(mfd) : recover real invalid counter
            set.invalid.store(0);
            set.cur_ru_epoch = e;
            set.active.store(true);
         }
      } else {
         persistant_ru_state->ru_epoch = 0;
         persistant_ru_state->oldest_active_ru_epoch = 0;
         persistant_ru_state->reclaimed_ru_epoch = -1;
         ru_discard_set.data[0].cur_ru_epoch = 0;
         ru_discard_set.data[0].active.store(true);
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
         u64 last_seen_tot_gc_writes = 0;
         u64 tot_page_written = 0;
         std::vector<u64> last_seen(FLAGS_pp_threads, 0);
         if (FLAGS_recover) {
            for (u32 pp = 0; pp < FLAGS_pp_threads; ++pp) {
               last_seen[pp] = per_pp_iostats[pp].io_counter.load();
               tot_page_written += last_seen[pp];
            }
         }
         auto check_for_new_ru_epoch = [&]() {
            bool open_new_ru_epoch = false;
            for (u64 pp_id = 0; pp_id < FLAGS_pp_threads; ++pp_id) {
               u64 new_value = per_pp_iostats[pp_id].io_counter.load(std::memory_order::acquire);
               ensure(new_value >= last_seen[pp_id]);
               u64 diff = new_value - last_seen[pp_id];
               tot_page_written += diff;
               last_seen[pp_id] = new_value;
            }
            u64 seen = tot_gc_writes.load(std::memory_order_acquire);
            tot_page_written += (seen - last_seen_tot_gc_writes);
            last_seen_tot_gc_writes = seen;
            if (tot_page_written >= RU_SIZE) {
               open_new_ru_epoch = true;
               tot_page_written = tot_page_written - RU_SIZE;
            }
            if (open_new_ru_epoch) {
               ru_epoch_t new_ru_epoch = ru_epoch.load(std::memory_order_relaxed) + 1;
               if (FLAGS_enable_discarding) {
                  // Once we almost exhausted all free RUs, we enter the steady state where we reclaim RUs continously.
                  // This will only happen if the workload access pattern never reaches the threshold of RU usage.
                  // When this happens the first time, I reclaim agressively the oldest 20 ru epochs. This is very
                  // similar to the device GC behaviour because, after all, there is no much time for the ru usage
                  // to improve the threshold.
                  const s64 free_ru_epochs = max_open_ru_epochs + reclaimed_ru_epoch - new_ru_epoch;
                  // Probably too early ?
                  if (free_ru_epochs < FLAGS_overprovisioning_ru_epochs) {
                     // cap the new rate at 80%
                     u64 new_rate = (free_ru_epochs * 80)/FLAGS_overprovisioning_ru_epochs;
                     rate.store(new_rate, std::memory_order_release);
                     LOG_WARN(logger, "Updated the rate of writes to %lu%%, free ru epochs = %ld", new_rate, free_ru_epochs);
                     if (!global_force_gc) {
                        global_force_gc = true;
                        const ru_epoch_t up_to = std::min<ru_epoch_t>(oldest_uncollected_ru_epoch + 20, ru_epoch.load(std::memory_order_relaxed) - 1);
                        for (ru_epoch_t r = oldest_uncollected_ru_epoch; r < up_to; ++r) {
                           ru_discard_set[r].force_gc = true;
                           bm_stats.forced_gc_count.fetch_add(1, std::memory_order_relaxed);
                        }
                     } else {
                        ru_discard_set[oldest_uncollected_ru_epoch].force_gc = true;
                        bm_stats.forced_gc_count.fetch_add(1, std::memory_order_relaxed);
                     }
                  } else {
                     if (rate.load(std::memory_order_relaxed) < 100) {
                        rate.store(100, std::memory_order_release);
                        LOG_INFO(logger, "Restored the rate of writes to 100%%, free ru epochs = %ld", free_ru_epochs);
                     }
                  }
               }
               auto& set = ru_discard_set.data[new_ru_epoch % max_open_ru_epochs];
               {
                  std::lock_guard _l(set.m);
                  set.open(new_ru_epoch);
               }
               ru_epoch.store(new_ru_epoch, std::memory_order_release);
               LOG_INFO(logger, "Opened up a new RU Epoch %lu!!!", new_ru_epoch);
            }
         };
         FILE* tfp = fopen("thresholds.txt", "w");
         u64 milliseconds = 0;
         while (bg_threads_keep_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
            // each minute
            if (FLAGS_trace_ru_threshold && FLAGS_enable_discarding) {
               if (milliseconds == (1000*60)) {
                  milliseconds = 0;
                  ru_epoch_t newest_ru_epoch = ru_epoch.load();
                  for (ru_epoch_t r = oldest_uncollected_ru_epoch; r < newest_ru_epoch; ++r) {
                     fprintf(tfp, "(%ld,%.2f)", r, ru_discard_set[r].ReclaimUnitUsage() * 100.0 / ru_discard_set[r].total.load());
                  }
                  fprintf(tfp, "\n");
                  fflush(tfp);
               }
               milliseconds += 10;
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

   const s32 ru_size = RU_SIZE;
   ensure(ru_discard_set[ru_epoch.load()].total.load() < ru_size);

   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree() && bf.isDirty()) {
            bf.page.magic_debugging_number = bf.header.pid;
            ru_epoch_t previous_ru_epoch = bf.page.ru_epoch;
            ru_epoch_t cur_ru_epoch = this->ru_epoch.load(std::memory_order_acquire);
            bf.page.prev_ru_epoch = previous_ru_epoch;
            bf.page.ru_epoch = cur_ru_epoch;
            if (!FLAGS_wal) { bf.page.last_written_lsn = cr::LogManager::NON_PERSISTED_LSN; }
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, static_cast<u8*>(page));
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure_equal(ret, PAGE_SIZE);
            if (ru_discard_set[cur_ru_epoch].total.fetch_add(1) == ru_size) {
               ru_discard_set.data[(cur_ru_epoch + 1) % max_open_ru_epochs].open(cur_ru_epoch+1);
               bool ok = ru_epoch.compare_exchange_strong(cur_ru_epoch, cur_ru_epoch + 1);
               ensure(ok);
               LOG_INFO(logger, "Opened up a new RU epoch %lu", cur_ru_epoch + 1);
               // FIXME(mfd) : Ensure that the buffer manager size is always less than the
               // overprovision size, so that we're gaarenteed that this will never happen.
               ensure((cur_ru_epoch + 1 - reclaimed_ru_epoch) <= max_open_ru_epochs);
            }
            if (previous_ru_epoch != -1 && previous_ru_epoch > reclaimed_ru_epoch) {
               s32 invalid = ru_discard_set[previous_ru_epoch].invalid.fetch_add(1);
               ensure(invalid <= ru_discard_set[previous_ru_epoch].total.load(std::memory_order_acquire));
            }
         }
         bf.header.latch.mutex.unlock();
      }
   });
   ru_epoch_t newest_ru_epoch = ru_epoch.load(std::memory_order_acquire);
   LOG_INFO(logger, "newest RU epoch is left with %d", ru_discard_set[newest_ru_epoch].total.load());
   ensure_equal(oldest_uncollected_ru_epoch, 0);
   ensure_equal(reclaimed_ru_epoch, -1);
   persistant_ru_state->ru_epoch = newest_ru_epoch;
   persistant_ru_state->oldest_active_ru_epoch = 0;
   persistant_ru_state->reclaimed_ru_epoch = -1;
   for (ru_epoch_t e = 0; e <= newest_ru_epoch; ++e) {
      auto &set = ru_discard_set[e];
      persistant_ru_state->totals[e] = set.total.load();
   }
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
   auto& header = free_bf.header;
   auto& page = free_bf.page;
   auto [free_pid, ru_epoch] = partition.nextPID();
   if (FLAGS_enable_discarding) discard_state[free_pid].unlockBF(&free_bf);
   ensure_equal(header.state, BufferFrame::STATE::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   header.latch.assertNotExclusivelyLatched();
   header.latch.mutex.lock();  // Exclusive lock before changing to HOT
   header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);
   header.pid = free_pid;
   header.state = BufferFrame::STATE::HOT;
   header.not_yet_persisted = true;
   header.undiscardable_cause = NEWLY_ALLOCATED;
   // A newly created page cannot be discarded.
   ensure(!header.discardable.load());
   header.last_written_plsn = 0;
   page.reset();
   page.ru_epoch = ru_epoch;
   page.magic_debugging_number = free_pid;
   header.latch.assertExclusivelyLatched();
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
   if (FLAGS_enable_discarding) discard_state[bf.header.pid].free();
   Partition& partition = getPartition(bf.header.pid);
   if (FLAGS_recycle_pages) {
      partition.freePage(bf.header.pid, bf.page.ru_epoch);
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
   BufferFrame& bf = randomPartition().dram_free_list.tryPop(false);
   readPageSync(meta_pid, bf.page);
   if (FLAGS_enable_discarding) discard_state[meta_pid].unlockBF(&bf);
   bf.header.last_written_plsn = bf.page.PLSN;
   bf.header.pid = meta_pid;
   bf.header.discardable = false;
   meta_swip.warm(&bf);
   bf.header.state = BufferFrame::STATE::HOT;
   jumpmu_return bf;
}
// -------------------------------------------------------------------------------------
bool BufferManager::logRecordSanityCheck(cr::WALEntry *entry, BufferFrame::Page& page, LID lsn)
{
   auto* dte = reinterpret_cast<cr::WALDTEntry*>(entry);
   auto* btree_entry = reinterpret_cast<btree::WALEntry*>(dte->payload);

   if (entry->type != cr::WALEntry::TYPE::DT_SPECIFIC
      && entry->type != cr::WALEntry::TYPE::PER_PAGE_DT_SPECIFIC) {
      cerr << "Invalid log entry type." << endl;
      goto fail;
   }
   if (entry->lsn != lsn) {
      goto fail;
   }
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
         bf.page.last_written_lsn = lsn_list[nb_log_records -1];
         return;
      }
      cr::WALEntry* entry = nullptr;
      cr::WALDTEntry* dte = nullptr;
      LID last_lsn = INVALID_LSN;
      LID last_gsn = 0;
      bool reconstruct_ppl = FLAGS_per_page_logging;
      for (u8 i = 0; i < nb_log_records; ++i) {
         last_lsn = lsn_list[i];
         ensure(last_lsn != INVALID_LSN);
         u64 off = (last_lsn % 4096) + 4096 * i;
         entry = reinterpret_cast<cr::WALEntry*>(&cr::Worker::my().log_record_buf[off]);

         bool ok = logRecordSanityCheck(entry, bf.page, last_lsn);

         if (!ok) {
            cout << "Page need fixing ? " << page_need_fixing << endl;
            cout << "RU epoch in swizzled pointer " << ru_epoch << endl;
            cout << "GC Fixed ? " << gc_fixed << endl;
            raise(SIGTRAP);
         }

         if (entry->type == cr::WALEntry::TYPE::DT_SPECIFIC) {
            dte = reinterpret_cast<cr::WALDTEntry*>(entry);
            ensure_lt(sizeof(cr::WALDTEntry), entry->size);
            const u16 lrec_size = entry->size - sizeof(cr::WALDTEntry);
            DTRegistry::global_dt_registry.redo(bf.page.dt_id, bf.page.dt, dte->payload, 1, lrec_size);
            if (reconstruct_ppl) {
               const bool canFitIntoPPL = bf.ppl.insertLogRecord(dte->payload, lrec_size);
               // THINK(mfd) : Is it the case that this should always be true ?
               // i.e, ensure(canFitIntoPPL);
               if (!canFitIntoPPL) {
                  reconstruct_ppl = false;
                  bf.markUnDiscardable(PPL_BUFFER_FULL);
               }
            }
            last_gsn = dte->gsn;
         } else {
            assert(entry->type == cr::WALEntry::TYPE::PER_PAGE_DT_SPECIFIC);
            ensure(FLAGS_per_page_logging);
            // For now I expect always the PPL entry to be the first one to apply.
            ensure_equal(i, 0);
            auto* ppl = reinterpret_cast<BufferFrame::PPL*>(entry);
            // some sanity checks
            ensure_equal(ppl->header.pid, bf.page.magic_debugging_number);
            ensure_equal(ppl->header.dt_id, bf.page.dt_id);
            DTRegistry::global_dt_registry.redo(bf.page.dt_id, bf.page.dt, ppl->log_records, ppl->nb_log_records, ppl->payload_size());
            bf.ppl.insertPPL(*ppl);
            last_gsn = ppl->header.gsn;
         }
      }
      bf.page.GSN = last_gsn;
      bf.page.PLSN += nb_log_records;
      bf.page.last_written_lsn = last_lsn;
      bf.header.fixed_at_plsn = bf.page.PLSN;

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
      LID lsn = INVALID_LSN;
      u8 nb_log_records = 0;
      LID* lsn_list = nullptr;
      if (FLAGS_enable_discarding) {
         // Since I already locked the partition and there is no IO frame.
         // Then the garbage collection is not able to concurrently fix the page.
         // Therefore, I expect the page state to be unlocked.
         // This only holds when PPL is disabled, because ppl keeps the page state
         // locked until PPL entry is persisted.
         const bool was_locked = discard_state[pid].isLocked();
         ensure(FLAGS_per_page_logging || !was_locked);
         std::tie(lsn, nb_log_records) = discard_state[pid].getLocked();
         lsn_list = (nb_log_records == 1) ? &lsn : reinterpret_cast<LID*>(lsn);
         COUNTERS_BLOCK(ppl_not_yet_persisted) {
            if (was_locked) { WorkerCounters::myCounters().ppl_not_yet_persisted++; }
         }
      }
      if (page_need_fixing) {
         gc_fixed = discard_state[pid].isClean();
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
      WorkerCounters::myCounters().experienced_buffer_miss = true;
      // -------------------------------------------------------------------------------------
      u32 wait_for_io = 1;
      using Time = decltype(std::chrono::high_resolution_clock::now());
      [[maybe_unused]] Time start_io, end_io;
      COUNTERS_BLOCK(buffer_miss_io_latency) { start_io = std::chrono::high_resolution_clock::now(); }
      if (page_need_fixing && !gc_fixed && !FLAGS_fake_log_reapply) {
         // issue the asynchronus log record read.
         ensure(discard_state[pid].isDiscarded());
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
      ensure_equal(s, static_cast<s32>(wait_for_io));
      COUNTERS_BLOCK(read_operations_counter)
      {
         WorkerCounters::myCounters().read_operations_counter++;
      }
      // -------------------------------------------------------------------------------------
      struct io_uring_cqe* cqes[1 + FLAGS_max_log_records_to_discard];
      u32 ready = io_uring_peek_batch_cqe(&cr::Worker::my().ring, cqes, wait_for_io);
      ensure_equal(ready, wait_for_io);
      end_io = std::chrono::high_resolution_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_io - start_io).count();
      COUNTERS_BLOCK(buffer_miss_io_latency)
      {
         WorkerCounters::myCounters().io_phase_us[wait_for_io-1] += elapsed;
         ensure(wait_for_io > 0 && wait_for_io <= (FLAGS_max_log_records_to_discard + 1));
         WorkerCounters::myCounters().read_operations_histogram[wait_for_io-1]++;
      }
      COUNTERS_BLOCK(ioReadHist)
      {
         if (WorkerCounters::myCounters().ioReadHistLock.try_lock()) {
            WorkerCounters::myCounters().ioReadHist.increaseSlot(elapsed);
            WorkerCounters::myCounters().ioReadHistLock.unlock();
         }
      }
      bool seen_page = false;
      for (u32 i = 0; i < wait_for_io; ++i) {
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
      bf.header.discardable.store(true, std::memory_order_release);
      bf.header.not_yet_persisted = false;
      if (FLAGS_crc_check) {
         bf.header.crc = utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE);
      }
      // -------------------------------------------------------------------------------------
      if (page_need_fixing && !gc_fixed) {
         // THINK of this: Is the last written lsn of a dirty page represents it's ru_epoch ?
         // TODO(mfd) : Do this sanity check for all log records.
         u32 log_id = cr::LogManager::global->LSN2LogID(lsn);
         if (log_id != cr::LogManager::getLogID(bf.page.ru_epoch)) {
            cerr << "RU epoch is swizzled pointer " << ru_epoch << endl;
            bf.dump();
         }
         ensure_equal(log_id, cr::LogManager::getLogID(bf.page.ru_epoch));
         bf.header.logging = &cr::LogManager::global->all_logs[log_id];
         ensure_lte(nb_log_records, FLAGS_max_log_records_to_discard);
         ensure_equal(bf.header.pending_lsn_count, 0);
         auto start_on_demand_redo = std::chrono::high_resolution_clock::now();
         fix_dirty_page(bf, lsn_list, nb_log_records);
         auto end_on_demand_redo = std::chrono::high_resolution_clock::now();
         u64 on_demand_redo_elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_on_demand_redo - start_on_demand_redo).count();
         WorkerCounters::myCounters().on_demand_redo_ns += on_demand_redo_elapsed_ns;
         if (on_demand_redo_elapsed_ns > WorkerCounters::myCounters().max_on_demand_redo_ns.load(std::memory_order_relaxed)) {
            WorkerCounters::myCounters().max_on_demand_redo_ns = on_demand_redo_elapsed_ns;
         }
         COUNTERS_BLOCK(redoHist)
         {
            if (WorkerCounters::myCounters().redoHistLock.try_lock()) {
               WorkerCounters::myCounters().redoHist.increaseSlot(on_demand_redo_elapsed_ns/1000.0);
               WorkerCounters::myCounters().redoHistLock.unlock();
            }
         }
         ensure_equal(bf.header.pending_lsn_count, 0);
         std::memcpy(bf.header.pending_lsn, lsn_list ,nb_log_records * sizeof(LID));
         bf.header.pending_lsn_count = nb_log_records;
         // ensure(!FLAGS_per_page_logging || (nb_log_records == 1));
         ensure(bf.ppl.absorbed_writes > 0);
         if (nb_log_records > 1) randomAllocator().free(lsn_list, nb_log_records);
      } else {
         ensure(bf.header.logging == nullptr);
      }
      if (FLAGS_enable_discarding) {
         discard_state[pid].unlockBF(&bf);
      }
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
   LOG_INFO(logger, "Shutting down...");
   while (pp_threads_counter) {
   }
   LOG_INFO(logger, "All page provider threads shutted down successfully.");
   while (gc_threads_counter) {
   }
   LOG_INFO(logger, "All background page fixer threads shutted down successfully.");
   while (bg_threads_counter) {
   }
   LOG_INFO(logger, "All background threads shutted down successfully.");
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   ensure_equal(bg_threads_keep_running.load(), false);
   ensure_equal(pp_threads_counter.load(), 0);
   ensure_equal(gc_threads_counter.load(), 0);
   ensure_equal(bg_threads_counter.load(), 0);
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   munmap(bfs, dram_total_size);
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
