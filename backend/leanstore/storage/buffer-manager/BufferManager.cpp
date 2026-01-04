#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
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
thread_local alignas(PAGE_SIZE) u8 log_record_buf[2 * PAGE_SIZE];
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(s32 ssd_fd) : ssd_fd(ssd_fd)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
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
      per_pp_iostats = std::make_unique<padded_iostat[]>(FLAGS_pp_threads);
      if (FLAGS_recover) {
         u8 *buf = (u8*)aligned_alloc(PAGE_SIZE, PAGE_SIZE);
         s64 ret = pread(ssd_fd, buf, PAGE_SIZE, utils::upAlign(FLAGS_ssd_gib * 1024 * 1048576, 4096));
         ensure_equal(ret, PAGE_SIZE);
         s64 prev_ru_epoch = *reinterpret_cast<s64*>(buf);
         printf("[INFO] Recovering, RU epoch is %lu\n", prev_ru_epoch);
         ru_epoch.store(prev_ru_epoch);
         u64 rmb = fdp_get_remaining_bytes_in_ru(ssd_fd, 0);
         per_pp_iostats[0].io_counter = RU_SIZE - rmb;
         printf("[INFO] Recovering, written in RU is %lu\n", RU_SIZE - rmb);
         for (s64 e = 0; e < prev_ru_epoch - 1; ++e) {
            ru_discard_set[e].total.store(RU_SIZE);
         }
         ru_discard_set[prev_ru_epoch - 1].total.store(RU_SIZE-rmb);
      }
      if (FLAGS_wal && FLAGS_wal_pwrite) {
         ensure(!FLAGS_redo_log_file.empty());
         log_fd = open(FLAGS_redo_log_file.c_str(), O_DIRECT | O_RDONLY);
         ensure(log_fd > 0);
      }
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
         bg_threads_counter++;
      }
      for (auto& thread : pp_threads) {
         thread.detach();
      }

      std::thread ru_epoch_mgr = std::thread([&]() {
         pthread_setname_np(pthread_self(), "ru_epoch_mgr");
         bg_threads_counter++;
         u64 oldest_uncollected_epoch = 0;
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
         u8 *buf = (u8*)aligned_alloc(PAGE_SIZE, PAGE_SIZE);
         s64 *ru_epoch_ptr = (s64*)buf;
         *ru_epoch_ptr = ru_epoch.load();
         while (bg_threads_keep_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
               if (tot_page_written >= RU_SIZE ) {
                  open_new_ru_epoch = true;
                  tot_page_written = tot_page_written - RU_SIZE;
               }
            }
            if (open_new_ru_epoch) {
               u64 new_epoch = ru_epoch.load(std::memory_order_relaxed) + 1;
               ensure(new_epoch < 8192);
               ru_epoch.store(new_epoch, std::memory_order_release);
               *ru_epoch_ptr = new_epoch;
               s64 ret = pwrite(ssd_fd, buf, PAGE_SIZE, utils::upAlign(FLAGS_ssd_gib * 1024 * 1048576, 4096));
               ensure_equal(ret, PAGE_SIZE);
               printf("[INFO] Opened up a new RU Epoch %lu!!!\n", new_epoch);
            }
            if (oldest_uncollected_epoch < ru_epoch.load(std::memory_order_relaxed)
                && ru_discard_set[oldest_uncollected_epoch].shouldGC()) {
               if (gc_m.try_lock()) {
                  to_gc_epochs.push_back(oldest_uncollected_epoch);
                  oldest_uncollected_epoch++;
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
         {
            std::lock_guard _l(gc_m);
            gc_cv.notify_all(); 
         }
         bg_threads_counter--;
      });
      ru_epoch_mgr.detach();

      auto garbage_collector_routine = [&](u32 gc_id) {
         ruGarbageCollectorThread(gc_id);
      };
      std::vector<std::thread> garbage_collectors;
      if (FLAGS_enable_discarding) {
         for (u32 gc_id = 0; gc_id < 8; ++gc_id) {
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
   u64 total_writes_local = 0;
   for (u64 pp_id = 0; pp_id < FLAGS_pp_threads; ++pp_id) {
      total_writes_local += per_pp_iostats[pp_id].io_counter.load();
   }
   std::atomic<u64> total_writes = total_writes_local;
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree() && bf.isDirty()) {
            page.dt_id = bf.page.dt_id;
            page.magic_debugging_number = bf.header.pid;
            s64 prev_ru_epoch = bf.page.ru_epoch;
            u64 cur_ru_epoch = this->ru_epoch.load(std::memory_order_acquire);
            page.ru_epoch = cur_ru_epoch;
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, page.dt);
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure(ret == PAGE_SIZE);
            if (prev_ru_epoch != s64(-1)) {
               ru_discard_set[prev_ru_epoch].invalid.fetch_add(1);
            }
            ru_discard_set[cur_ru_epoch].total.fetch_add(1);
            if ((total_writes.fetch_add(1) % RU_SIZE) == 0) {
               bool ok = ru_epoch.compare_exchange_strong(cur_ru_epoch, cur_ru_epoch + 1);
               ensure(ok);
            }
         }
         bf.header.latch.mutex.unlock();
      }
  });
   // TODO(mfd) : Persist RU state needed
   u8 *buf = (u8*)aligned_alloc(PAGE_SIZE, PAGE_SIZE);
   *(s64*)buf = ru_epoch;
   s64 ret = pwrite(ssd_fd, buf, PAGE_SIZE, utils::upAlign(FLAGS_ssd_gib * 1024 * 1048576, 4096));
   ensure_equal(ret, PAGE_SIZE);
   u64 e = ru_epoch.load(std::memory_order_acquire);
   fprintf(stdout, "%lu\n", e);
   for (u32 i = 0; i < e; ++i) {
      auto &set = ru_discard_set[i];
      fprintf(stdout, "(%u,%u,%u),", set.size(), set.invalid.load(), set.total.load());
   }
   fprintf(stdout, "\n");
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
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
   // Pick a pratition randomly
   Partition& partition = randomPartition();
   BufferFrame& free_bf = partition.dram_free_list.tryPop();
   PID free_pid = partition.nextPID();
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
   Partition& partition = getPartition(pid);
   JMUW<std::unique_lock<std::mutex>> g_guard(partition.ht_mutex);
   swip_guard.recheck();
   paranoid(!swip_value.isHOT());
   // -------------------------------------------------------------------------------------
   // TODO(mfd) : Refactor this as a method of the buffer pool
   //  it will be used by the gc thread also.
   auto fix_dirty_page = [&](BufferFrame& bf) {
      ensure(bf.page.ru_epoch >= 0);
      COUNTERS_BLOCK(dirty_read_operations_counter)
      {
         WorkerCounters::myCounters().dirty_read_operations_counter++;
      }
      LID lsn = ru_discard_set[bf.page.ru_epoch].erase(pid);
      if (FLAGS_fake_log_reapply) {
         bf.page.PLSN++;
         return;
      }
      ensure(lsn != LID(-1));
      u64 off = lsn % PAGE_SIZE;
      // TODO(mfd) : remove the pread from the critical section
      s64 br = pread(log_fd, log_record_buf, 2 * PAGE_SIZE, lsn - off);
      ensure_equal(br, (2 * PAGE_SIZE));
      auto* entry = (cr::WALEntry*)&log_record_buf[off];

      if (entry->type != cr::WALEntry::TYPE::DT_SPECIFIC) {
         cout << "LSN = " << lsn << endl;
         entry->dump();
         raise(SIGTRAP);
      }
      ensure_equal(entry->type, cr::WALEntry::TYPE::DT_SPECIFIC);
      ensure_equal(entry->lsn, lsn);
      auto* dte = (cr::WALDTEntry*)entry;
      ensure_equal(dte->pid, bf.page.magic_debugging_number);
      ensure(dte->gsn >= bf.page.GSN);
      bf.page.GSN = dte->gsn;
      bf.page.PLSN++;
      DTRegistry::global_dt_registry.redo(bf.page.dt_id, bf.page.dt, dte->payload);
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
      g_guard->unlock();
      // -------------------------------------------------------------------------------------
      readPageSync(pid, bf.page);
      // -------------------------------------------------------------------------------------
      paranoid(bf.header.state == BufferFrame::STATE::FREE);
      COUNTERS_BLOCK()
      {
         WorkerCounters::myCounters().dt_page_reads[bf.page.dt_id]++;
         if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == FLAGS_trace_dt_id &&
             utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
            utils::printBackTrace();
         }
      }
      paranoid(bf.page.magic_debugging_number == pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      paranoid(!bf.header.is_being_written_back);
      bf.header.last_written_plsn = bf.page.PLSN;
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      if (FLAGS_crc_check) {
         bf.header.crc = utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE);
      }
      // -------------------------------------------------------------------------------------
      if (swip_value.isDIRTY()) {
         fix_dirty_page(bf);
      }
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         swip_guard.recheck();
         JMUW<std::unique_lock<std::mutex>> g_guard(partition.ht_mutex);
         BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         if (!swip_value.isDIRTY()) {
            ensure(bf.page.ru_epoch >= 0);
            ru_discard_set[bf.page.ru_epoch].ensureInexistant(pid);
         }
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
         jumpmu::jump();
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
      jumpmu::jump();
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
         paranoid(bf->header.pid == pid);
         if (!swip_value.isDIRTY()) {
            ensure(bf->page.ru_epoch >= 0);
            ru_discard_set[bf->page.ru_epoch].ensureInexistant(pid);
         }
         swip_value.warm(bf);
         paranoid(swip_value.isHOT());
         paranoid(bf->header.state == BufferFrame::STATE::LOADED);
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
      jumpmu::jump();
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
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
