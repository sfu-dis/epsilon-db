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
      if (FLAGS_iostat || FLAGS_use_fdp_rumaw) {
         per_pp_iostats = std::make_unique<padded_iostat[]>(FLAGS_pp_threads); 
      }
      write_credit_available = FLAGS_ssd_gib * 1048576UL;
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
                   //utils::pinThisThread(FLAGS_wal + t_i);
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
      
      // TODO(mfd) : Refactor those two
      if (FLAGS_use_fdp_rumaw) {
	 std::thread ru_epoch_mgr = std::thread([&]() {
	    pthread_setname_np(pthread_self(), "ru_epoch_mgr");
	    bg_threads_counter++;
	    u64 prev_rmb = fdp_get_remaining_bytes_in_ru(ssd_fd, 0);
	    u64 rmb = prev_rmb;
	    if (rmb != RU_SIZE) {
	       fdp_reset_free_ru(ssd_fd, /* default plid*/ 0);
	    }
	    ensure(fdp_get_remaining_bytes_in_ru(ssd_fd, 0) == s64(RU_SIZE));
	    prev_rmb = RU_SIZE;
	    u64 oldest_uncollected_epoch = 0;
	    while (bg_threads_keep_running) {
	       std::this_thread::sleep_for(std::chrono::milliseconds(50));
	       rmb = fdp_get_remaining_bytes_in_ru(ssd_fd, 0);
	       // XXX(mfd) Ugly hack to avoid fluctuations
	       if (prev_rmb < rmb && rmb > (RU_SIZE - 200000)) {
		  u64 new_epoch = ru_epoch.load(std::memory_order_relaxed) + 1;
		  ensure(new_epoch < 8192);
		  ru_epoch.store(new_epoch, std::memory_order_release);
		  printf("[INFO] Opened up a new RU Epoch %lu!!!\n", new_epoch);
	       }
	       prev_rmb = rmb;
		// if oldest uncollected has utilzation of 80% or higher
	       // add it to the gc set
	       // and if the garbage collector thread is sleeping, wake him up. 
	       if ( oldest_uncollected_epoch < ru_epoch.load(std::memory_order_relaxed) 
		    && ru_discard_set[oldest_uncollected_epoch].shouldGC()) {
		  if (gc_m.try_lock()) {
		     to_gc_epochs.push_back(oldest_uncollected_epoch);
		     oldest_uncollected_epoch++;
		     if (is_gc_sleeping) {
			gc_m.unlock();
			gc_cv.notify_one();
		     } else {
			gc_m.unlock();
		     }
		  }
	       }
	    }
	    bg_threads_counter--;
	 });
	 ru_epoch_mgr.detach();
      } else {
	 std::thread ru_epoch_mgr = std::thread([&]() {
	    pthread_setname_np(pthread_self(), "ru_epoch_mgr");
	    bg_threads_counter++;
	    u64 oldest_uncollected_epoch = 0;
	    std::vector<u64> last_seen(FLAGS_pp_threads, 0);
            u64 last_seen_tot_gc_writes = 0;
            u64 tot_page_written = 0;
	    while (bg_threads_keep_running) {
	       std::this_thread::sleep_for(std::chrono::milliseconds(50));
               u64 local_tot = 0;
               for (u64 pp_id = 0; pp_id < FLAGS_pp_threads; ++pp_id) {
                  u64 new_value = per_pp_iostats[pp_id].io_counter.load(std::memory_order::relaxed);
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
               s64 old_wc = write_credit_available.fetch_sub(local_tot, std::memory_order_relaxed);
               ensure(old_wc > local_tot);
               bool must_gc = (old_wc < (4 * RU_SIZE));
               if (must_gc) {
                  // XXX(mfd) : We will assume the GC will successed freeing an RU epoch, That's fine
                  write_credit_available.fetch_add(RU_SIZE, std::memory_order_relaxed);
                  printf("[WARN] Free pages shortage...Forcing GC\n");
               }
               if (tot_page_written >= RU_SIZE) {
                  tot_page_written = tot_page_written - RU_SIZE;
		  u64 new_epoch = ru_epoch.load(std::memory_order_relaxed) + 1;
		  ensure(new_epoch < 8192);
		  ru_epoch.store(new_epoch, std::memory_order_release);
		  printf("[INFO] Opened up a new RU Epoch %lu!!!\n", new_epoch);
               }
	       if ( oldest_uncollected_epoch < ru_epoch.load(std::memory_order_relaxed) 
		    && (must_gc || ru_discard_set[oldest_uncollected_epoch].shouldGC())) {
		  if (gc_m.try_lock()) {
		     to_gc_epochs.push_back(oldest_uncollected_epoch);
		     oldest_uncollected_epoch++;
		     if (is_gc_sleeping) {
			gc_m.unlock();
			gc_cv.notify_one();
		     } else {
			gc_m.unlock();
		     }
		  }
	       }
	    }
	    bg_threads_counter--;
	 });
	 ru_epoch_mgr.detach();
     }

      /**
      
      */ 
      std::thread garbage_collector = std::thread([&]() {
         pthread_setname_np(pthread_self(), "ru_garbage_collector");
         const u32 batch_size = 32;
         void *buf;
         std::vector<u64> to_gc_epochs_snapshot; 
         
         bg_threads_counter++;
         if (posix_memalign(&buf, 4096, batch_size * PAGE_SIZE) != 0) {
            printf("posix_memalign failed !!");
            raise(SIGTRAP);
         }

         BufferFrame::Page *buf_pages = reinterpret_cast<BufferFrame::Page *>(buf);

         struct io_uring r_ring;
         struct io_uring w_ring;
         u32 flags = 0U;
         int rc = io_uring_queue_init( 2 * batch_size, &r_ring, flags);
         posix_check(rc == 0);
         
         rc = io_uring_queue_init( 2 * batch_size, &w_ring, flags);
         posix_check(rc == 0);

         std::unique_ptr<struct io_uring_cqe *[]> cqes;
         cqes = std::make_unique<struct io_uring_cqe *[]>(2 * batch_size);
         
         
         auto fix_page_cb = [this, &w_ring, buf_pages](struct io_uring_cqe *cqe) {
            // FIXME(mfd) : What if the read fails?
            ensure(cqe->res == PAGE_SIZE);
            // Get where it was written
            u64 data = io_uring_cqe_get_data64(cqe);
            u64 fixed_pid = data & 0x0000FFFFFFFFFFFF;
            u64 idx = data >> 48;
            assert(idx < batch_size);
            BufferFrame::Page *page = &buf_pages[idx];
            ensure(page->magic_debugging_number == fixed_pid);
            // TODO(mfd) : Pass in the gc epoch for debugging.
            // ensure(page->ru_epoch == cgc_epoch);
            // TODO(mfd) : Apply the log here
            page->ru_epoch = BMC::global_bf->ru_epoch.load(std::memory_order_acquire);
            page->undirtied = 1;
            page->PLSN = page->PLSN + 1;
            page->nbfixed++; // Just for debugging
            // Write back the page.
            struct io_uring_sqe *sqe = io_uring_get_sqe(&w_ring);
            ensure(sqe != nullptr);
            io_uring_prep_write(sqe, ssd_fd, (void *)page, PAGE_SIZE, fixed_pid * PAGE_SIZE);
            io_uring_sqe_set_data64(sqe, data);
            int s = io_uring_submit(&w_ring);
            ensure(s == 1);
         };
         while (bg_threads_keep_running) {
            {
               std::unique_lock _l(gc_m);
               is_gc_sleeping = true;
               gc_cv.wait(_l, [this]() {return !to_gc_epochs.empty(); });
               is_gc_sleeping = false;
               // take a snapshot of the vector and clear it.
               to_gc_epochs_snapshot = to_gc_epochs;
               to_gc_epochs.clear(); 
            }
            ensure(!to_gc_epochs_snapshot.empty());
            for (const auto& gc_ru_epoch : to_gc_epochs_snapshot) {
               auto &set = ru_discard_set[gc_ru_epoch];
               set.is_garbage_collected.store(true, std::memory_order_release);
               printf("[INFO] Garbage collecting RU epoch %lu, ~%lu pages to fix\n", gc_ru_epoch, set.size());
               auto start = std::chrono::system_clock::now();
               // We get a batch without removing it from the set and we release
               // the lock because other code paths acquire locks in this order:
               // Guard Lock -> Set lock.
               std::vector<u64> pids_batch;
               while (set.getBatch(pids_batch, batch_size)) {
                  u64 pages_to_fix = 0;
                  for (auto &pid : pids_batch) {
                     Partition& partition = getPartition(pid); 
                     std::unique_lock g_guard(partition.ht_mutex);
                     auto frame_handler = partition.io_ht.lookup(pid);
                     if (frame_handler) {
                        // someone is reading the page, so he will fix it.
                        continue;
                     }
                     // still it is not safe to read and fix the page, someone 
                     // may have just finished fixing the page and removed the frame.
                     // We acquire the set lock and try to remove the pid from the set.
                     if (set.erase(pid, nullptr, 'R') == false) {
                        // someone has already fixed the page.
                        continue;
                     }
                     
                     IOFrame& io_frame = partition.io_ht.insert(pid);
                     io_frame.readers_counter = 1;
                     io_frame.state = IOFrame::STATE::READING;
                     io_frame.mutex.lock();
                     g_guard.unlock();

                     // issue the async read.
                     struct io_uring_sqe *sqe = io_uring_get_sqe(&r_ring);
                     ensure(sqe != nullptr);
                     io_uring_prep_read(sqe, ssd_fd, &buf_pages[pages_to_fix], PAGE_SIZE, pid * PAGE_SIZE);
                     ensure((pid & 0xFFFF000000000000) == 0);
                     u64 data = (pages_to_fix << 48) | pid;
                     io_uring_sqe_set_data64(sqe, data);
                     ++pages_to_fix;
                     // TODO(mfd) : Probably amortize this for each 4 pages for example, to balance between 
                     //  the syscall overhead and the waiting for pages to be read.
                     int s = io_uring_submit(&r_ring);
                     ensure(s == 1);
                  }
                  if (pages_to_fix > 0) {
                     u32 ready = 0;
/*
                     u32 ready = io_uring_peek_batch_cqe(&r_ring, cqes.get(), pages_to_fix);
                     for (u32 i = 0; i < ready; ++i) {
                        fix_page_cb(cqes[i]);
                     }
                     if (ready > 0) io_uring_cq_advance(&r_ring, ready);
                     // Wait for the remaining part, and do the same thing.
*/
                     u32 remaining = pages_to_fix - ready;
                     if (remaining > 0) {
                        // do the same, loop over all finshed. define a callback.
                        int rc = io_uring_wait_cqe_nr(&r_ring, cqes.get(), remaining);
                        posix_check(rc == 0);
                        struct io_uring_cqe *cqe;
                        u32 head;
                        u32 i = 0;
                        io_uring_for_each_cqe(&r_ring, head, cqe) {
                           fix_page_cb(cqe);
                           ++i;
                        }
                        ensure(i == remaining);
                        io_uring_cq_advance(&r_ring, remaining);
                     }
                     // We already removed the pages from the set
                     // So we just need to remove the frame 
                     // Wait for the writes to finish.
                     int rc = io_uring_wait_cqe_nr(&w_ring, cqes.get(), pages_to_fix);
                     // Temporly check that this did not fail
                     posix_check(rc == 0);

                     struct io_uring_cqe *cqe;
                     u32 head;
                     u32 i = 0;
                     u64 old_tot_gc_writes = tot_gc_writes.load(std::memory_order_relaxed);
                     io_uring_for_each_cqe(&w_ring, head, cqe) {
                        // get the frame and the pid of the fixed page
                        // make sure the write has succesfully completed.
                        // For now just assert it.
                        // If the write fails, I think it is safe to just return it to the set.
                        ensure(cqe->res == PAGE_SIZE);
                        u64 data = io_uring_cqe_get_data64(cqe);
                        PID pid = data & 0x0000FFFFFFFFFFFF;
                        u64 idx = data >> 48;
                        assert(idx < pages_to_fix);
                        BufferFrame::Page *page = &buf_pages[idx];
                        ensure(page->magic_debugging_number == pid);
                        ensure(page->undirtied == 1);
  
                        Partition &partition = getPartition(pid);
                        // TODO(mfd) : A lock guard is enough here.
                        std::unique_lock g_guard(partition.ht_mutex);
                        auto frame_handler = partition.io_ht.lookup(pid);
                        ensure(frame_handler);
                        IOFrame &frame = frame_handler.frame();
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
                        g_guard.unlock();
                        ++i;
                     }
                     ensure(i == pages_to_fix);
                     tot_gc_writes.store(old_tot_gc_writes + i, std::memory_order_release);
                     io_uring_cq_advance(&w_ring, pages_to_fix);
                  }
               }
               auto end = std::chrono::system_clock::now();
               auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
               printf("GC epoch %u, time taken %lu seconds\n", gc_ru_epoch, duration.count());
            }
         }
         bg_threads_counter--;
      });
      garbage_collector.detach();
   }
   if (FLAGS_iostat) {
      std::thread iostat_timer;
      if (FLAGS_pin_threads) {
         utils::pinThisThread(FLAGS_worker_threads + FLAGS_wal + FLAGS_pp_threads + 1);
      } else {
         //utils::pinThisThread(FLAGS_wal + FLAGS_pp_threads);
      }
      CPUCounters::registerThread("iostat_timer");
      if (FLAGS_root) {
         posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
      }
      iostat_timer = std::thread( [&] () {
         FILE *fp;
         fp = fopen(FLAGS_iostat_output_file.c_str(), "w");
         ensure(fp != nullptr);
         std::vector<u64> last_seen(FLAGS_pp_threads, 0);
         std::vector<u64> last_seen_discard(FLAGS_pp_threads, 0);
         bg_threads_counter++;
         while (bg_threads_keep_running) {
            u64 tot_page_evicted = 0;
            u64 tot_page_discard = 0;
            // grab the sum for each thread
            for (u64 pp_id = 0; pp_id < FLAGS_pp_threads; ++pp_id) {
               u64 new_value = per_pp_iostats[pp_id].io_counter.load(std::memory_order::relaxed);
               ensure(new_value >= last_seen[pp_id]);
               u64 diff = new_value - last_seen[pp_id];
               tot_page_evicted += diff;
               last_seen[pp_id] = new_value;
               // ------------------------------------------------------
               u64 new_discard_value = per_pp_iostats[pp_id].discard.load(std::memory_order::relaxed);
               ensure(new_discard_value >= last_seen_discard[pp_id]);
               diff = new_discard_value - last_seen_discard[pp_id];
               tot_page_discard += diff;
               last_seen_discard[pp_id] = new_discard_value;
            }
            double wps = (tot_page_evicted * PAGE_SIZE / 1024) * 1.0f / FLAGS_iostat_interval;
            double dps = (tot_page_discard * PAGE_SIZE / 1024) * 1.0f / FLAGS_iostat_interval;
            double free_per = write_credit_available.load(std::memory_order_acquire) * 100.0f/ (FLAGS_ssd_gib * 1048576ul);
            fprintf(fp, "[iostat] : %.2f kb_written/s, %.2f kb_discard/s, %.2f%% nand free\n", wps, dps, free_per); 
            sleep(FLAGS_iostat_interval);
         }
         bg_threads_counter--;
      });
      iostat_timer.detach();
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
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree()) {
            page.dt_id = bf.page.dt_id;
            page.magic_debugging_number = bf.header.pid;
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, page.dt);
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure(ret == PAGE_SIZE);
         }
         bf.header.latch.mutex.unlock();
      }
   });
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
   free_bf.page.undirtied = 0;
   free_bf.header.latch.assertExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().allocate_operations_counter++; }
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
      jumpmuCatch() { last_read_bf = nullptr; }
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
      jumpmuTry()
      {
         swip_guard.recheck();
         JMUW<std::unique_lock<std::mutex>> g_guard(partition.ht_mutex);
         BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         if (swip_value.isDIRTY()) {
            if (bf.page.undirtied == 1 && !swip_value.isDiscardUndirty()) {
               // ru_discard_set[old_ru_epoch].ensureInexistant(pid);
               PARANOID_BLOCK() {
                  ru_discard_set[bf.page.ru_epoch].log_op(pid, &bf, 'c');
               }
            } else {
               bf.page.PLSN++;
               ensure(bf.page.ru_epoch >= 0);
               bool ok = ru_discard_set[bf.page.ru_epoch].erase(pid, &bf);
               ensure(ok);
            }
         } else {
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
         if (swip_value.isDIRTY()) {
            if (bf->page.undirtied == 1 && !swip_value.isDiscardUndirty()) {
               // bf->page.undirtied = 0;
               // TODO(mfd) : Store the ru_epoch in the swizzeled pointer for debugging.
               // ru_discard_set[bf->page.ru_epoch].ensureInexistant(pid);
               PARANOID_BLOCK() {
                  ru_discard_set[bf->page.ru_epoch].log_op(pid, bf, 'c');
               }
            } else {
               bf->page.PLSN++;
               ensure(bf->page.ru_epoch >= 0);
               bool ok = ru_discard_set[bf->page.ru_epoch].erase(pid, bf);
               ensure(ok);
            }
         } else {
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
   COUNTERS_BLOCK()
   {
      auto end = std::chrono::high_resolution_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      if (WorkerCounters::myCounters().ioReadHistLock.try_lock()) {
         WorkerCounters::myCounters().ioReadHist.increaseSlot(elapsed);
         WorkerCounters::myCounters().ioReadHistLock.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().read_operations_counter++; }
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
