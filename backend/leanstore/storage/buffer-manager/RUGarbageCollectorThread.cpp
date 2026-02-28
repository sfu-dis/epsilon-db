#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency-recovery/Logging.hpp"
#include "leanstore/concurrency-recovery/LogManager.hpp"
// -------------------------------------------------------------------------------------
#include <liburing.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void BufferManager::ruGarbageCollectorThread(u32 gc_id)
{
   std::string gc_thread_name = "ru_gc_" + std::to_string(gc_id);
   pthread_setname_np(pthread_self(), gc_thread_name.c_str());
   const u32 batch_size = 16;
   void* buf;
   s64 current_gc_epoch = -1;

   gc_threads_counter++;
   bg_threads_counter++;
   if (posix_memalign(&buf, 4096, batch_size * PAGE_SIZE) != 0) {
      printf("posix_memalign failed !!");
      raise(SIGTRAP);
   }

   BufferFrame::Page* buf_pages = reinterpret_cast<BufferFrame::Page*>(buf);

   u8 *log_records = static_cast<u8*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
   ensure(log_records != MAP_FAILED);

   std::memset(log_records, 0, 4096);

   struct io_uring r_ring;
   struct io_uring w_ring;
   u32 flags = 0U;
   int rc = io_uring_queue_init(2 * batch_size, &r_ring, flags);
   posix_check(rc == 0);

   rc = io_uring_queue_init(2 * batch_size, &w_ring, flags);
   posix_check(rc == 0);

   std::unique_ptr<struct io_uring_cqe*[]> cqes;
   cqes = std::make_unique<struct io_uring_cqe*[]>(2 * batch_size);

   std::vector<std::pair<PID, LID>> to_fix_pids;
   to_fix_pids.reserve(batch_size);

   auto fix_page_cb = [&] (struct io_uring_cqe* cqe) {
      ensure_equal(cqe->res, PAGE_SIZE);
      u64 data = io_uring_cqe_get_data64(cqe);
      u64 fixed_pid = data & 0x0000FFFFFFFFFFFF;
      u64 idx = data >> 48;
      assert(idx < to_fix_pids.size());
      ensure_equal(to_fix_pids[idx].first, fixed_pid);
      LID lsn = to_fix_pids[idx].second;
      BufferFrame::Page* page = &buf_pages[idx];
      ensure_equal(page->magic_debugging_number, fixed_pid);
      ensure_equal(page->ru_epoch, current_gc_epoch);
      page->PLSN++;
      page->last_written_lsn = lsn;
      if (false && !FLAGS_fake_log_reapply) {
         ensure(lsn != INVALID_LSN);
         u64 off = lsn % PAGE_SIZE;
         s64 br = pread(log_fd, log_records, 4096, lsn - off);
         ensure_equal(br, 4096);
         auto* entry = (cr::WALEntry*)&log_records[off];
         auto* dte  =  (cr::WALDTEntry*)entry;

         bool ok = logRecordSanityCheck(entry, *page, lsn);
         ensure(ok);
 
         page->GSN = dte->gsn;

         DTRegistry::global_dt_registry.redo(page->dt_id, page->dt, dte->payload);
      }
      page->prev_ru_epoch = page->ru_epoch;
      page->ru_epoch = BMC::global_bf->ru_epoch.load(std::memory_order_acquire);
      page->nbfixed++;  // Just for debugging

      struct io_uring_sqe* sqe = io_uring_get_sqe(&w_ring);
      ensure(sqe != nullptr);
      io_uring_prep_write(sqe, ssd_fd, (void*)page, PAGE_SIZE, fixed_pid * PAGE_SIZE);
      io_uring_sqe_set_data64(sqe, data);
      int s = io_uring_submit(&w_ring);
      ensure(s == 1);
   };

   auto ack_fixed_page = [this, buf_pages](struct io_uring_cqe* cqe) {
      // get the frame and the pid of the fixed page
      // make sure the write has succesfully completed.
      // For now just assert it.
      // If the write fails, I think it is safe to just return it to the set.
      ensure(cqe->res == PAGE_SIZE);
      u64 data = io_uring_cqe_get_data64(cqe);
      PID pid = data & 0x0000FFFFFFFFFFFF;
      u64 idx = data >> 48;
      BufferFrame::Page* page = &buf_pages[idx];
      ensure(page->magic_debugging_number == pid);

      /// TODO(mfd) : Probably consider, considering the batch as happening always in
      /// the same ru epoch to reduce the number of atomic fetch add.
      /// Do we need to update the invalid count of the previous epoch ?
      ru_discard_set[page->ru_epoch].total.fetch_add(1, std::memory_order_acq_rel);

      Partition& partition = getPartition(pid);
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
   };

   u64 tls_max_collected_ru_epoch = 0;
   u64 tls_min_uncollected_ru_epoch = 0;

   while (bg_threads_keep_running) {
      {
         std::unique_lock _l(gc_m);
         is_gc_sleeping++;
         gc_cv.wait(_l,
                    [tls_max_collected_ru_epoch, this]() { return to_gc_epochs.size() > tls_max_collected_ru_epoch || !bg_threads_keep_running; });
         is_gc_sleeping--;
         tls_max_collected_ru_epoch = to_gc_epochs.size();
      }
      if (!bg_threads_keep_running)
         break;
      // ensure(!to_gc_epochs_snapshot.empty());
      fprintf(fp, "[INFO] Will GC those epochs [%lu, %lu)\n", tls_min_uncollected_ru_epoch, tls_max_collected_ru_epoch);
      for (s64 gc_ru_epoch = tls_min_uncollected_ru_epoch; gc_ru_epoch < tls_max_collected_ru_epoch; gc_ru_epoch++) {
         current_gc_epoch = gc_ru_epoch;
         s64 prev_gc_ru_epoch = current_gc_epoch - 1;
         auto& set = ru_discard_set[gc_ru_epoch];
         set.m.lock();
         if (!set.is_garbage_collected.exchange(true, std::memory_order_release)) {
            bool ok = reclaiming_ru_epoch.compare_exchange_strong(prev_gc_ru_epoch, current_gc_epoch);
            u64 to_gc_pages = set.pids.size();
            set.m.unlock();
            ensure(ok);
            fprintf(fp, "[INFO] Garbage collecting RU epoch %lu, ~%lu pages to fix\n", gc_ru_epoch, to_gc_pages);
         } else {
            set.m.unlock();
         }
         auto start = std::chrono::system_clock::now();
         while (set.size() > 0) {
            // -------------------------------------------------------------------------------------
            set.m.lock();
            auto it = set.pids.begin();
            u32 attempts = 0;
            while (it != set.pids.end() && attempts < batch_size) {
               attempts++;
               auto [pid, lsn] = *it;
               it++;
               Partition& partition = getPartition(pid);
               if (!partition.ht_mutex.try_lock())
               {
                  // do not block, just move on
                  continue;
               }
               auto frame_handler = partition.io_ht.lookup(pid);
               if (frame_handler) {
                  // someone is reading the page, so he will fix it.
                  partition.ht_mutex.unlock();
                  continue;
               }
               
               u64 erased = set.pids.erase(pid);
               ensure_equal(erased, 1);

               IOFrame& io_frame = partition.io_ht.insert(pid);
               io_frame.readers_counter = 1;
               io_frame.state = IOFrame::STATE::READING;
               bool ok = io_frame.mutex.try_lock();
               ensure(ok);
               partition.ht_mutex.unlock();
              
               to_fix_pids.emplace_back(pid, lsn);
           }
           set.m.unlock();
           // -------------------------------------------------------------------------------------

           u64 pages_to_fix = 0;
           for (const auto& [pid, lsn] : to_fix_pids) {
               // issue the async read.
               struct io_uring_sqe* sqe = io_uring_get_sqe(&r_ring);
               ensure(sqe != nullptr);
               io_uring_prep_read(sqe, ssd_fd, &buf_pages[pages_to_fix], PAGE_SIZE, pid * PAGE_SIZE);
               // TODO(mfd) : store just the index into the array.
               //  we can keep the pid here just for debugging.
               ensure((pid & 0xFFFF000000000000) == 0);
               u64 data = (pages_to_fix << 48) | pid;
               io_uring_sqe_set_data64(sqe, data);
               ++pages_to_fix;
               // TODO(mfd) : Probably amortize this for each 4 pages for example, to balance between
               //  the syscall overhead and the waiting for pages to be read.
               int s = io_uring_submit(&r_ring);
               ensure(s == 1);
            }
            ensure_equal(pages_to_fix, to_fix_pids.size());
            if (pages_to_fix > 0) {
               u32 ready = io_uring_peek_batch_cqe(&r_ring, cqes.get(), pages_to_fix);
               for (u32 i = 0; i < ready; ++i) {
                  fix_page_cb(cqes[i]);
               }
               if (ready > 0)
                  io_uring_cq_advance(&r_ring, ready);
               // Wait for the remaining part, and do the same thing.
               u32 remaining = pages_to_fix - ready;
               if (remaining > 0) {
                  // do the same, loop over all finshed. define a callback.
                  int rc = io_uring_wait_cqe_nr(&r_ring, cqes.get(), remaining);
                  posix_check(rc == 0);
                  struct io_uring_cqe* cqe;
                  u32 head;
                  u32 i = 0;
                  io_uring_for_each_cqe(&r_ring, head, cqe)
                  {
                     fix_page_cb(cqe);
                     ++i;
                  }
                  ensure_equal(i, remaining);
                  io_uring_cq_advance(&r_ring, remaining);
               }
               // We already removed the pages from the set
               // So we just need to remove the frame
               // Wait for the writes to finish.
               ready = io_uring_peek_batch_cqe(&w_ring, cqes.get(), pages_to_fix);

               for (u32 i = 0; i < ready; ++i) {
                  ack_fixed_page(cqes[i]);
               }
               if (ready > 0)
                  io_uring_cq_advance(&w_ring, ready);
               // Wait for the remaining part, and do the same thing.
               remaining = pages_to_fix - ready;
               if (remaining > 0) {
                  int rc = io_uring_wait_cqe_nr(&w_ring, cqes.get(), remaining);
                  // Temporly check that this did not fail
                  posix_check(rc == 0);

                  struct io_uring_cqe* cqe;
                  u32 head;
                  u32 i = 0;
                  io_uring_for_each_cqe(&w_ring, head, cqe)
                  {
                     ack_fixed_page(cqe);
                     ++i;
                  }
                  ensure_equal(i, remaining);
                  io_uring_cq_advance(&w_ring, remaining);
               }
               tot_gc_writes.fetch_add(pages_to_fix, std::memory_order_acq_rel);
               to_fix_pids.clear();
            }
         }
         auto end = std::chrono::system_clock::now();
         auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
         if (set.done_gc.fetch_sub(1) == 1) {
            s64 old_re = reclaimed_ru_epoch.load(std::memory_order_relaxed);
            s64 new_re = old_re + 1;
            ensure_equal(new_re, current_gc_epoch);
            u32 log_id = (current_gc_epoch % (cr::LogManager::global->log_count - 1)) + 1;
            auto& logging = cr::LogManager::global->all_logs[log_id];
            set.m.lock();
            set.reset();
            logging.reset();
            set.m.unlock();
            if (FLAGS_wal && FLAGS_wal_pwrite) {
               cr::LogManager::global->resetLogSegment(current_gc_epoch);
            }
            fprintf(fp, "GC epoch %lu, time taken %lu seconds\n", gc_ru_epoch, duration.count());
         } 
         while (set.done_gc.load() != FLAGS_ru_gc_threads) {}
      }
      tls_min_uncollected_ru_epoch = tls_max_collected_ru_epoch;
   }
   bg_threads_counter--;
   gc_threads_counter--;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
