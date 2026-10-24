#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "CompressedSpace.hpp"
#include "CustomSlabAllocator.hpp"
#include "DTRegistry.hpp"
#include "Exceptions.hpp"
#include "PageState.hpp"
#include "SSDLayout.hpp"
#include "RUBackgroundPageFixer.hpp"
#include "RUEpochManagerThread.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/storage/NvmeDevice.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
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
// Used to identify writer threads.
// Range [0, FLAGS_pp_threads) for page provider threads.
// Range [FLAGS_pp_threads, FLAGS_pp_threads + FLAGS_ru_gc_thread) for bg page fixer threads.
// -1 for now writer threads.
thread_local s32 tls_writer_thread_id = -1;
u64 BufferManager::RU_SIZE = 3193344UL; // Hardcoded for now, we will read from the device later.
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(s32 ssd_fd, const SSDLayout& ssd_layout, u32 max_open_ru_epochs) :
  ssd_fd(ssd_fd),
  io_writer_threads_count(FLAGS_pp_threads + FLAGS_ru_gc_threads),
  max_open_ru_epochs(max_open_ru_epochs),
  persistant_ru_state_offset(ssd_layout.persistant_ru_state_offset),
  max_pid(ssd_layout.dbPages())
{
   // PIDs are used as raw page offsets on ssd_fd.
   ensure(ssd_layout.db_start == 0);
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
      mlock(big_memory_chunk, dram_total_size);
#if 0
      if (mlock(big_memory_chunk, dram_total_size) == -1) {
         perror("mlock");
         SetupFailed("Cannot prefault the buffer pool, do you have enough memory?");
      }
#endif
      // -------------------------------------------------------------------------------------
      logger = std::make_unique<utils::Logger>("buffer_manager_journal.txt");
      // -------------------------------------------------------------------------------------
      if (FLAGS_enable_discarding) {
         void *entries_p = mmap(nullptr, ssd_layout.dbPages() * sizeof(PageState), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
         if (entries_p == MAP_FAILED) {
            perror("Failed to allocated memory for the Discard Set\n");
            SetupFailed("Do you have enough memory ?");
         }
         discard_state = static_cast<PageState*>(entries_p);
         PageState::global_discard_state = discard_state;
         madvise(entries_p, ssd_layout.dbPages() * sizeof(PageState), MADV_HUGEPAGE);
         int rc = mlock(entries_p, ssd_layout.dbPages() * sizeof(PageState));
         if (rc == -1) {
            perror("mlock");
            raise(SIGTRAP);
         }
         COUNTERS_BLOCK(discard_state_peak_mem_usage)
         {
            bm_stats.discard_state_peak_mem_usage.store(ssd_layout.dbPages() * sizeof(PageState));
         }
         max_pending_lsn = FLAGS_per_page_logging ? FLAGS_ppl_merge_threshold : FLAGS_max_log_records_to_discard;
         if (max_pending_lsn > MAX_PENDING_LSN_COUNT) {
            SetupFailed("");
         }
         LOG_INFO(logger, "Max Pending LSN : %u", max_pending_lsn);
         if (FLAGS_discard_lsns_in_parent) {
            ensure_lte(max_pending_lsn, btree::BTreeNode::max_pending_lsn);
            ensure(!FLAGS_optimistic_parent_pointer);
         } else if (max_pending_lsn > 1) {
            per_pp_allocator = std::make_unique<CustomSlabAllocator<LID>[]>(FLAGS_pp_threads);
         }
         if (!FLAGS_initialize_leanstore) {
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
         partitions.push_back(std::make_unique<Partition>(p_i, partitions_count, max_pid, free_bfs_limit));
      }
      // -------------------------------------------------------------------------------------
      utils::Parallelize::parallelRange(dram_total_size, [&](u64 begin, u64 end) { memset(reinterpret_cast<u8*>(bfs) + begin, 0, end - begin); });
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         u64 p_i = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            partitions[p_i]->dram_free_list.push(*new (bfs + bf_i) BufferFrame());
            p_i = (p_i + 1) % partitions_count;
         }
      });
      // -------------------------------------------------------------------------------------
      // Every RUH gets a ring as large as the global budget since a single RUH may hold all open RU epochs.
      // The unused RUHs get a state as well since a recovered page may still point to one of them.
      ru_discard_set.reserve(MAX_RUHS);
      for (ruh_id_t ruh = 0; ruh < MAX_RUHS; ++ruh) {
         ru_discard_set.emplace_back(max_open_ru_epochs);
         for (u32 i = 0; i < max_open_ru_epochs; ++i) {
            auto& set = ru_discard_set[ruh].at(i);
            set.ruh = ruh;
            set.id = i;
         }
         ru_epoch[ruh].store(0);
         oldest_uncollected_ru_epoch[ruh].store(0);
         reclaimed_ru_epoch[ruh].store(-1);
      }
      // -------------------------------------------------------------------------------------
      ensure(io_writer_threads_count > 0);
      writers_iostat = std::make_unique<padded_iostat[]>(io_writer_threads_count);
      u64 aligned_size = utils::upAlign(PersistantRUState::sizeFor(max_open_ru_epochs), 4096);
      persistant_ru_state = reinterpret_cast<PersistantRUState*>(std::aligned_alloc(4096, aligned_size));
      ensure(persistant_ru_state != nullptr);
      new (persistant_ru_state) PersistantRUState(max_open_ru_epochs);
      if (!FLAGS_initialize_leanstore) {
         /// XXX(mfd) : We only support recovering from a load only workload, i.e. no RU epoch
         /// of any RUH has been discarded yet.
         persistant_ru_state->loadFromPersistantStorage(ssd_fd, persistant_ru_state_offset);
         ensure_equal(persistant_ru_state->max_open_ru_epochs, max_open_ru_epochs);
         u64 used_ru_epochs = 0;
         for (ruh_id_t ruh = fdp::num_ruhs; ruh < MAX_RUHS; ++ruh) {
            if (persistant_ru_state->ru_epoch[ruh] != 0 || persistant_ru_state->total(ruh, 0) != 0) {
               LOG_WARN(logger, "RUH %u is not used by the %s placement policy but holds persisted pages, ignoring its RU state", ruh,
                        FLAGS_fdp_placement_policy.c_str());
            }
         }
         for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
            const ru_epoch_t newest_active_ru_epoch = persistant_ru_state->ru_epoch[ruh];
            LOG_INFO(logger, "Recovering RUH %u, Newest Active RU epoch is %ld", ruh, newest_active_ru_epoch);
            ensure(newest_active_ru_epoch >= 0 && newest_active_ru_epoch < max_open_ru_epochs);
            if (persistant_ru_state->oldest_active_ru_epoch[ruh] != 0) {
               // We do not fully support recovering from discarded state for now.
               TODOException();
            }
            ensure_equal(persistant_ru_state->reclaimed_ru_epoch[ruh], -1);
            ru_epoch[ruh].store(newest_active_ru_epoch);
            oldest_uncollected_ru_epoch[ruh].store(persistant_ru_state->oldest_active_ru_epoch[ruh]);
            reclaimed_ru_epoch[ruh].store(persistant_ru_state->reclaimed_ru_epoch[ruh]);
            const ru_epoch_t oldest = oldest_uncollected_ru_epoch[ruh].load(std::memory_order_relaxed);
            const u32 last_total = persistant_ru_state->total(ruh, newest_active_ru_epoch - oldest);
            ensure_lt(last_total, RU_SIZE);
            writers_iostat[0].io_counter[ruh] = last_total;
            LOG_INFO(logger, "Recovering RUH %u, pages used in the newest RU %u", ruh, last_total);
            for (ru_epoch_t e = oldest; e <= newest_active_ru_epoch; ++e) {
               auto& set = ru_discard_set[ruh].at(e);
               set.total.store(persistant_ru_state->total(ruh, e - oldest));
               // FIXME(mfd) : recover real invalid counter
               set.invalid.store(0);
               set.cur_ru_epoch = e;
               set.active.store(true);
            }
            used_ru_epochs += newest_active_ru_epoch - oldest + 1;
         }
         ensure_lte(used_ru_epochs, max_open_ru_epochs);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
         recoverPerTableStats();
#endif
      } else {
         for (ruh_id_t ruh = 0; ruh < MAX_RUHS; ++ruh) {
            persistant_ru_state->ru_epoch[ruh] = 0;
            persistant_ru_state->oldest_active_ru_epoch[ruh] = 0;
            persistant_ru_state->reclaimed_ru_epoch[ruh] = -1;
            ru_discard_set[ruh].at(0).cur_ru_epoch = 0;
            ru_discard_set[ruh].at(0).active.store(true);
         }
      }
      if (FLAGS_wal && FLAGS_wal_pwrite) {
         ensure(!FLAGS_redo_log_file.empty());
         log_fd = open(FLAGS_redo_log_file.c_str(), O_DIRECT | O_RDONLY);
         ensure(log_fd > 0);
      }
      // -------------------------------------------------------------------------------------
      if (FLAGS_compress_pages) {
         compressed_space = std::make_unique<CompressedSpace>(max_pid, io_writer_threads_count);
         if (!FLAGS_initialize_leanstore) {
            compressed_space->recover(FLAGS_compression_space_map_file);
            LOG_INFO(logger, "Recovered the compression space map from %s, %lu blocks in use", FLAGS_compression_space_map_file.c_str(),
                     compressed_space->usedBlocks());
         }
      }
      // -------------------------------------------------------------------------------------
      ru_epoch_mgr = std::make_unique<RUEpochManager>(*this);
      ru_bg_page_fixer = std::make_unique<RUBackgroundPageFixer>(*this);
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
                if (FLAGS_cpu_counters) {
                   CPUCounters::registerThread("pp_" + std::to_string(t_i));
                }
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

      ru_epoch_mgr->start();

      if (FLAGS_enable_discarding) {
         ru_bg_page_fixer->start();
      }
   }
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> BufferManager::serialize()
{
   // TODO: correctly serialize ranges of used pages
   std::unordered_map<std::string, std::string> map;
   // "max_pid" is the allocation high watermark, not BufferManager::max_pid. The key is kept for old state files.
   PID next_pid = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      next_pid = std::max<PID>(partitions[p_i]->next_pid, next_pid);
   }
   map["max_pid"] = std::to_string(next_pid);
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      map["ruh_" + std::to_string(ruh) + "_pages"] = std::to_string(ruh_pages[ruh].pages.load());
   }
   return map;
}
// -------------------------------------------------------------------------------------
void BufferManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   PID next_pid = std::stol(map["max_pid"]);
   next_pid = (next_pid + (partitions_count - 1)) & ~(partitions_count - 1);
   if (next_pid > max_pid) {
      SetupFailed("The recovered database allocated up to PID " + std::to_string(next_pid) + " but the SSD layout only has " +
                  std::to_string(max_pid) + " database pages, was it created with a larger ssd_gib?");
   }
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      partitions[p_i]->next_pid = next_pid + p_i;
   }
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      const std::string key = "ruh_" + std::to_string(ruh) + "_pages";
      if (map.count(key) == 0) {
         SetupFailed("The recovered database is missing counters of the per ruh allocated pages.");
      }
      ruh_pages[ruh].pages.store(std::stol(map[key]));
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::writeAllBufferFrames()
{
   ensure(FLAGS_persist);
   // The caller ~Leanstore is responsible for shutting down writer threads.
   ensure_equal(pp_threads_counter.load(), 0);
   ensure_equal(ru_bg_page_fixer->runningThreads(), 0);
   ensure(shutdown_flush_in_progress.load(std::memory_order_acquire));

   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      ensure_lt(ru_discard_set[ruh][ru_epoch[ruh].load()].total.load(), static_cast<s32>(RU_SIZE));
   }

   if (FLAGS_compress_pages) {
      writeAllBufferFramesCompressed();
   } else {
      std::atomic<u32> ephemeral_write_id = 0;
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         tls_writer_thread_id = ephemeral_write_id.fetch_add(1) % io_writer_threads_count;
         BufferFrame::Page page;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            auto& bf = bfs[bf_i];
            bf.header.latch.mutex.lock();
            if (!bf.isFree() && bf.isDirty()) {
               bf.page.magic_debugging_number = bf.header.pid;
               fdp_placement_policy.onWriteBackDirtyPage(bf);
               pageWriteBackPrologue(bf);
               bf.header.discard_count = 0;
               ++bf.page.write_back_count;
               DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, static_cast<u8*>(page));
               s64 ret = nvme_pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE, bf.header.next_ruh);
               ensure_equal(ret, PAGE_SIZE);
            }
            bf.header.latch.mutex.unlock();
         }
      });
   }
   stopRUEpochManagerThread();
   // TODO(mfd) : Move this as the responsability of the RU Epoch Manager.
   for (ruh_id_t ruh = 0; ruh < MAX_RUHS; ++ruh) {
      if (ruh >= fdp::num_ruhs) {
         // Unused by the placement policy, persisted as empty.
         persistant_ru_state->ru_epoch[ruh] = 0;
         persistant_ru_state->oldest_active_ru_epoch[ruh] = 0;
         persistant_ru_state->reclaimed_ru_epoch[ruh] = -1;
         persistant_ru_state->total(ruh, 0) = 0;
         continue;
      }
      ensure_equal(oldest_uncollected_ru_epoch[ruh].load(), 0);
      ensure_equal(reclaimed_ru_epoch[ruh].load(), -1);
      const ru_epoch_t newest_ru_epoch = ru_epoch[ruh].load(std::memory_order_acquire);
      LOG_INFO(logger, "RUH %u newest RU epoch %ld is left with %d", ruh, newest_ru_epoch, ru_discard_set[ruh][newest_ru_epoch].total.load());
      persistant_ru_state->ru_epoch[ruh] = newest_ru_epoch;
      persistant_ru_state->oldest_active_ru_epoch[ruh] = 0;
      persistant_ru_state->reclaimed_ru_epoch[ruh] = -1;
      for (ru_epoch_t e = 0; e <= newest_ru_epoch; ++e) {
         persistant_ru_state->total(ruh, e) = ru_discard_set[ruh][e].total.load();
      }
   }
   persistant_ru_state->writetoPersistantStorage(ssd_fd, persistant_ru_state_offset);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   persistPerTableStats();
#endif
}
// -------------------------------------------------------------------------------------
// Every write gives the page a new PID that the copy of its parent on the SSD must hold, so a parent is
// written after its cached children. This goes in rounds, each round writes the frames that need a write
// (dirty, never written or one of their cached children got a new PID) and whose cached children do not.
void BufferManager::writeAllBufferFramesCompressed()
{
   auto forEachCachedChild = [&](BufferFrame& bf, auto&& callback) {
      getDTRegistry().iterateChildrenSwips(bf.page.dt_id, bf, [&](Swip<BufferFrame>& swip) {
         if (!swip.isEVICTED()) {
            callback(swip.asBufferFrameMasked());
         }
         return true;
      });
   };
   constexpr u64 PACKER_BLOCKS = 64;
   std::vector<u8> needs_write(dram_pool_size);
   std::atomic<u32> ephemeral_write_id = 0;
   u64 rounds = 0, written_frames = 0;
   while (true) {
      std::atomic<u64> to_write = 0;
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         u64 local_to_write = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            auto& bf = bfs[bf_i];
            bool write = !bf.isFree() && (bf.isDirty() || !cpid::onSSD(bf.header.pid));
            if (!write && !bf.isFree()) {
               forEachCachedChild(bf, [&](BufferFrame& child) { write |= child.header.pid_changed; });
            }
            needs_write[bf_i] = write;
            local_to_write += write;
         }
         to_write += local_to_write;
      });
      if (to_write == 0) {
         break;
      }
      ensure(++rounds < 64);
      std::atomic<u64> written = 0;
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         tls_writer_thread_id = ephemeral_write_id.fetch_add(1) % io_writer_threads_count;
         std::unique_ptr<u8, decltype(&std::free)> blocks(static_cast<u8*>(std::aligned_alloc(PAGE_SIZE, PACKER_BLOCKS * PAGE_SIZE)), &std::free);
         ensure(blocks != nullptr);
         BlockPacker packer(*compressed_space, blocks.get(), PACKER_BLOCKS);
         auto flush = [&]() {
            packer.seal();
            for (u64 b_i = 0; b_i < packer.blocksCount(); b_i++) {
               s64 ret = nvme_pwrite(ssd_fd, packer.block(b_i), PAGE_SIZE, packer.blockOffset(b_i), DEFAULT_RUH);
               ensure_equal(ret, PAGE_SIZE);
            }
            countWrittenBlocks(DEFAULT_RUH, packer.blocksCount());
            packer.reset();
         };
         BufferFrame::Page page;
         u64 local_written = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            if (!needs_write[bf_i]) {
               continue;
            }
            auto& bf = bfs[bf_i];
            bool children_written = true;
            forEachCachedChild(bf, [&](BufferFrame& child) { children_written &= !needs_write[&child - bfs]; });
            if (!children_written) {
               continue;
            }
            if (packer.full()) {
               flush();
            }
            bf.header.latch.mutex.lock();
            fdp_placement_policy.onWriteBackDirtyPage(bf);
            pageWriteBackPrologue(bf);
            bf.header.discard_count = 0;
            ++bf.page.write_back_count;
            // The cached children are written already, the checkpoint holds their latest PIDs.
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, static_cast<u8*>(page));
            if (FLAGS_compression_zero_free_space) {
               reinterpret_cast<btree::BTreeNode*>(page.dt)->zeroFreeSpace();
            }
            const PID new_pid = packer.add(page);
            compressed_space->free(bf.header.pid);
            bf.header.pid = new_pid;
            bf.header.pid_changed = true;
            bf.header.last_written_plsn = bf.page.PLSN;
            forEachCachedChild(bf, [&](BufferFrame& child) { child.header.pid_changed = false; });
            bf.header.latch.mutex.unlock();
            local_written++;
         }
         flush();
         written += local_written;
      });
      written_frames += written;
   }
   compressed_space->persist(FLAGS_compression_space_map_file);
   LOG_INFO(logger, "Compressed shutdown flush: %lu frames written in %lu rounds, %lu blocks in use. Persisted the space map to %s",
            written_frames, rounds, compressed_space->usedBlocks(), FLAGS_compression_space_map_file.c_str());
   const u64 written_pages = compressed_space->written_pages.load();
   if (written_pages) {
      LOG_INFO(logger, "Compression: %lu pages written into %lu blocks, %.2f compression ratio, %.2f pages per block", written_pages,
               compressed_space->written_blocks.load(), written_pages * PAGE_SIZE * 1.0 / compressed_space->written_compressed_bytes.load(),
               written_pages * 1.0 / compressed_space->written_blocks.load());
   }
}
// -------------------------------------------------------------------------------------
#ifdef PER_TPCC_TABLE_STATS_DEBUG
// Only the tables with a non zero total are written, the dt name is informative only.
void BufferManager::persistPerTableStats()
{
   namespace rs = rapidjson;
   rs::Document d;
   d.SetObject();
   auto& allocator = d.GetAllocator();
   d.AddMember("max_open_ru_epochs", max_open_ru_epochs, allocator);
   d.AddMember("max_tracked_dt_id", RUEpochDiscardSet::MAX_TRACKED_DT_ID, allocator);
   rs::Value ruhs(rs::kArrayType);
   for (ruh_id_t ruh = 0; ruh < fdp::num_ruhs; ++ruh) {
      rs::Value ruh_json(rs::kObjectType);
      ruh_json.AddMember("ruh", ruh, allocator);
      rs::Value epochs(rs::kArrayType);
      const ru_epoch_t oldest = oldest_uncollected_ru_epoch[ruh].load();
      const ru_epoch_t newest = ru_epoch[ruh].load();
      for (ru_epoch_t e = oldest; e <= newest; ++e) {
         auto& set = ru_discard_set[ruh][e];
         rs::Value epoch_json(rs::kObjectType);
         epoch_json.AddMember("ru_epoch", static_cast<s64>(e), allocator);
         rs::Value tables(rs::kArrayType);
         for (DTID dt_id = 0; dt_id < RUEpochDiscardSet::MAX_TRACKED_DT_ID; ++dt_id) {
            auto& t = *set.tableStats(dt_id);
            if (t.total.load() == 0) {
               continue;
            }
            rs::Value table_json(rs::kObjectType);
            table_json.AddMember("dt_id", dt_id, allocator);
            rs::Value dt_name;
            dt_name.SetString(DTRegistry::global_dt_registry.getDTName(dt_id).c_str(), allocator);
            table_json.AddMember("dt_name", dt_name, allocator);
            table_json.AddMember("total", t.total.load(), allocator);
            table_json.AddMember("invalid", t.invalid.load(), allocator);
            table_json.AddMember("inserted", t.inserted.load(), allocator);
            table_json.AddMember("deleted", t.deleted.load(), allocator);
            tables.PushBack(table_json, allocator);
         }
         epoch_json.AddMember("tables", tables, allocator);
         epochs.PushBack(epoch_json, allocator);
      }
      ruh_json.AddMember("epochs", epochs, allocator);
      ruhs.PushBack(ruh_json, allocator);
   }
   d.AddMember("ruhs", ruhs, allocator);
   // -------------------------------------------------------------------------------------
   rs::StringBuffer sb;
   rs::PrettyWriter<rs::StringBuffer> writer(sb);
   d.Accept(writer);
   std::ofstream json_file(FLAGS_ru_table_stats_file, std::ios::trunc);
   json_file << sb.GetString();
   ensure(json_file.good());
   LOG_INFO(logger, "Persisted the per table RU epoch stats to %s", FLAGS_ru_table_stats_file.c_str());
}
// -------------------------------------------------------------------------------------
// Must run after the RU epochs of all RUHs got recovered from the PersistantRUState.
void BufferManager::recoverPerTableStats()
{
   namespace rs = rapidjson;
   std::ifstream json_file(FLAGS_ru_table_stats_file);
   if (!json_file.is_open()) {
      LOG_WARN(logger, "%s not found, the per table RU epoch stats start from 0", FLAGS_ru_table_stats_file.c_str());
      return;
   }
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   ensure(!d.HasParseError() && d.IsObject());
   ensure_equal(d["max_open_ru_epochs"].GetUint(), max_open_ru_epochs);
   ensure_equal(d["max_tracked_dt_id"].GetInt(), RUEpochDiscardSet::MAX_TRACKED_DT_ID);
   for (auto& ruh_json : d["ruhs"].GetArray()) {
      const ruh_id_t ruh = ruh_json["ruh"].GetUint();
      ensure_lt(ruh, MAX_RUHS);
      if (ruh >= fdp::num_ruhs) {
         continue;
      }
      const ru_epoch_t oldest = oldest_uncollected_ru_epoch[ruh].load();
      const ru_epoch_t newest = ru_epoch[ruh].load();
      for (auto& epoch_json : ruh_json["epochs"].GetArray()) {
         const ru_epoch_t e = epoch_json["ru_epoch"].GetInt64();
         ensure(e >= oldest && e <= newest);
         auto& set = ru_discard_set[ruh][e];
         s32 tables_total = 0;
         for (auto& table_json : epoch_json["tables"].GetArray()) {
            auto* t = set.tableStats(table_json["dt_id"].GetInt());
            ensure(t != nullptr);
            t->total.store(table_json["total"].GetInt());
            t->invalid.store(table_json["invalid"].GetInt());
            t->inserted.store(table_json["inserted"].GetInt());
            t->deleted.store(table_json["deleted"].GetInt());
            tables_total += table_json["total"].GetInt();
         }
         // Tables beyond MAX_TRACKED_DT_ID are not tracked.
         ensure_lte(tables_total, set.total.load());
      }
   }
   LOG_INFO(logger, "Recovered the per table RU epoch stats from %s", FLAGS_ru_table_stats_file.c_str());
}
#endif
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   u64 total_used_pages = 0, total_freed_pages = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      total_freed_pages += partitions[p_i]->freedPages();
      total_used_pages += partitions[p_i]->allocatedPages();
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
   return *partitions[rand_partition_i];
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
   auto [free_pid, ru_epoch, ruh] = partition.nextPID();
   if (free_pid == INVALID_PID) {
      outOfSpace();
   }
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
   page.ruh = ruh;
   page.ru_epoch = ru_epoch;
   page.magic_debugging_number = free_pid;
   fdp_placement_policy.onAllocateNewPage(free_bf);
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
void BufferManager::outOfSpace()
{
   // We are in the middle of a transaction with latches held, the worker cannot unwind and the
   // shutdown sequence would wait for it forever, so the best we can do is to fail loudly.
   // Other workers may run dry concurrently, only the first one reports and aborts, the rest block here.
   static std::mutex out_of_space_mutex;
   out_of_space_mutex.lock();
   LOG_ERROR(logger,
             "Out of SSD space: all %lu database pages [0, %lu) are allocated, the next page would overwrite "
             "the persistant RU state at offset %lu. Increase ssd_gib or reduce the workload size. Aborting.",
             max_pid, max_pid, persistant_ru_state_offset);
   std::cerr << "[FATAL] Out of SSD space: all " << max_pid << " database pages are allocated and the next one would overwrite the "
             << "persistant RU state at offset " << persistant_ru_state_offset << ". Increase ssd_gib or reduce the workload size. "
             << "The database cannot be shutdown cleanly from inside a transaction, aborting." << std::endl;
   std::abort();
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
                                            !last_read_bf->isDirty() &&
                                            (!FLAGS_compress_pages || cpid::onSSD(last_read_bf->header.pid))
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
         onEvictedPageSwip(*last_read_bf, *parent_handler.parent_bf);
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
   if (FLAGS_compress_pages) {
      // The page keeps its logical id in the magic debugging number, that is what gets recycled.
      const PID logical_pid = bf.page.magic_debugging_number;
      ensure(!cpid::onSSD(logical_pid));
      if (FLAGS_recycle_pages) {
         partitions[logical_pid & partitions_mask]->freePage(logical_pid, bf.page.ru_epoch, bf.page.ruh);
      }
      // A block must not be reused while a write to it is in flight, so the space of a page that is
      // being written back is leaked.
      if (!bf.header.is_being_written_back) {
         compressed_space->free(bf.header.pid);
      }
   } else if (FLAGS_recycle_pages) {
      partition.freePage(bf.header.pid, bf.page.ru_epoch, bf.page.ruh);
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
   ensure(!FLAGS_initialize_leanstore);
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
   ensure_equal(utils::downAlign(lsnOffset(lsn), 4096), utils::downAlign(lsnOffset(lsn) + entry->size, 4096));
   return true;

fail:
   cout << "LSN = " << lsnToString(lsn) << endl;
   entry->dump();
   auto& log = cr::LogManager::getLog(page.ru_epoch, page.magic_debugging_number);
   cout << "Page ID : " << page.magic_debugging_number << endl;
   cout << "Page ID on Log record : " << dte->pid << endl;
   cout << "GSN on Log record : " << dte->gsn << endl;
   cout << "RU epoch on Log record : " << dte->ru_epoch << endl;
   cout << "Hardened GSN : " <<  log.hardened_gsn << endl;
   cout << "Page GSN : " << page.GSN << endl;
   cout << "Last written LSN to page : " << lsnToString(page.last_written_lsn) << endl;
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
   const u8 parent_lsn_count = swip_value.pendingLSNCount();
   LID parent_lsn[btree::BTreeNode::max_pending_lsn];
   if (FLAGS_discard_lsns_in_parent) {
      std::memcpy(parent_lsn, swip_value.pendingLSNs(), parent_lsn_count * sizeof(LID));
   }
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
      if (bf.page.dt_id == 10 || bf.page.dt_id == 6) {
         WorkerCounters::myCounters().dirty_orderlike_counter++;
      }
      cr::WALEntry* entry = nullptr;
      cr::WALDTEntry* dte = nullptr;
      LID last_lsn = INVALID_LSN;
      LID last_gsn = 0;
      bool reconstruct_ppl = FLAGS_per_page_logging;
      for (u8 i = 0; i < nb_log_records; ++i) {
         last_lsn = lsn_list[i];
         ensure(last_lsn != INVALID_LSN);
         u64 off = (lsnOffset(last_lsn) % 4096) + 4096 * i;
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
      u8 discard_count = 0;
      if (FLAGS_enable_discarding) {
         // Since I already locked the partition and there is no IO frame.
         // Then the garbage collection is not able to concurrently fix the page.
         // Therefore, I expect the page state to be unlocked.
         ensure(!discard_state[pid].isLocked());
         std::tie(lsn, nb_log_records) = discard_state[pid].getLocked();
         lsn_list = (nb_log_records == 1) ? &lsn : reinterpret_cast<LID*>(lsn);
         discard_count = discard_state[pid].getDiscardCount();
         if (FLAGS_discard_lsns_in_parent && page_need_fixing && discard_state[pid].isDiscarded()) {
            // The page state only holds the last LSN
            ensure_equal(nb_log_records, parent_lsn_count);
            ensure_equal(lsn, parent_lsn[parent_lsn_count - 1]);
            lsn_list = parent_lsn;
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
      if (page_need_fixing && !gc_fixed) {
         // issue the asynchronus log record read.
         ensure(discard_state[pid].isDiscarded());
         for (u8 i = 0; i < nb_log_records; ++i) {
            // Try to get the log record from the WAL buffer first.
            // fix_dirty_page expects the record at the same in-block offset as a device read.
            const u64 lsn_offset_in_block = lsnOffset(lsn_list[i]) % 4096;
            u8* dst = cr::Worker::my().log_record_buf + 4096 * i + lsn_offset_in_block;
            const u32 log_id = cr::LogManager::global->LSN2LogID(lsn_list[i]);
            if (cr::LogManager::global->all_logs[log_id].tryCopyFromWALBuffer(lsn_list[i], dst, 4096 - lsn_offset_in_block)) {
               COUNTERS_BLOCK(log_reads_in_wal_buffer)
               {
                  WorkerCounters::myCounters().log_reads_in_wal_buffer++;
               }
               continue;
            }
            struct io_uring_sqe* sqe = io_uring_get_sqe(&cr::Worker::my().ring);
            ensure(sqe != nullptr);
            u64 off = utils::downAlign(cr::LogManager::global->LSN2DevOffset(lsn_list[i]), 4096);
            // io_uring_prep_read(sqe, 1 /*log_fd*/, cr::Worker::my().log_record_buf, 4096, off);
            io_uring_prep_read_fixed(sqe, 1 /*log_fd*/, cr::Worker::my().log_record_buf + 4096 * i, 4096, off, 0 /*buf_idx*/);
            // io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, lsn_list[i]);
            wait_for_io++;
            COUNTERS_BLOCK(log_reads_in_wal_buffer)
            {
               WorkerCounters::myCounters().log_reads_from_device++;
            }
         }
      }
      // readPageSync(pid, bf.page);
      struct io_uring_sqe* sqe = io_uring_get_sqe(&cr::Worker::my().ring);
      ensure(sqe != nullptr);
      // With compression, read the whole block holding the page, an uncompressed page fills its block.
      u8* read_buffer = bf.page;
      if (FLAGS_compress_pages) {
         ensure(cpid::onSSD(pid));
         if (cpid::length(pid) != PAGE_SIZE) {
            read_buffer = compression::threadLocalBlockBuffer();
         }
         io_uring_prep_read(sqe, 0 /*ssd_fd*/, read_buffer, PAGE_SIZE, cpid::block(pid) * PAGE_SIZE);
      } else {
         io_uring_prep_read(sqe, 0 /*ssd_fd*/, bf.page, PAGE_SIZE, pid * PAGE_SIZE);
      }
      io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
      io_uring_sqe_set_data64(sqe, pid | (1UL << 63));
      s32 s = io_uring_submit_and_wait(&cr::Worker::my().ring, wait_for_io);
      ensure_equal(s, static_cast<s32>(wait_for_io));
      COUNTERS_BLOCK(read_operations_counter)
      {
         WorkerCounters::myCounters().read_operations_counter++;
      }
      // -------------------------------------------------------------------------------------
      struct io_uring_cqe* cqes[1 + max_pending_lsn];
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
      if (read_buffer != bf.page) {
         compression::decompressPage(read_buffer + cpid::offsetInBlock(pid), cpid::length(pid), bf.page);
      }
      // -------------------------------------------------------------------------------------
      if (page_need_fixing && !gc_fixed) {
         ru_discard_set[DEFAULT_RUH].at(bf.page.ru_epoch).deleted.fetch_add(1);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
         if (auto* t = ru_discard_set[DEFAULT_RUH].at(bf.page.ru_epoch).tableStats(bf.page.dt_id)) {
            t->deleted.fetch_add(1, std::memory_order_relaxed);
         }
#endif
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
      if (!FLAGS_compress_pages) {
         ensure_equal(bf.page.magic_debugging_number, pid);
      }
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      paranoid(!bf.header.is_being_written_back);
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      bf.header.last_written_plsn = bf.page.PLSN;
      bf.header.discardable.store(true, std::memory_order_release);
      bf.header.not_yet_persisted = false;
      bf.header.discard_count = discard_count;
      bf.header.loaded_from_dirty_swip = page_need_fixing;
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
         ensure_lte(nb_log_records, max_pending_lsn);
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
         ensure(!FLAGS_per_page_logging || (bf.ppl.absorbed_writes > 0));
         if (PageState::holdsLSNArray(nb_log_records)) randomAllocator().free(lsn_list, nb_log_records);
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
void BufferManager::prepareDirtyWriteBack(BufferFrame& bf)
{
   auto& header = bf.header;
   ensure(header.is_being_written_back);
   /// We directly update the header information because we need this information
   /// to determnine which log we will map to. Therefore, we need to wait until the write succeeds.
   bf.ppl.absorbed_writes = 0;
   header.not_yet_persisted = false;
   header.logging = nullptr;
   header.pending_lsn_count = 0;
   header.last_written_plsn = bf.page.PLSN;
   header.undiscardable_cause = UNDISCARDABLE_CAUSE::NONE;
   header.will_flush_ppl = false;
   fdp_placement_policy.onWriteBackDirtyPage(bf);
   pageWriteBackPrologue(bf);
   header.discard_count = 0;
   if (FLAGS_per_page_logging) { bf.ppl.reset(); }
   if (FLAGS_crc_check) {
      header.crc = utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE);
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::pageWriteBackPrologue(BufferFrame& bf)
{
   auto& page = bf.page;
   const ruh_id_t prev_ruh = page.ruh;
   const ru_epoch_t prev_ru_epoch = page.ru_epoch;
   page.prev_ru_epoch = prev_ru_epoch;
   page.prev_ruh = prev_ruh;
   ensure(bf.header.next_ruh != UNMAPPED_RUH);
   const ruh_id_t ruh = page.ruh = bf.header.next_ruh;
   page.ru_epoch = ru_epoch[ruh].load(std::memory_order_acquire);
   if (!FLAGS_wal) { page.last_written_lsn = NON_PERSISTED_LSN; }
   // Update the stats here
   if (prev_ruh != ruh) {
      if (prev_ruh != UNMAPPED_RUH) {
         ruh_pages[prev_ruh].pages.fetch_sub(1, std::memory_order_relaxed);
      }
      ruh_pages[ruh].pages.fetch_add(1, std::memory_order_relaxed);
   }
   ensure(tls_writer_thread_id != -1);
   if (FLAGS_compress_pages) {
      // The RU epochs are filled with blocks holding several pages, the writers count the blocks.
      return;
   }
   ru_discard_set[ruh][page.ru_epoch].total.fetch_add(1, std::memory_order_relaxed);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
   if (auto* t = ru_discard_set[ruh][page.ru_epoch].tableStats(page.dt_id)) {
      t->total.fetch_add(1, std::memory_order_relaxed);
   }
#endif
   if (prev_ru_epoch != UNMAPPED_RU_EPOCH && prev_ru_epoch > reclaimed_ru_epoch[prev_ruh].load(std::memory_order_acquire)) {
      ru_discard_set[prev_ruh].at(prev_ru_epoch).invalid.fetch_add(1);
#ifdef PER_TPCC_TABLE_STATS_DEBUG
      if (auto* t = ru_discard_set[prev_ruh].at(prev_ru_epoch).tableStats(page.dt_id)) {
         t->invalid.fetch_add(1, std::memory_order_relaxed);
      }
#endif
   }
   writers_iostat[tls_writer_thread_id].io_counter[ruh].fetch_add(1, std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
void BufferManager::countWrittenBlocks(ruh_id_t ruh, u64 n)
{
   ensure(tls_writer_thread_id != -1);
   ru_discard_set[ruh][ru_epoch[ruh].load(std::memory_order_acquire)].total.fetch_add(n, std::memory_order_relaxed);
   writers_iostat[tls_writer_thread_id].io_counter[ruh].fetch_add(n, std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
void BufferManager::onEvictedPageSwip(BufferFrame& bf, BufferFrame& parent_bf)
{
   if (!FLAGS_compress_pages) {
      return;
   }
   ensure(cpid::onSSD(bf.header.pid));
   if (bf.header.pid_changed) {
      // The parent on the SSD still points to the previous copy of the page.
      parent_bf.page.PLSN++;
   }
}
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
   paranoid(u64(destination) % 512 == 0);
   if (FLAGS_compress_pages) {
      ensure(cpid::onSSD(pid));
      u8* block = compression::threadLocalBlockBuffer();
      const s64 ret = pread(ssd_fd, block, PAGE_SIZE, cpid::block(pid) * PAGE_SIZE);
      ensure_equal(ret, s64(PAGE_SIZE));
      compression::decompressPage(block + cpid::offsetInBlock(pid), cpid::length(pid), destination);
      COUNTERS_BLOCK(read_operations_counter)
      {
         WorkerCounters::myCounters().read_operations_counter++;
      }
      return;
   }
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
   if (FLAGS_compress_pages) {
      // Byte addresses, the low bits are not well distributed.
      return ((pid * 0x9E3779B97F4A7C15ull) >> 32) & partitions_mask;
   }
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
void BufferManager::stopWriterThreads()
{
   // ATTENTION: The two flags should be set in this order.
   shutdown_flush_in_progress.store(true, std::memory_order_release);
   bg_threads_keep_running.store(false);
   while (pp_threads_counter) {
   }
   LOG_INFO(logger, "All page provider threads shutted down successfully.");
   // The page fixers may be sleeping, waiting for RU epochs to collect.
   ru_bg_page_fixer->wakeUpAll();
   while (ru_bg_page_fixer->runningThreads()) {
   }
   LOG_INFO(logger, "All background page fixer threads shutted down successfully.");
}
// -------------------------------------------------------------------------------------
void BufferManager::stopRUEpochManagerThread()
{
   ensure(bg_threads_keep_running.load() == false);
   // No flush follows in this path, so there is nothing more for ru_epoch_mgr to wait for.
   shutdown_flush_in_progress.store(false, std::memory_order_release);
   // The RU Epoch Manager Thread is the last one to exit.
   // So I wait for all threads here.
   waitForAllBackgroundThreads();
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   stopWriterThreads();
   stopRUEpochManagerThread();
   ensure_equal(bg_threads_counter.load(), 0);
}
// -------------------------------------------------------------------------------------
void BufferManager::waitForAllBackgroundThreads()
{
   static std::atomic<bool> once = true;
   ensure(shutdown_flush_in_progress.load() == false);
   while (bg_threads_counter) {
   }
   if (once.exchange(false)) {
      LOG_INFO(logger, "All background threads shutted down successfully.");
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   ensure_equal(bg_threads_keep_running.load(), false);
   ensure_equal(pp_threads_counter.load(), 0);
   ensure_equal(ru_bg_page_fixer->runningThreads(), 0);
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
