#include "BMTable.hpp"
#include <cstdint>

#include "leanstore/Config.hpp"
#include "leanstore/storage/buffer-manager/CompressedSpace.hpp"
#include "leanstore/storage/buffer-manager/LBAChain.hpp"
#include "leanstore/storage/buffer-manager/RUBackgroundPageFixer.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/GCCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
using leanstore::utils::threadlocal::sum2;
using leanstore::utils::threadlocal::max;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
BMTable::BMTable(BufferManager& bm) : ProfilingTable(), bm(bm)
{
   absorption_histogram_file.open("absorbed_writes_histogram.txt",  std::ios::out | std::ios::trunc);
   if (!absorption_histogram_file.is_open()) {
      throw std::runtime_error("Failed to open absorbed_writes_histogram.txt");
   }
}
// -------------------------------------------------------------------------------------
std::string BMTable::getName()
{
   return "bm";
}
// -------------------------------------------------------------------------------------
void BMTable::open()
{
   columns.emplace("key", [](Column& col) { col << 0; });
   columns.emplace("space_usage_gib", [&](Column& col) {
      const double gib = bm.consumedPages() * 1.0 * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0;
      col << gib;
   });
   columns.emplace("space_usage_kib", [&](Column& col) {
      const double kib = bm.consumedPages() * 1.0 * PAGE_SIZE / 1024.0;
      col << kib;
   });
   columns.emplace("consumed_pages", [&](Column& col) { col << bm.consumedPages(); });
   // Live PIDs (allocated - freed) over the database region [0, max_pid).
   columns.emplace("ssd_util_pct", [&](Column& col) { col << (bm.consumedPages() * 100.0 / bm.max_pid); });
   // Roughly sums up to ssd_util_pct, freed and not yet written pages make the difference.
   for (ruh_id_t ruh = 0; ruh < storage::fdp::num_ruhs; ++ruh) {
      columns.emplace("ruh_" + to_string(ruh) + "_util_pct", [ruh, this](Column& col) {
         col << (bm.ruh_pages[ruh].pages.load(std::memory_order_relaxed) * 100.0 / bm.max_pid);
      });
   }
   // --compress_pages : cumulative compression ratio of the written pages, and the 4 KiB blocks in use.
   columns.emplace("comp_ratio", [&](Column& col) {
      const u64 bytes = bm.compressed_space ? bm.compressed_space->written_compressed_bytes.load() : 0;
      col << (bytes ? bm.compressed_space->written_pages.load() * PAGE_SIZE * 1.0 / bytes : 0.0);
   });
   columns.emplace("comp_used_gib", [&](Column& col) {
      col << (bm.compressed_space ? bm.compressed_space->usedBlocks() * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0 : 0.0);
   });
   columns.emplace("p1_pct", [&](Column& col) { col << (local_phase_1_ms * 100.0 / total); });
   columns.emplace("p2_pct", [&](Column& col) { col << (local_phase_2_ms * 100.0 / total); });
   columns.emplace("p3_pct", [&](Column& col) { col << (local_phase_3_ms * 100.0 / total); });
   columns.emplace("poll_pct", [&](Column& col) { col << ((local_poll_ms * 100.0 / total)); });
   columns.emplace("find_parent_pct", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::find_parent_ms) * 100.0 / total); });
   columns.emplace("iterate_children_pct",
                   [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::iterate_children_ms) * 100.0 / total); });
   columns.emplace("pc1", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_1_counter)); });
   columns.emplace("free_pct", [&](Column& col) { col << (local_total_free * 100.0 / bm.getPoolSize()); });
   columns.emplace("evicted_mib",
                   [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::evicted_pages) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0); });
   columns.emplace("discarded_mib", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::discarded_pages) * PAGE_SIZE / 1048576.0); });
   // columns.emplace("rounds", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::pp_thread_rounds)); });
   columns.emplace("pp_write_latency", [&](Column& col) {
      col << (sum(PPCounters::pp_counters, &PPCounters::io_phase_us) * 1.0 / (sum(PPCounters::pp_counters, &PPCounters::pp_thread_rounds))); });
   columns.emplace("touches", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::touched_bfs_counter)); });
   columns.emplace("unswizzled", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::unswizzled_pages_counter)); });
   columns.emplace("w_mib", [&](Column& col) {
      col << (sum(PPCounters::pp_counters, &PPCounters::flushed_pages_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("allocate_ops", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter)); });
   columns.emplace("r_mib", [&](Column& col) {
      col << (local_read_operations_counter * PAGE_SIZE / 1024.0 / 1024.0);
   });
   columns.emplace("split_w_mib", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::write_just_splitted) * PAGE_SIZE/1048576.0); });
   columns.emplace("new_w_mib", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::write_new_page) * PAGE_SIZE/1048576.0); });
   columns.emplace("pplf_w_mib", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::ppl_buffer_full) * PAGE_SIZE/1048576.0); });
   for (u8 i = 0; i < 3; ++i) {
      const std::string name = (i < 2) ? "undiscardable_wb_dc" + std::to_string(i) + "_pct" : "undiscardable_wb_dcN_pct";
      columns.emplace(name, [i, this](Column& col) {
         col << (local_undiscardable_wb_total ? (local_undiscardable_wb_discard_count[i] * 100.0 / local_undiscardable_wb_total) : 0.0);
      });
   }
   columns.emplace("parent_full", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::parent_full)); });
   columns.emplace("hot_w_mib", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::hot) * PAGE_SIZE/1048576.0); });
   columns.emplace("dirty_pct", [&](Column& col) {
      col <<
      (sum(WorkerCounters::worker_counters, &WorkerCounters::dirty_read_operations_counter) * 100.0 / local_read_operations_counter);   });
   columns.emplace("order_deletions", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::order_deletions)); });
   columns.emplace("history_deletions", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::history_deletions)); });
   columns.emplace("delivery_fail", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::delivery_fail)); });
   columns.emplace("tpcc_debug1", [&](Column& col) { col << (std::accumulate(WorkerCounters::worker_counters.begin(), WorkerCounters::worker_counters.end(), 0, [](uint64_t sum, const WorkerCounters& wc){ return sum + wc.tpcc_order_insert;})); });
   columns.emplace("tpcc_debug2", [&](Column& col) { col << (std::accumulate(WorkerCounters::worker_counters.begin(), WorkerCounters::worker_counters.end(), 0, [](uint64_t sum, const WorkerCounters& wc){ return sum + wc.tpcc_order_erase;})); });
   columns.emplace("tpcc_debug3", [&](Column& col) { col << (std::accumulate(WorkerCounters::worker_counters.begin(), WorkerCounters::worker_counters.end(), 0, [](uint64_t sum, const WorkerCounters& wc){ return sum + wc.tpcc_order_erase_skipped_new_order;})); });
   columns.emplace("osskip", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::tpcc_order_status_skip)); });

   columns.emplace("rcnt", [&](Column& col) { col << (ioReadHist.getCounter()); });
   columns.emplace("rmin", [&](Column& col) { col << (ioReadHist.getMin()); });
   columns.emplace("ravg", [&](Column& col) { col << (ioReadHist.getAvg()); });
   columns.emplace("r10p", [&](Column& col) { col << (ioReadHist.getPercentile(10)); });
   columns.emplace("r20p", [&](Column& col) { col << (ioReadHist.getPercentile(20)); });
   columns.emplace("r30p", [&](Column& col) { col << (ioReadHist.getPercentile(30)); });
   columns.emplace("r40p", [&](Column& col) { col << (ioReadHist.getPercentile(40)); });
   columns.emplace("r50p", [&](Column& col) { col << (ioReadHist.getPercentile(50)); });
   columns.emplace("r60p", [&](Column& col) { col << (ioReadHist.getPercentile(60)); });
   columns.emplace("r70p", [&](Column& col) { col << (ioReadHist.getPercentile(70)); });
   columns.emplace("r80p", [&](Column& col) { col << (ioReadHist.getPercentile(80)); });
   columns.emplace("r90p", [&](Column& col) { col << (ioReadHist.getPercentile(90)); });
   columns.emplace("r95p", [&](Column& col) { col << (ioReadHist.getPercentile(95)); });
   columns.emplace("r99p", [&](Column& col) { col << (ioReadHist.getPercentile(99)); });
   columns.emplace("r99p9", [&](Column& col) { col << (ioReadHist.getPercentile(99.9)); });
   columns.emplace("r99p99", [&](Column& col) { col << (ioReadHist.getPercentile(99.99)); });
   columns.emplace("r99p999", [&](Column& col) { col << (ioReadHist.getPercentile(99.999)); });
   columns.emplace("rmax", [&](Column& col) { col << (ioReadHist.getMax()); });

#if 1
   columns.emplace("redocnt", [&](Column& col) { col << (redoHist.getCounter()); });
   columns.emplace("redomin", [&](Column& col) { col << (redoHist.getMin()); });
   columns.emplace("redoavg", [&](Column& col) { col << (redoHist.getAvg()); });
   columns.emplace("redo10p", [&](Column& col) { col << (redoHist.getPercentile(10)); });
   columns.emplace("redo20p", [&](Column& col) { col << (redoHist.getPercentile(20)); });
   columns.emplace("redo30p", [&](Column& col) { col << (redoHist.getPercentile(30)); });
   columns.emplace("redo40p", [&](Column& col) { col << (redoHist.getPercentile(40)); });
   columns.emplace("redo50p", [&](Column& col) { col << (redoHist.getPercentile(50)); });
   columns.emplace("redo60p", [&](Column& col) { col << (redoHist.getPercentile(60)); });
   columns.emplace("redo70p", [&](Column& col) { col << (redoHist.getPercentile(70)); });
   columns.emplace("redo80p", [&](Column& col) { col << (redoHist.getPercentile(80)); });
   columns.emplace("redo90p", [&](Column& col) { col << (redoHist.getPercentile(90)); });
   columns.emplace("redo95p", [&](Column& col) { col << (redoHist.getPercentile(95)); });
   columns.emplace("redo99p", [&](Column& col) { col << (redoHist.getPercentile(99)); });
   columns.emplace("redo99p9", [&](Column& col) { col << (redoHist.getPercentile(99.9)); });
   columns.emplace("redo99p99", [&](Column& col) { col << (redoHist.getPercentile(99.99)); });
   columns.emplace("redo99p999", [&](Column& col) { col << (redoHist.getPercentile(99.999)); });
   columns.emplace("redomax", [&](Column& col) { col << (redoHist.getMax()); });
#endif

   columns.emplace("wmin", [&](Column& col) { col << (ioWriteHist.getMin()); });
   columns.emplace("wavg", [&](Column& col) { col << (ioWriteHist.getAvg()); });
   columns.emplace("w10p", [&](Column& col) { col << (ioWriteHist.getPercentile(10)); });
   columns.emplace("w20p", [&](Column& col) { col << (ioWriteHist.getPercentile(20)); });
   columns.emplace("w30p", [&](Column& col) { col << (ioWriteHist.getPercentile(30)); });
   columns.emplace("w40p", [&](Column& col) { col << (ioWriteHist.getPercentile(40)); });
   columns.emplace("w50p", [&](Column& col) { col << (ioWriteHist.getPercentile(50)); });
   columns.emplace("w60p", [&](Column& col) { col << (ioWriteHist.getPercentile(60)); });
   columns.emplace("w70p", [&](Column& col) { col << (ioWriteHist.getPercentile(70)); });
   columns.emplace("w80p", [&](Column& col) { col << (ioWriteHist.getPercentile(80)); });
   columns.emplace("w90p", [&](Column& col) { col << (ioWriteHist.getPercentile(90)); });
   columns.emplace("w95p", [&](Column& col) { col << (ioWriteHist.getPercentile(95)); });
   columns.emplace("w99p", [&](Column& col) { col << (ioWriteHist.getPercentile(99)); });
   columns.emplace("w99p9", [&](Column& col) { col << (ioWriteHist.getPercentile(99.9)); });
   columns.emplace("w99p99", [&](Column& col) { col << (ioWriteHist.getPercentile(99.99)); });
   columns.emplace("w99p999", [&](Column& col) { col << (ioWriteHist.getPercentile(99.999)); });
   columns.emplace("wmax", [&](Column& col) { col << (ioWriteHist.getMax()); });

   columns.emplace("txcnt", [&](Column& col) { col << (txHist.getCounter()); });
   columns.emplace("txmin", [&](Column& col) { col << (txHist.getMin()); });
   columns.emplace("txavg", [&](Column& col) { col << (txHist.getAvg()); });
   columns.emplace("tx10p", [&](Column& col) { col << (txHist.getPercentile(10)); });
   columns.emplace("tx20p", [&](Column& col) { col << (txHist.getPercentile(20)); });
   columns.emplace("tx30p", [&](Column& col) { col << (txHist.getPercentile(30)); });
   columns.emplace("tx40p", [&](Column& col) { col << (txHist.getPercentile(40)); });
   columns.emplace("tx50p", [&](Column& col) { col << (txHist.getPercentile(50)); });
   columns.emplace("tx60p", [&](Column& col) { col << (txHist.getPercentile(60)); });
   columns.emplace("tx70p", [&](Column& col) { col << (txHist.getPercentile(70)); });
   columns.emplace("tx80p", [&](Column& col) { col << (txHist.getPercentile(80)); });
   columns.emplace("tx90p", [&](Column& col) { col << (txHist.getPercentile(90)); });
   columns.emplace("tx95p", [&](Column& col) { col << (txHist.getPercentile(95)); });
   columns.emplace("tx99p", [&](Column& col) { col << (txHist.getPercentile(99)); });
   columns.emplace("tx99p9", [&](Column& col) { col << (txHist.getPercentile(99.9)); });
   columns.emplace("tx99p99", [&](Column& col) { col << (txHist.getPercentile(99.99)); });
   columns.emplace("tx99p999", [&](Column& col) { col << (txHist.getPercentile(99.999)); });
   columns.emplace("txmax", [&](Column& col) { col << (txHist.getMax()); });
   columns.emplace("tximin", [&](Column& col) { col << (txIncWaitHist.getMin()); });
   columns.emplace("txiavg", [&](Column& col) { col << (txIncWaitHist.getAvg()); });
   columns.emplace("txi10p", [&](Column& col) { col << (txIncWaitHist.getPercentile(10)); });
   columns.emplace("txi20p", [&](Column& col) { col << (txIncWaitHist.getPercentile(20)); });
   columns.emplace("txi30p", [&](Column& col) { col << (txIncWaitHist.getPercentile(30)); });
   columns.emplace("txi40p", [&](Column& col) { col << (txIncWaitHist.getPercentile(40)); });
   columns.emplace("txi50p", [&](Column& col) { col << (txIncWaitHist.getPercentile(50)); });
   columns.emplace("txi60p", [&](Column& col) { col << (txIncWaitHist.getPercentile(60)); });
   columns.emplace("txi70p", [&](Column& col) { col << (txIncWaitHist.getPercentile(70)); });
   columns.emplace("txi80p", [&](Column& col) { col << (txIncWaitHist.getPercentile(80)); });
   columns.emplace("txi90p", [&](Column& col) { col << (txIncWaitHist.getPercentile(90)); });
   columns.emplace("txi95p", [&](Column& col) { col << (txIncWaitHist.getPercentile(95)); });
   columns.emplace("txi99p", [&](Column& col) { col << (txIncWaitHist.getPercentile(99)); });
   columns.emplace("txi99p9", [&](Column& col) { col << (txIncWaitHist.getPercentile(99.9)); });
   columns.emplace("txi99p99", [&](Column& col) { col << (txIncWaitHist.getPercentile(99.99)); });
   columns.emplace("txi99p999", [&](Column& col) { col << (txIncWaitHist.getPercentile(99.999)); });
   columns.emplace("tximax", [&](Column& col) { col << (txIncWaitHist.getMax()); });
   columns.emplace("failed_try_pop", [&](Column& col) {
         col << (sum(WorkerCounters::worker_counters, &WorkerCounters::failed_try_pop) * 100.0 /sum(WorkerCounters::worker_counters, &WorkerCounters::total_try_pop));
   });
   columns.emplace("yielded_cpu_no_frame", [&](Column& col) {
      col << (sum(WorkerCounters::worker_counters, &WorkerCounters::yielded_cpu_no_frame));
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("wait_for_frame_us", [&](Column& col) {
      col << (sum(WorkerCounters::worker_counters, &WorkerCounters::blocked_on_frame_ns) / 1000.0);
   });
   columns.emplace("on_demand_redo_us", [&](Column& col) {
      col << (sum(WorkerCounters::worker_counters, &WorkerCounters::on_demand_redo_ns) / 1000.0);
   });
   columns.emplace("max_on_demand_redo_ns", [&](Column& col) {
      col << (max(WorkerCounters::worker_counters, &WorkerCounters::max_on_demand_redo_ns));
   });
   // -------------------------------------------------------------------------------------
   // GARBAGE COLLECTION STATS
   columns.emplace("gc_fixed_mib", [&](Column& col) {
      col << (sum(GCCounters::gc_counters, &GCCounters::total_fixed) * PAGE_SIZE / 1024.0 / 1024.0);
   });
   columns.emplace("gc_write_mib", [&](Column& col) {
      col << (sum(GCCounters::gc_counters, &GCCounters::pages_written) * PAGE_SIZE / 1024.0 / 1024.0);
   });
   columns.emplace("gc_read_mib", [&](Column& col) {
      col << (sum(GCCounters::gc_counters, &GCCounters::pages_read) * PAGE_SIZE / 1024.0 / 1024.0);
   });
   columns.emplace("fasle_dirty", [&](Column& col) {
      col << sum(GCCounters::gc_counters, &GCCounters::dirty_in_other_ru_epoch);
   });
   columns.emplace("gc_clean", [&](Column& col) {
      col << sum(GCCounters::gc_counters, &GCCounters::clean);
   });
   columns.emplace("gc_hot", [&](Column& col) {
      col << sum(GCCounters::gc_counters, &GCCounters::hot_fixed);
   });
   columns.emplace("gc_tree_lookups", [&](Column& col) {
      col << sum(GCCounters::gc_counters, &GCCounters::tree_lookups);
   });
   columns.emplace("gc_tree_retries", [&](Column& col) {
      col << sum(GCCounters::gc_counters, &GCCounters::tree_retries);
   });
   columns.emplace("gc_tree_other_epoch", [&](Column& col) {
      col << sum(GCCounters::gc_counters, &GCCounters::tree_other_ru_epoch);
   });
   // -------------------------------------------------------------------------------------
   // BG PAGE FIXER DEBUGGING (livelock / deadlock hunting). Counters are per second, gauges are sampled.
   const auto gc_counter = [this](const std::string& name, atomic<u64> GCCounters::*c) {
      columns.emplace(name, [c](Column& col) { col << sum(GCCounters::gc_counters, c); });
   };
   gc_counter("gc_tree_inner", &GCCounters::tree_inner);
   gc_counter("gc_tree_parent_nf", &GCCounters::tree_parent_not_found);
   gc_counter("gc_tree_swip_cached", &GCCounters::tree_swip_cached);
   gc_counter("gc_tree_swip_clean", &GCCounters::tree_swip_clean);
   gc_counter("gc_tree_io_busy", &GCCounters::tree_io_frame_busy);
   gc_counter("gc_tree_claimed", &GCCounters::tree_claimed);
   gc_counter("gc_filter_free", &GCCounters::filter_free);
   gc_counter("gc_filter_clean", &GCCounters::filter_clean);
   gc_counter("gc_filter_other_log", &GCCounters::filter_other_log);
   gc_counter("gc_filter_passed", &GCCounters::filter_passed);
   gc_counter("gc_hot_deferred", &GCCounters::hot_deferred);
   gc_counter("gc_drain_rounds", &GCCounters::drain_rounds);
   gc_counter("gc_blocks_read", &GCCounters::blocks_read);
   gc_counter("gc_blocks_overwritten", &GCCounters::blocks_overwritten);
   gc_counter("gc_dead_pages", &GCCounters::dead_pages);
   gc_counter("gc_clean_copied", &GCCounters::clean_copied);
   // --compress_pages, cumulative: pages written in total over the ones the database had to write, the clean copies of
   // the bg page fixer being the extra ones.
   columns.emplace("db_waf", [this](Column& col) {
      const u64 written = bm.compressed_space ? bm.compressed_space->written_pages.load() : 0;
      const u64 copied = bm.compressed_space ? bm.compressed_space->clean_copied_pages.load() : 0;
      col << (written > copied ? written * 1.0 / (written - copied) : 1.0);
   });
   columns.emplace("gc_tree_max_retries", [](Column& col) {
      u64 m = 0;
      for (auto& c : GCCounters::gc_counters) {
         m = std::max<u64>(m, c.tree_max_retries.exchange(0));
      }
      col << m;
   });
   // One letter per ru_gc thread, see RUBackgroundPageFixer::phase_letters.
   columns.emplace("gc_phases", [this](Column& col) {
      std::string phases;
      if (bm.ru_bg_page_fixer) {
         for (s32 t = 0; t < FLAGS_ru_gc_threads; ++t) {
            phases += RUBackgroundPageFixer::phase_letters[bm.ru_bg_page_fixer->stats.threads[t].phase.load(std::memory_order_relaxed)];
         }
      }
      col << (phases.empty() ? std::string("-") : phases);
   });
   for (u8 p = 0; p < static_cast<u8>(RUBackgroundPageFixer::Phase::COUNT); ++p) {
      columns.emplace(std::string("gc_thr_") + RUBackgroundPageFixer::phase_names[p], [p, this](Column& col) {
         u64 n = 0;
         if (bm.ru_bg_page_fixer) {
            for (s32 t = 0; t < FLAGS_ru_gc_threads; ++t) {
               n += bm.ru_bg_page_fixer->stats.threads[t].phase.load(std::memory_order_relaxed) == p;
            }
         }
         col << n;
      });
   }
   // The longest time a busy (not idle) ru_gc thread has spent in its current phase, a stuck thread keeps growing it.
   columns.emplace("gc_max_phase_ms", [this](Column& col) {
      u64 max_ns = 0;
      if (bm.ru_bg_page_fixer) {
         const u64 now = RUBackgroundPageFixer::nowNs();
         for (s32 t = 0; t < FLAGS_ru_gc_threads; ++t) {
            auto& ts = bm.ru_bg_page_fixer->stats.threads[t];
            if (ts.phase.load(std::memory_order_relaxed) == static_cast<u8>(RUBackgroundPageFixer::Phase::IDLE)) {
               continue;
            }
            const u64 since = ts.phase_since_ns.load(std::memory_order_relaxed);
            max_ns = std::max<u64>(max_ns, now > since ? now - since : 0);
         }
      }
      col << max_ns / 1000000;
   });
   columns.emplace("gc_deferred_hot_now", [this](Column& col) {
      u64 n = 0;
      if (bm.ru_bg_page_fixer) {
         for (s32 t = 0; t < FLAGS_ru_gc_threads; ++t) {
            n += bm.ru_bg_page_fixer->stats.threads[t].deferred_hot.load(std::memory_order_relaxed);
         }
      }
      col << n;
   });
   columns.emplace("gc_io_frames_held", [this](Column& col) {
      col << (bm.ru_bg_page_fixer ? bm.ru_bg_page_fixer->stats.io_frames_held.load(std::memory_order_relaxed) : s64(0));
   });
   columns.emplace("gc_epoch", [this](Column& col) {
      col << (bm.ru_bg_page_fixer ? bm.ru_bg_page_fixer->stats.cur_epoch.load(std::memory_order_relaxed) : s64(-1));
   });
   columns.emplace("gc_epoch_age_s", [this](Column& col) {
      u64 age_s = 0;
      if (bm.ru_bg_page_fixer && bm.ru_bg_page_fixer->stats.cur_epoch.load(std::memory_order_relaxed) >= 0) {
         age_s = (RUBackgroundPageFixer::nowNs() - bm.ru_bg_page_fixer->stats.cur_epoch_start_ns.load(std::memory_order_relaxed)) / 1000000000;
      }
      col << age_s;
   });
   columns.emplace("gc_chain_entries", [this](Column& col) {
      col << (bm.ru_bg_page_fixer ? bm.ru_bg_page_fixer->stats.chain_entries.load(std::memory_order_relaxed) : u64(0));
   });
   columns.emplace("gc_chain_done_pct", [this](Column& col) {
      double pct = 0;
      if (bm.ru_bg_page_fixer) {
         const u64 entries = bm.ru_bg_page_fixer->stats.chain_entries.load(std::memory_order_relaxed);
         pct = entries ? bm.ru_bg_page_fixer->stats.chain_done.load(std::memory_order_relaxed) * 100.0 / entries : 0.0;
      }
      col << pct;
   });
   columns.emplace("gc_jobs_enqueued", [this](Column& col) {
      col << (bm.ru_bg_page_fixer ? bm.ru_bg_page_fixer->stats.jobs_enqueued.load(std::memory_order_relaxed) : u64(0));
   });
   columns.emplace("gc_enqueue_fail", [this](Column& col) {
      col << (bm.ru_bg_page_fixer ? bm.ru_bg_page_fixer->stats.enqueue_failures.exchange(0, std::memory_order_relaxed) : u64(0));
   });
   // RU epochs of the DEFAULT_RUH: open, being reclaimed, reclaimed, the oldest not handed to the GC yet.
   columns.emplace("ru_open", [this](Column& col) { col << bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed); });
   columns.emplace("ru_reclaiming", [this](Column& col) { col << bm.reclaiming_ru_epoch.load(std::memory_order_relaxed); });
   columns.emplace("ru_reclaimed", [this](Column& col) { col << bm.reclaimed_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed); });
   columns.emplace("ru_oldest_uncollected", [this](Column& col) {
      col << bm.oldest_uncollected_ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed);
   });
   // 1 when the slot of the next RU epoch is still in use: the ru_epoch_mgr defers opening it (the GC is behind).
   columns.emplace("ru_next_slot_busy", [this](Column& col) {
      if (!FLAGS_enable_discarding) {
         col << 0;
         return;
      }
      const ru_epoch_t next = bm.ru_epoch[DEFAULT_RUH].load(std::memory_order_relaxed) + 1;
      col << static_cast<u32>(bm.ru_discard_set[DEFAULT_RUH].at(next).active.load(std::memory_order_relaxed));
   });
   columns.emplace("lba_stalls", [this](Column& col) {
      u64 delta = 0;
      if (bm.lba_chain) {
         const u64 stalls = bm.lba_chain->stalls();
         delta = stalls - prev_lba_chain_stalls;
         prev_lba_chain_stalls = stalls;
      }
      col << delta;
   });
   columns.emplace("lba_stalled_now", [this](Column& col) { col << (bm.lba_chain ? bm.lba_chain->stalledWriters() : s64(0)); });
   // Workers
   columns.emplace("wk_fixer_wait_now", [this](Column& col) { col << bm.bm_stats.workers_waiting_fixer_io.load(std::memory_order_relaxed); });
   columns.emplace("wk_fixer_waits", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::fixer_io_frame_waits); });
   columns.emplace("wk_fixer_wait_ms", [](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::fixer_io_frame_wait_ns) / 1000000.0;
   });
   columns.emplace("wk_fixer_max_wait_ms", [](Column& col) {
      u64 m = 0;
      for (auto& c : WorkerCounters::worker_counters) {
         m = std::max<u64>(m, c.fixer_io_frame_max_wait_ns.exchange(0));
      }
      col << m / 1000000.0;
   });
   columns.emplace("wk_io_reading_waits", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::io_frame_reading_waits); });
   columns.emplace("wk_io_to_delete_retries", [](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::io_frame_to_delete_retries);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("discard_state_peak_mem_usage", [this](Column& col) {
      col << bm.bm_stats.discard_state_peak_mem_usage.load(std::memory_order_acquire) / 1073741824.0;
   });
   // TODO(mfd) : decompose per ruh usage
   columns.emplace("total_in_use_ru", [this](Column& col) {
      s64 in_use_ru = 0;
      for (ruh_id_t ruh = 0; ruh < storage::fdp::num_ruhs; ++ruh) {
         in_use_ru += bm.ru_epoch[ruh].load(std::memory_order_acquire) - bm.reclaimed_ru_epoch[ruh].load(std::memory_order_acquire);
      }
      col << (in_use_ru * 100.0 / bm.max_open_ru_epochs);
   });
   columns.emplace("estimated_gc_writes", [this](Column& col) {
      // PAGE_SIZE = 4KiB
      col << bm.bm_stats.estimated_gc_writes.load(std::memory_order_acquire) * 4 / 1048576.0;
   });
   columns.emplace("forced_gc_count", [this](Column& col) {
      col << bm.bm_stats.forced_gc_count.load(std::memory_order_acquire);
   });
   columns.emplace("device_gc_events", [this](Column& col) {
      col << bm.bm_stats.device_gc_events.load(std::memory_order_relaxed);
   });
   // -------------------------------------------------------------------------------------
   for (u8 i = 0; i <= bm.max_pending_lsn; ++i) {
      columns.emplace("read_depth_" + to_string(i), [i, this](Column& col) {
         col << local_read_operations_histogram[i];
      });
   }
   for (u8 i = 0; i <= bm.max_pending_lsn; ++i) {
      columns.emplace("read_latency_" + to_string(i) + "_us", [i, this](Column& col) {
         col << (local_io_phase_us[i] * 1.0) / local_read_operations_histogram[i];
      });
   }
   columns.emplace("read_latency_us", [this](Column& col) {
      col << (local_agg_io_phase_us * 1.0) / local_read_operations_counter;
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("log_reads", [this](Column& col) {
      col << local_log_reads_from_device;
   });
   columns.emplace("log_reads_in_wal_buffer", [this](Column& col) {
      col << local_log_reads_in_wal_buffer;
   });
   columns.emplace("log_reads_in_wal_buffer_pct", [this](Column& col) {
      const u64 total_log_reads = local_log_reads_from_device + local_log_reads_in_wal_buffer;
      col << (total_log_reads ? (local_log_reads_in_wal_buffer * 100.0) / (total_log_reads) : 0.0);
   });
   columns.emplace("consecutive_same_key_in_page", [this](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::consecutive_same_key_in_page);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("txns_experienced_buffer_miss", [this](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::txns_experienced_buffer_miss);
   });
   // -------------------------------------------------------------------------------------
   // TODO(mfd) : Add a macro for TPCC_DEBUG_COUNTERS
#if 1
   columns.emplace("stock_discard_mib", [this](Column& col) {
      col << sum(PPCounters::pp_counters, &PPCounters::discarded_pages_per_dtid, 9) * PAGE_SIZE / 1048576.0;
   });
   columns.emplace("stock_w_mib", [this](Column& col) {
      col << sum(PPCounters::pp_counters, &PPCounters::written_back_pages_per_dtid, 9) * PAGE_SIZE / 1048576.0;
   });
   columns.emplace("orderline_discard_mib", [this](Column& col) {
      col << sum(PPCounters::pp_counters, &PPCounters::discarded_pages_per_dtid, 10) * PAGE_SIZE / 1048576.0;
   });
   columns.emplace("orderline_w_mib", [this](Column& col) {
      col << sum(PPCounters::pp_counters, &PPCounters::written_back_pages_per_dtid, 10) * PAGE_SIZE / 1048576.0;
   });
   columns.emplace("dirty_ol_mib", [this](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::dirty_orderlike_counter) * PAGE_SIZE / 1048576.0;
   });
#endif
}
// -------------------------------------------------------------------------------------
void BMTable::next()
{
   clear();
   local_phase_1_ms = sum(PPCounters::pp_counters, &PPCounters::phase_1_ms);
   local_phase_2_ms = sum(PPCounters::pp_counters, &PPCounters::phase_2_ms);
   local_phase_3_ms = sum(PPCounters::pp_counters, &PPCounters::phase_3_ms);
   local_poll_ms = sum(PPCounters::pp_counters, &PPCounters::poll_ms);
   // -------------------------------------------------------------------------------------
   local_total_free = 0;
   for (u64 p_i = 0; p_i < bm.partitions_count; p_i++) {
      local_total_free += bm.partitions[p_i]->dram_free_list.counter.load();
   }
   total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
   // -------------------------------------------------------------------------------------
   local_read_operations_counter = sum(WorkerCounters::worker_counters, &WorkerCounters::read_operations_counter);
   local_log_reads_from_device = sum(WorkerCounters::worker_counters, &WorkerCounters::log_reads_from_device);
   local_log_reads_in_wal_buffer = sum(WorkerCounters::worker_counters, &WorkerCounters::log_reads_in_wal_buffer);
   local_undiscardable_wb_total = 0;
   for (u8 i = 0; i < 3; ++i) {
      local_undiscardable_wb_discard_count[i] = sum(PPCounters::pp_counters, &PPCounters::undiscardable_wb_discard_count, i);
      local_undiscardable_wb_total += local_undiscardable_wb_discard_count[i];
   }
   local_agg_io_phase_us = 0;
   for (u8 i = 0; i <= bm.max_pending_lsn; ++i) {
      local_io_phase_us[i] = sum(WorkerCounters::worker_counters, &WorkerCounters::io_phase_us, i);
      local_agg_io_phase_us += local_io_phase_us[i];
      local_read_operations_histogram[i] = sum(WorkerCounters::worker_counters, &WorkerCounters::read_operations_histogram, i);
   }
   // -------------------------------------------------------------------------------------
   // worker io read latency histogram, every N seconds
   WorkerCounters::worker_counters.begin()->seconds++;
   if (WorkerCounters::worker_counters.begin()->seconds % (5*60) == 0) {
      // aggregate the histograms
      ioReadHist.resetData();
      redoHist.resetData();
      ioWriteHist.resetData();
      txHist.resetData();
      txIncWaitHist.resetData();
      for (auto& wc : WorkerCounters::worker_counters) {
         ioReadHist += wc.ioReadHist;
         redoHist += wc.redoHist;
         ioWriteHist += wc.ioWriteHist;
         txHist += wc.txHist;
         txIncWaitHist += wc.txIncWaitHist;
         {
            std::lock_guard<std::mutex> lock(wc.ioReadHistLock);
            wc.ioReadHist.resetData();
         }
         {
            std::lock_guard<std::mutex> lock(wc.redoHistLock);
            wc.redoHist.resetData();
         }
         {
            std::lock_guard<std::mutex> lock(wc.ioWriteHistLock);
            wc.ioWriteHist.resetData();
         }
         {
            std::lock_guard<std::mutex> lock(wc.txHistLock);
            wc.txHist.resetData();
         }
         {
            std::lock_guard<std::mutex> lock(wc.txIncWaitHistLock);
            wc.txIncWaitHist.resetData();
         }
      }
      // -------------------------------------------------------------------------------------
   }
   if (WorkerCounters::worker_counters.begin()->seconds % (30 * 60) == 0) {
      for (u8 i = 1; i < 64; ++i) {
         absorption_histogram_file << sum(PPCounters::pp_counters, &PPCounters::absorbed_writes_histogram, i) << ",";
      }
      absorption_histogram_file << std::endl;
      for (u8 i = 1; i < 64; ++i) {
         absorption_histogram_file << sum(GCCounters::gc_counters, &GCCounters::absorbed_writes_histogram, i) << ",";
      }
      absorption_histogram_file << std::endl;
   }
   // -------------------------------------------------------------------------------------
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
