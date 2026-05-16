#include "BMTable.hpp"
#include <cstdint>

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/GCCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
BMTable::BMTable(BufferManager& bm) : ProfilingTable(), bm(bm) {}
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
   columns.emplace("p1_pct", [&](Column& col) { col << (local_phase_1_ms * 100.0 / total); });
   columns.emplace("p2_pct", [&](Column& col) { col << (local_phase_2_ms * 100.0 / total); });
   columns.emplace("p3_pct", [&](Column& col) { col << (local_phase_3_ms * 100.0 / total); });
   columns.emplace("poll_pct", [&](Column& col) { col << ((local_poll_ms * 100.0 / total)); });
   columns.emplace("find_parent_pct", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::find_parent_ms) * 100.0 / total); });
   columns.emplace("iterate_children_pct",
                   [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::iterate_children_ms) * 100.0 / total); });
   columns.emplace("pc1", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_1_counter)); });
   columns.emplace("pc2", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_2_counter)); });
   columns.emplace("pc3", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_3_counter)); });
   columns.emplace("free_pct", [&](Column& col) { col << (local_total_free * 100.0 / bm.getPoolSize()); });
   columns.emplace("evicted_mib",
                   [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::evicted_pages) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0); });
   columns.emplace("discarded_mib", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::discarded_pages) * PAGE_SIZE / 1048576.0); });
   columns.emplace("rounds", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::pp_thread_rounds)); });
   columns.emplace("touches", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::touched_bfs_counter)); });
   columns.emplace("unswizzled", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::unswizzled_pages_counter)); });
   columns.emplace("submit_ms", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::submit_ms) * 100.0 / total); });
   columns.emplace("async_mb_ws", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::async_wb_ms)); });
   columns.emplace("w_mib", [&](Column& col) {
      col << (sum(PPCounters::pp_counters, &PPCounters::flushed_pages_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("allocate_ops", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter)); });
   columns.emplace("r_mib", [&](Column& col) {
      col << (local_read_operations_counter * PAGE_SIZE / 1024.0 / 1024.0);
   });
   columns.emplace("dirty_pct", [&](Column& col) {
      col << 
      (sum(WorkerCounters::worker_counters, &WorkerCounters::dirty_read_operations_counter) * 100.0 / local_read_operations_counter);   });
   columns.emplace("order_deletions", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::order_deletions)); });
   columns.emplace("history_deletions", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::history_deletions)); });
   columns.emplace("tpcc_time_in_truncate", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::tpcc_time_in_truncate)/1000); });
   columns.emplace("delivery_fail", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::delivery_fail)); });
   columns.emplace("tpcc_debug1", [&](Column& col) { col << (std::accumulate(WorkerCounters::worker_counters.begin(), WorkerCounters::worker_counters.end(), 0, [](uint64_t sum, const WorkerCounters& wc){ return sum + wc.tpcc_order_insert;})); });
   columns.emplace("tpcc_debug2", [&](Column& col) { col << (std::accumulate(WorkerCounters::worker_counters.begin(), WorkerCounters::worker_counters.end(), 0, [](uint64_t sum, const WorkerCounters& wc){ return sum + wc.tpcc_order_erase;})); });
   columns.emplace("tpcc_debug3", [&](Column& col) { col << (std::accumulate(WorkerCounters::worker_counters.begin(), WorkerCounters::worker_counters.end(), 0, [](uint64_t sum, const WorkerCounters& wc){ return sum + wc.tpcc_order_erase_skipped_new_order;})); });
   columns.emplace("osskip", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::tpcc_order_status_skip)); });
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
   // -------------------------------------------------------------------------------------
   // GARBAGE COLLECTION STATS
   columns.emplace("gc_fixed_mib", [&](Column& col) {
      col << (sum(GCCounters::gc_counters, &GCCounters::total_fixed) * PAGE_SIZE / 1024.0 / 1024.0);
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
   // -------------------------------------------------------------------------------------
   columns.emplace("discard_state_peak_mem_usage", [&](Column& col) {
      col << BMC::global_bf->bm_stats.discard_state_peak_mem_usage.load(std::memory_order_acquire) / 1073741824.0;
   });
   columns.emplace("total_in_use_ru", [&](Column& col) {
      s64 newest_active_ru = BMC::global_bf->ru_epoch.load(std::memory_order_acquire);
      s64 reclaimed_ru = BMC::global_bf->reclaimed_ru_epoch.load(std::memory_order_acquire);
      col << newest_active_ru - reclaimed_ru;
   });
   columns.emplace("estimated_gc_writes", [&](Column& col) {
      // PAGE_SIZE = 4KiB
      col << BMC::global_bf->bm_stats.estimated_gc_writes.load(std::memory_order_acquire) * 4 / 1048576.0;
   });
   // -------------------------------------------------------------------------------------
   for (u8 i = 0; i <= FLAGS_max_log_records_to_discard; ++i) {
      columns.emplace("read_depth_" + to_string(i), [i, this](Column& col) {
         col << local_read_operations_histogram[i];
      });
   }
   for (u8 i = 0; i <= FLAGS_max_log_records_to_discard; ++i) {
      columns.emplace("read_latency_" + to_string(i) + "_us", [i, this](Column& col) {
         col << (local_io_phase_us[i] * 1.0) / local_read_operations_histogram[i];
      });
   }
   columns.emplace("read_latency_us", [this](Column& col) {
      col << (local_agg_io_phase_us * 1.0) / local_read_operations_counter;
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("ppl_not_yet_persisted", [this](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::ppl_not_yet_persisted);
   });
   columns.emplace("consecutive_same_key_in_page", [this](Column& col) {
      col << sum(WorkerCounters::worker_counters, &WorkerCounters::consecutive_same_key_in_page);
   });
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
      local_total_free += bm.getPartition(p_i).dram_free_list.counter.load();
   }
   total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
   // -------------------------------------------------------------------------------------
   local_read_operations_counter = sum(WorkerCounters::worker_counters, &WorkerCounters::read_operations_counter);
   local_agg_io_phase_us = 0;
   for (u8 i = 0; i <= FLAGS_max_log_records_to_discard; ++i) {
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
      ioWriteHist.resetData();
      txHist.resetData();
      txIncWaitHist.resetData();
      for (auto& wc : WorkerCounters::worker_counters) {
         ioReadHist += wc.ioReadHist;
         ioWriteHist += wc.ioWriteHist;
         txHist += wc.txHist;
         txIncWaitHist += wc.txIncWaitHist;
         {
            std::lock_guard<std::mutex> lock(wc.ioReadHistLock);
            wc.ioReadHist.resetData();
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
   }
   // -------------------------------------------------------------------------------------
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
