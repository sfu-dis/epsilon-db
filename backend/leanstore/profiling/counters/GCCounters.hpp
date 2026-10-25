#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
struct GCCounters {
   // -------------------------------------------------------------------------------------
   atomic<u64> total_fixed;
   atomic<u64> pages_written;  // fixed + hot pages written back by the GC
   atomic<u64> hot_fixed;
   atomic<u64> clean;
   atomic<u64> dirty_in_other_ru_epoch;
   atomic<u64> pages_read;
   // FLAGS_bg_page_fixer_tree_lookup: pages looked up in their B-Tree, retries of the lookup (latched or changed
   // node on the path), pages whose copy was written to another RU epoch.
   atomic<u64> tree_lookups;
   atomic<u64> tree_retries;
   atomic<u64> tree_other_ru_epoch;
   // Outcome of the lookups: inner node (skipped), parent not found, swip cached (queued as hot), swip clean, IO frame
   // already present (a worker loads it), claimed for fixing. tree_max_retries is the max retries of a single lookup.
   atomic<u64> tree_inner;
   atomic<u64> tree_parent_not_found;
   atomic<u64> tree_swip_cached;
   atomic<u64> tree_swip_clean;
   atomic<u64> tree_io_frame_busy;
   atomic<u64> tree_claimed;
   atomic<u64> tree_max_retries;
   // FLAGS_bg_page_fixer_tree_lookup_state_filter: pages not read because free, clean or discarded into another log,
   // and the pages that went on to be read.
   atomic<u64> filter_free;
   atomic<u64> filter_clean;
   atomic<u64> filter_other_log;
   atomic<u64> filter_passed;
   // Hot pages that could not be written back yet (frame latched, being written back...), rounds of the final drain.
   atomic<u64> hot_deferred;
   atomic<u64> drain_rounds;
   // -------------------------------------------------------------------------------------
   atomic<u64> absorbed_writes_histogram[64] = {0};
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<GCCounters> gc_counters;
   static tbb::enumerable_thread_specific<GCCounters>::reference myCounters() { return gc_counters.local(); }
};
}  // namespace leanstore
