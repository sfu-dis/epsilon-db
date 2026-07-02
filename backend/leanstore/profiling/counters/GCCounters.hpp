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
   atomic<u64> hot_fixed;
   atomic<u64> clean;
   atomic<u64> dirty_in_other_ru_epoch;
   atomic<u64> pages_read;
   // -------------------------------------------------------------------------------------
   atomic<u64> absorbed_writes_histogram[64] = {0};
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<GCCounters> gc_counters;
   static tbb::enumerable_thread_specific<GCCounters>::reference myCounters() { return gc_counters.local(); }
};
}  // namespace leanstore
