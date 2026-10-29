#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>

// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
struct CRCounters {
   atomic<s64> worker_id = -1;
   // -------------------------------------------------------------------------------------
   atomic<u64> gct_phase_1_ms = 0;
   atomic<u64> gct_phase_2_ms = 0;
   atomic<u64> gct_write_ms = 0;
   atomic<u64> gct_write_bytes = 0;
   atomic<u64> gct_blocks = 0;       // Log device blocks written by the GCT
   atomic<u64> gct_blocks_full = 0;  // Those that are full
   // -------------------------------------------------------------------------------------
   atomic<u64> gct_rounds = 0;
   atomic<u64> gct_committed_tx = 0;
   atomic<u64> rfa_committed_tx = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> cc_versions_space_removed = {0};
   // -------------------------------------------------------------------------------------
   // Time
   atomic<u64> cc_ms_snapshotting = 0; // Everything related to commit log
   atomic<u64> cc_ms_gc = 0;
   atomic<u64> cc_ms_gc_graveyard = 0;
   atomic<u64> cc_ms_gc_history_tree = 0;
   atomic<u64> cc_ms_gc_cm = 0;
   atomic<u64> cc_ms_committing = 0;
   atomic<u64> cc_ms_history_tree_insert = 0;
   atomic<u64> cc_ms_history_tree_retrieve = 0;
   atomic<u64> cc_ms_refresh_global_state = 0;

   atomic<u64> cc_ms_oltp_tx = 0;
   atomic<u64> cc_ms_olap_tx = 0;
   atomic<u64> cc_ms_fat_tuple = 0;
   atomic<u64> cc_ms_fat_tuple_conversion = 0;

   atomic<u64> cc_ms_start_tx = 0;
   atomic<u64> cc_ms_commit_tx = 0;
   atomic<u64> cc_ms_abort_tx = 0;
   // -------------------------------------------------------------------------------------
   CRCounters() {}
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<CRCounters> cr_counters;
   static tbb::enumerable_thread_specific<CRCounters>::reference myCounters() { return cr_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
