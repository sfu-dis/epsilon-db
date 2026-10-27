#pragma once
#include <gflags/gflags_declare.h>
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_bool(validate_flags_and_exit);
// -------------------------------------------------------------------------------------
DECLARE_double(dram_gib);
DECLARE_double(ssd_gib);
DECLARE_string(ssd_path);
DECLARE_uint32(worker_threads);
DECLARE_bool(cpu_counters);
DECLARE_bool(pin_threads);
DECLARE_bool(smt);
DECLARE_string(csv_path);
DECLARE_bool(csv_truncate);
DECLARE_uint32(free_pct);
DECLARE_uint32(partition_bits);
DECLARE_uint32(write_buffer_size);
DECLARE_uint32(pp_threads);
DECLARE_bool(worker_page_eviction);
DECLARE_bool(trunc);
DECLARE_bool(root);
DECLARE_bool(print_debug);
DECLARE_bool(print_tx_console);
DECLARE_bool(crc_check);
// -------------------------------------------------------------------------------------
DECLARE_bool(optimistic_scan);
DECLARE_bool(measure_time);
// -------------------------------------------------------------------------------------
DECLARE_double(zipf_factor);
DECLARE_double(target_gib);
DECLARE_uint64(run_for_seconds);
// -------------------------------------------------------------------------------------
DECLARE_double(tmp4);
DECLARE_double(tmp5);
DECLARE_double(tmp6);
// -------------------------------------------------------------------------------------
DECLARE_bool(btree_print_height);
DECLARE_bool(btree_print_tuples_count);
DECLARE_bool(btree_prefix_compression);
DECLARE_int64(btree_hints);
DECLARE_bool(btree_heads);
DECLARE_bool(bulk_insert);
DECLARE_bool(btree_split_alloc_right);
DECLARE_bool(btree_pin_inner_nodes);
// -------------------------------------------------------------------------------------
DECLARE_int64(trace_dt_id);
DECLARE_int64(trace_trigger_probability);
DECLARE_string(tag);
// -------------------------------------------------------------------------------------
DECLARE_bool(optimistic_parent_pointer);
DECLARE_uint64(replacement_chunk_size);
DECLARE_bool(recycle_pages);
// -------------------------------------------------------------------------------------
DECLARE_bool(wal);
DECLARE_bool(wal_rfa);
DECLARE_bool(wal_tuple_rfa);
DECLARE_bool(wal_pwrite);
DECLARE_bool(wal_fsync);
DECLARE_uint64(wal_buffer_size);
DECLARE_string(wal_partition_by);
DECLARE_uint64(wal_partitions_count);
DECLARE_uint32(wal_sink_logs);
// -------------------------------------------------------------------------------------
DECLARE_string(isolation_level);
DECLARE_bool(mv);
DECLARE_bool(todo);
// -------------------------------------------------------------------------------------
DECLARE_bool(vi);
DECLARE_bool(vi_delta);
DECLARE_bool(vi_fremove);
DECLARE_bool(vi_fupdate_chained);
DECLARE_bool(vi_fupdate_fat_tuple);
DECLARE_uint64(vi_fat_tuple_trigger);
DECLARE_bool(vi_fat_tuple_alternative);
DECLARE_bool(vi_fat_tuple);
DECLARE_bool(vi_dangling_pointer);
// -------------------------------------------------------------------------------------
DECLARE_bool(olap_mode);
DECLARE_bool(graveyard);
// -------------------------------------------------------------------------------------
DECLARE_bool(pgc);
DECLARE_uint64(vi_max_chain_length);
DECLARE_bool(history_tree_inserts);
// -------------------------------------------------------------------------------------
DECLARE_bool(persist);
DECLARE_bool(initialize_leanstore);
DECLARE_bool(skip_wal_recovery);
DECLARE_string(leanstore_state_file);
DECLARE_string(ru_table_stats_file);
// -------------------------------------------------------------------------------------
DECLARE_bool(io_trace);
DECLARE_string(io_trace_file);
// -------------------------------------------------------------------------------------
DECLARE_bool(enable_discarding);
DECLARE_bool(discard_lsns_in_parent);
DECLARE_bool(bg_fixer_write_back_hot_pages);
DECLARE_int32(ru_gc_threads);
DECLARE_uint32(bg_page_fixer_variant);
DECLARE_bool(bg_page_fixer_tree_lookup);
DECLARE_bool(bg_page_fixer_tree_lookup_state_filter);
DECLARE_bool(discard_state_paranoid);
DECLARE_bool(bg_page_fixer_log_io_uring);
DECLARE_uint32(lba_chain_buffer_blocks);
DECLARE_bool(lba_chain_verify);
DECLARE_uint32(max_log_records_to_discard);
DECLARE_double(ru_gc_threshold);
DECLARE_uint32(overprovisioning_ru_epochs);
DECLARE_double(overprovisioning_ratio);
DECLARE_uint32(ru_force_gc_margin);
DECLARE_uint32(ru_force_gc_batch);
DECLARE_uint32(ru_rate_limit_floor);
DECLARE_uint32(ru_size);
DECLARE_bool(per_page_logging);
DECLARE_uint32(ppl_merge_threshold);
DECLARE_bool(opportunistic_log_compaction);
DECLARE_bool(trace_ru_threshold);
// -------------------------------------------------------------------------------------
DECLARE_uint64(tx_rate);
// -------------------------------------------------------------------------------------
DECLARE_string(redo_log_file);
DECLARE_uint64(log_dev_size_gib);
DECLARE_bool(log_same_namespace);
DECLARE_double(log_space_pct);
DECLARE_bool(log_trim_on_reclaim);
DECLARE_bool(log_fake_wrap_around);
// -------------------------------------------------------------------------------------
DECLARE_bool(enable_fdp);
DECLARE_string(fdp_placement_policy);
DECLARE_bool(fdp_count_gc_events);
// -------------------------------------------------------------------------------------
DECLARE_bool(compress_pages);
DECLARE_int32(compression_level);
DECLARE_uint32(compression_max_pages_per_block);
DECLARE_bool(compression_zero_free_space);
DECLARE_string(compression_space_map_file);
