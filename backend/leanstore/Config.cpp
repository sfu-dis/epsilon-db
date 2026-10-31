#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DEFINE_bool(validate_flags_and_exit, false, "");
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, "");
DEFINE_double(ssd_gib, 0, "");
DEFINE_uint32(free_pct, 1, "pct");
DEFINE_uint32(partition_bits, 12, "bits per partition");
DEFINE_uint32(pp_threads, 10, "number of page provider threads");
DEFINE_bool(worker_page_eviction, false, "");
// -------------------------------------------------------------------------------------
DEFINE_string(csv_path, "./log", "");
DEFINE_bool(csv_truncate, true, "");
DEFINE_string(ssd_path, "./leanstore", "Position of SSD, gets persisted");
DEFINE_uint32(write_buffer_size, 1024, "");
DEFINE_bool(trunc, false, "Truncate file");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_bool(print_tx_console, true, "");
DEFINE_bool(crc_check, false, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 4, "");
DEFINE_bool(cpu_counters, false, "Enable per-thread hardware CPU counters (perf events)");
DEFINE_bool(pin_threads, false, "Responsibility of the driver");
DEFINE_bool(smt, true, "Simultaneous multithreading");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_double(target_gib, 0.0, "size of dataset in gib (exact interpretation depends on the driver)");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
// -------------------------------------------------------------------------------------
DEFINE_bool(optimistic_scan, true, "Jump to next leaf directly if the pointer in the parent has not changed");
DEFINE_bool(measure_time, false, "");
// -------------------------------------------------------------------------------------
DEFINE_double(tmp4, 0.0, "");
DEFINE_double(tmp5, 0.0, "");
DEFINE_double(tmp6, 0.0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(btree_print_height, false, "Print BTree height in destructor");
DEFINE_bool(btree_print_tuples_count, false, "Print # tuples in each BTree in destructor");
DEFINE_bool(btree_prefix_compression, true, "");
DEFINE_bool(btree_heads, true, "Enable heads optimization in lowerBound search");
DEFINE_int64(btree_hints, 1, "0: disabled 1: serial 1: AVX512");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");
DEFINE_bool(btree_split_alloc_right, true, "On split, allocate the new node as the right sibling (current node keeps the left half)");
DEFINE_bool(btree_pin_inner_nodes, false, "Pin B-Tree inner nodes in memory so that they are never evicted");
// -------------------------------------------------------------------------------------
DEFINE_int64(trace_dt_id, -1, "Print a stack trace for page reads for this DT ID");
DEFINE_int64(trace_trigger_probability, 100, "");
// -------------------------------------------------------------------------------------
DEFINE_string(tag, "", "Unique identifier for this, will be appended to each line csv");
// -------------------------------------------------------------------------------------
DEFINE_bool(optimistic_parent_pointer, false, "");
DEFINE_uint64(replacement_chunk_size, 64, "Replacement strategy chunk size");
DEFINE_bool(recycle_pages, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(wal, true, "");
DEFINE_bool(wal_rfa, false, "Remote Flush Avoidance (RFA)");
DEFINE_bool(wal_tuple_rfa, false, "tuple-wise tracking");
DEFINE_bool(wal_pwrite, false, "Does not really write logs on SSD");
DEFINE_bool(wal_fsync, false, "");
DEFINE_uint64(wal_buffer_size, 1024 * 1024 * 10, "");
DEFINE_string(wal_partition_by, "worker", "options: worker, page, ru_epoch");
DEFINE_uint64(wal_partitions_count, 32, "Valid only when we use page based partitioning");
DEFINE_uint32(wal_sink_logs, 32, "");
// -------------------------------------------------------------------------------------
DEFINE_string(isolation_level, "rc", "options: ru (READ_UNCOMMITTED), rc (READ_COMMITTED), si (SNAPSHOT_ISOLATION), ser (SERIALIZABLE)");
DEFINE_bool(mv, true, "Multi-version");
DEFINE_bool(todo, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(vi, false, "BTree with SI using in-place version");
DEFINE_bool(vi_delta, true, "");
DEFINE_bool(vi_fremove, false, "");
DEFINE_bool(vi_fupdate_chained, false, "");
DEFINE_bool(vi_fupdate_fat_tuple, false, "");
DEFINE_bool(vi_fat_tuple, false, "");
DEFINE_uint64(vi_fat_tuple_trigger, 0, "1: oldest_oltp, 1: probability");
DEFINE_bool(vi_fat_tuple_alternative, false, "hit the previous version at every update");
DEFINE_bool(vi_dangling_pointer, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(olap_mode, true, "Use OLAP mode for long running transactions");
DEFINE_bool(graveyard, true, "Use Graveyard Index");
// -------------------------------------------------------------------------------------
DEFINE_bool(pgc, true, "Precise garbage collection/recycling");
DEFINE_uint64(vi_max_chain_length, 1000, "");
DEFINE_bool(history_tree_inserts, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(persist, false, "This run is persistent: on shutdown flush everything and write a recoverable state file for a later run");
DEFINE_bool(initialize_leanstore, false,
            "Initialize the database and the device (fresh SSD layout). Otherwise recover from leanstore_state_file, which "
            "must have been written by a --persist run");
DEFINE_bool(skip_wal_recovery, true, "Hacky flag used to allow recovering without checking the WAL (.i.e, assuming no crash)");
DEFINE_string(leanstore_state_file, "./leanstore.json", "State file written by a --persist run and read when recovering");
DEFINE_string(ru_table_stats_file, "./ru_table_stats.json", "Where the per table (dt_id) RU epoch stats are persisted to and recovered from");
// -------------------------------------------------------------------------------------
DEFINE_bool(io_trace, false, "");
DEFINE_string(io_trace_file, "./iotrace.csv", "io trace file");
// -------------------------------------------------------------------------------------
DEFINE_bool(enable_discarding, true, "Discard dirty pages upon evictions");
DEFINE_bool(discard_lsns_in_parent, true, "Store the pending LSNs of a discarded page next to its swip in the parent instead of the page state");
DEFINE_bool(bg_fixer_write_back_hot_pages, false, "The RU bg page fixer writes back the cached (hot) pages of the RU epoch it reclaims");
DEFINE_int32(ru_gc_threads, 4, "Number of threads that will fix pages in RU epochs");
DEFINE_uint32(bg_page_fixer_variant, 1, "1: the bg page fixer scans the log of the RU epoch for the pages to fix. "
              "2: it walks the chain of the LBAs written to the RU epoch (DEFAULT_RUH only), with compress_pages the LBAs are blocks");
DEFINE_bool(bg_page_fixer_tree_lookup, false, "bg_page_fixer_variant 2: the bg page fixer reads every page of the LBA chain and looks for its "
            "parent with the page's fence. Whether the page is discarded, and its pending LSNs, come from the parent swip instead of the page state");
DEFINE_bool(bg_page_fixer_tree_lookup_state_filter, false, "bg_page_fixer_tree_lookup: the page state filters the pages of the LBA chain "
            "before they are read, a clean page or one discarded into another log is neither read nor looked up");
DEFINE_bool(discard_state_paranoid, false, "bg_page_fixer_tree_lookup: the page state is maintained (and allocated) only to check it "
            "against the parent swip, the parent alone is needed otherwise");
DEFINE_bool(bg_page_fixer_log_io_uring, false, "bg_page_fixer_variant 2: the bg page fixer does not mmap the log of the RU epoch, it reads "
            "the log records of the pages to fix with batched io_uring reads of 4 KiB log blocks");
DEFINE_uint32(lba_chain_buffer_blocks, 1024, "bg_page_fixer_variant 2: 4 KiB blocks of the in memory ring the writers record their LBAs into");
DEFINE_bool(lba_chain_verify, false, "bg_page_fixer_variant 2 with compress_pages: checks at startup and shutdown that every page on the SSD "
            "is listed in the LBA chain of its RU epoch");
DEFINE_uint32(max_log_records_to_discard, 7, "Limit discarding if log records to re-apply to a page exceed this threshold");
DEFINE_double(ru_gc_threshold, 0.8, "Trigger GC of the RU if its usage exceeds this threshold");
DEFINE_bool(steady_gc, true, "Once the bg page fixers kicked in for the first time, the oldest uncollected RU epoch is dispatched to GC "
            "whenever the bg page fixers are done with the previous ones, even if it is below ru_gc_threshold");
DEFINE_uint32(overprovisioning_ru_epochs, 0, "Force GC if the number of active RU epochs is less than max open by this value");
DEFINE_double(overprovisioning_ratio, 0.05, "");
DEFINE_uint32(ru_force_gc_margin, 2, "Start forcing GC this many free RU epochs before entering the over provisioning (rate limited) ones");
DEFINE_uint32(ru_force_gc_batch, 20, "Oldest RU epochs forced to GC at once, the next batch waits until this one is fully reclaimed");
DEFINE_uint32(ru_rate_limit_floor, 10, "Lowest rate (%) of the writes while rate limited, they are stopped only once no free RU epoch is left");
DEFINE_uint32(ru_size, 0, "Unit of garbage collection in database pages (4 KiB)");
DEFINE_bool(per_page_logging, true, "Group log records per page");
DEFINE_uint32(ppl_merge_threshold, 4, "Threshold after which we merge the log records of a page");
DEFINE_bool(opportunistic_log_compaction, true, "Merge consecutive log records that target same record whenever possible");
DEFINE_bool(trace_ru_threshold, true, "Periodically trace the ru threshold of all open RUs");
// -------------------------------------------------------------------------------------
DEFINE_uint64(tx_rate, 0, "");
// -------------------------------------------------------------------------------------
DEFINE_string(redo_log_file, "", ""); // Should provide a log file for now
DEFINE_uint64(log_dev_size_gib, 0, "Size of the log device, 0 value will read actual device size");
DEFINE_bool(log_same_namespace, false, "Place the WAL in a disjoint LBA range at the end of ssd_path instead of a separate device");
DEFINE_double(log_space_pct, 10.0, "Percentage of ssd_path reserved for the WAL when log_same_namespace is set");
DEFINE_bool(log_trim_on_reclaim, true, "Trim (BLKDISCARD) the log segment of an RU epoch once it is reclaimed");
DEFINE_bool(log_fake_wrap_around, false, "Artificially wrap each log segment around at half its size (stands in for the missing checkpointer, vanilla only)");
// -------------------------------------------------------------------------------------
DEFINE_bool(enable_fdp, false, "Support for NVMe Flexible Data Placement");
DEFINE_string(fdp_placement_policy, "Default", "FDP placement policy: Default, FreezeNewlyAllocated, FreezeTables, HardcodedTPCC or FrozenRecords");
DEFINE_bool(fdp_count_gc_events, false, "Count the device GC events (FDP Media Reallocated events), polled once per second by libfdp");
// -------------------------------------------------------------------------------------
DEFINE_bool(compress_pages, false, "Experimental: compress pages on write back and pack them out-of-place into 4 KiB blocks (no crash recovery)");
DEFINE_int32(compression_level, 1, "zstd compression level used with compress_pages");
DEFINE_uint32(compression_max_pages_per_block, 2, "Maximum number of logical pages packed into a 4 KiB block, also the ratio of logical pages to physical blocks (at most CompressedSpace::MAX_PAGES_PER_BLOCK)");
DEFINE_bool(compression_zero_free_space, true, "Zero the free space of a B-Tree node before compressing it");
DEFINE_string(compression_space_map_file, "./compression_space_map.bin", "Where the live bytes of the compressed blocks are persisted to and recovered from");
