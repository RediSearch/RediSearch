/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once
#include <stddef.h>
#include "fork_gc.h"
#include "rs_wall_clock.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  // Vector Indexing
  size_t total_vector_idx_mem;        // Total memory used by the vector index
  size_t total_mark_deleted_vectors;  // Number of vectors marked as deleted
} TotalIndexesFieldsInfo;

typedef struct {
  // Memtable metrics
  uint64_t num_immutable_memtables;
  uint64_t num_immutable_memtables_flushed;
  uint64_t mem_table_flush_pending;
  uint64_t active_memtable_size;
  uint64_t all_memtables_size;
  uint64_t size_all_mem_tables;
  uint64_t num_entries_active_memtable;
  uint64_t num_entries_imm_memtables;
  uint64_t num_deletes_active_memtable;
  uint64_t num_deletes_imm_memtables;

  // Compaction metrics
  uint64_t compaction_pending;
  uint64_t num_running_compactions;
  uint64_t num_running_flushes;
  uint64_t estimate_pending_compaction_bytes;

  // Data size estimates
  uint64_t estimate_num_keys;
  uint64_t estimate_live_data_size;
  uint64_t live_sst_files_size;

  // Level information
  uint64_t base_level;

  // Write control
  uint64_t actual_delayed_write_rate;
  uint64_t is_write_stopped;

  // Version tracking
  uint64_t num_live_versions;
  uint64_t current_super_version_number;

  // Snapshot info
  uint64_t oldest_snapshot_time;
  uint64_t oldest_snapshot_sequence;

  // Memory usage
  uint64_t estimate_table_readers_mem;
} TotalDiskColumnFamilyMetrics;

typedef struct {
  // Memory
  size_t total_mem;  // Total memory used by the indexes
  size_t min_mem;    // Memory used by the smallest (local) index
  size_t max_mem;    // Memory used by the largest (local) index

  // Indexing
  rs_wall_clock_ns_t indexing_time;  // Time spent on indexing

  // GC
  InfoGCStats gc_stats;  // Garbage collection statistics

  // Field stats
  TotalIndexesFieldsInfo fields_stats;  // Aggregated Fields statistics

  // Indexing Errors
  size_t indexing_failures;      // Total count of indexing errors
  size_t max_indexing_failures;  // Maximum number of indexing errors among all specs
  size_t background_indexing_failures_OOM;  // Total count of background indexing errors due to OOM
  // Index
  size_t num_active_indexes;           // Number of active indexes
  size_t num_active_indexes_querying;  // Number of active read indexes
  size_t num_active_indexes_indexing;  // Number of active write indexes
  size_t total_active_write_threads;   // Total number of active writes (proportional to the number
  // of threads)
  size_t total_num_docs_in_indexes;      // Total number of documents in all indexes
  size_t total_active_queries;         // Total number of active queries (reads)

  // Disk metrics
  TotalDiskColumnFamilyMetrics disk_doc_table;      // Aggregated doc_table metrics
  TotalDiskColumnFamilyMetrics disk_inverted_index; // Aggregated inverted_index metrics
} TotalIndexesInfo;

// Returns an aggregated statistics of all the currently existing indexes
TotalIndexesInfo IndexesInfo_TotalInfo();

#ifdef __cplusplus
}
#endif
