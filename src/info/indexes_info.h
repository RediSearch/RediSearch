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
#include "gc.h"
#include "rs_wall_clock.h"
#include "search_disk_api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  // Vector Indexing
  size_t total_vector_idx_mem;            // Total memory used by the vector index
  size_t total_mark_deleted_vectors;      // Number of vectors marked as deleted
  size_t total_direct_hnsw_insertions;    // Total vectors inserted directly to HNSW (bypassing flat buffer)
  size_t total_flat_buffer_size;          // Total flat buffer size across all tiered indexes
} TotalIndexesFieldsInfo;

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
} TotalIndexesInfo;

// Returns an aggregated statistics of all the currently existing indexes
TotalIndexesInfo IndexesInfo_TotalInfo();

#ifdef __cplusplus
}
#endif
