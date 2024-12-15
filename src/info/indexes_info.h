/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <stddef.h>
#include "fork_gc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  // Vector Indexing
  size_t total_vector_idx_mem;        // Total memory used by the vector index
  size_t total_mark_deleted_vectors;  // Number of vectors marked as deleted
} TotalIndexesFieldsInfo;

typedef struct {
  // Memory
  size_t total_mem;  // Total memory used by the indexes
  size_t min_mem;    // Memory used by the smallest (local) index
  size_t max_mem;    // Memory used by the largest (local) index

  // Indexing
  size_t indexing_time;  // Time spent on indexing

  // GC
  InfoGCStats gc_stats;  // Garbage collection statistics

  TotalIndexesFieldsInfo fields_stats;  // Aggregated Fields statistics

  // Indexing Errors
  size_t indexing_failures;      // Total count of indexing errors
  size_t max_indexing_failures;  // Maximum number of indexing errors among all specs

  // Index
  size_t num_active_indexes;           // Number of active indexes
  size_t num_active_indexes_querying;  // Number of active read indexes
  size_t num_active_indexes_indexing;  // Number of active write indexes
  size_t total_active_writes;          // Total number of active writes (proportional to the number of threads)
  size_t total_active_queries;         // Total number of active queries (reads)
} TotalIndexesInfo;

// Retrunes an aggregated statistics of all the currently existing indexes
TotalIndexesInfo IndexesInfo_TotalInfo();

#ifdef __cplusplus
}
#endif
