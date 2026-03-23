/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "indexes_info.h"
#include "util/dict.h"
#include "spec.h"
#include "field_spec_info.h"
#include "search_disk.h"
#include <string.h>  // Add this for strerror

// Assuming the GIL is held by the caller
TotalIndexesInfo IndexesInfo_TotalInfo() {
  TotalIndexesInfo info = {0};
  info.min_mem = -1;  // Initialize to max value
  // Since we are holding the GIL, we know the BG indexer is not currently running, but it might
  // have been running before we acquired the GIL.
  // We will set this flag to true if we find any index with a scan in progress, and then
  // count it ONCE in the total_active_write_threads. Assumes there is only one BG indexer thread.
  bool BGIndexerInProgress = false;
  // Traverse `specDict_g`, and aggregate indices statistics
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    StrongRef ref = dictGetRef(entry);
    IndexSpec *sp = (IndexSpec *)StrongRef_Get(ref);
    if (!sp) {
      continue;
    }
    // Lock for read
    int rc = pthread_rwlock_rdlock(&sp->rwlock);
    if (rc != 0) {
      RedisModule_Log(RSDummyContext, "warning", "Failed to acquire read lock on index %s: rc=%d (%s). Cannot continue getting Index info", HiddenString_GetUnsafe(sp->specName, NULL), rc, strerror(rc));
      continue;
    }

    // Vector indexes stats
    VectorIndexStats vec_info = IndexSpec_GetVectorIndexesStats(sp);
    info.fields_stats.total_vector_idx_mem += vec_info.memory;
    info.fields_stats.total_mark_deleted_vectors += vec_info.marked_deleted;
    info.fields_stats.total_direct_hnsw_insertions += vec_info.direct_hnsw_insertions;
    info.fields_stats.total_flat_buffer_size += vec_info.flat_buffer_size;

    size_t cur_mem = IndexSpec_TotalMemUsage(sp, 0, 0, 0, vec_info.memory);
    size_t prev_total_mem = info.total_mem;
    info.total_mem += cur_mem;

    info.indexing_time += sp->stats.totalIndexTime;

    if (sp->gc) {
      InfoGCStats gcStats;
      GCContext_GetStats(sp->gc, &gcStats);
      InfoGCStats_Add(&info.gc_stats, &gcStats);
    }

    // Index
    size_t activeQueries = IndexSpec_GetActiveQueries(sp);
    size_t activeWrites = IndexSpec_GetActiveWrites(sp);
    if (activeQueries) info.num_active_indexes_querying++;
    if (activeWrites || sp->scan_in_progress) info.num_active_indexes_indexing++;
    if (activeQueries || activeWrites || sp->scan_in_progress) info.num_active_indexes++;
    info.total_active_queries += activeQueries;
    info.total_active_write_threads += activeWrites;
    BGIndexerInProgress |= sp->scan_in_progress;
    info.total_num_docs_in_indexes += sp->stats.scoring.numDocuments;

    // Index errors metrics
    size_t index_error_count = IndexSpec_GetIndexErrorCount(sp);
    info.indexing_failures += index_error_count;
    if (info.max_indexing_failures < index_error_count) {
      info.max_indexing_failures = index_error_count;
    }
    info.background_indexing_failures_OOM += sp->scan_failed_OOM;

    // Collect disk metrics if disk API is enabled.
    // This stores metrics internally and returns the index's disk memory contribution.
    if (sp->diskSpec) {
      info.total_mem += SearchDisk_CollectIndexMetrics(sp->diskSpec);
    }

    size_t total_index_mem = info.total_mem - prev_total_mem;

    // Update min_mem and max_mem with total memory including disk storage
    if (info.min_mem > total_index_mem) info.min_mem = total_index_mem;
    if (info.max_mem < total_index_mem) info.max_mem = total_index_mem;

    pthread_rwlock_unlock(&sp->rwlock);
  }
  dictReleaseIterator(iter);
  if (info.min_mem == -1) info.min_mem = 0;             // No index found
  if (BGIndexerInProgress) info.total_active_write_threads++;  // BG indexer is currently active
  return info;
}
