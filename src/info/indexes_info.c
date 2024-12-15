/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "indexes_info.h"
#include "util/dict.h"
#include "spec.h"

TotalIndexesInfo IndexesInfo_TotalInfo() {
  TotalIndexesInfo info = {0};
  info.min_mem = -1;  // Initialize to max value
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
    pthread_rwlock_rdlock(&sp->rwlock);
    size_t cur_mem = IndexSpec_TotalMemUsage(sp, 0, 0, 0);
    info.total_mem += cur_mem;
    if (info.min_mem > cur_mem) info.min_mem = cur_mem;
    if (info.max_mem < cur_mem) info.max_mem = cur_mem;
    info.indexing_time += sp->stats.totalIndexTime;

    // Vector index stats
    VectorIndexStats vec_info = IndexSpec_GetVectorIndexStats(sp);
    info.fields_stats.total_vector_idx_mem += vec_info.memory;
    info.fields_stats.total_mark_deleted_vectors += vec_info.marked_deleted;

    if (sp->gc) {
      ForkGCStats gcStats = ((ForkGC *)sp->gc->gcCtx)->stats;
      info.gc_stats.totalCollectedBytes += gcStats.totalCollected;
      info.gc_stats.totalCycles += gcStats.numCycles;
      info.gc_stats.totalTime += gcStats.totalMSRun;
    }

    // Index
    size_t activeQueries = IndexSpec_GetActiveQueries(sp);
    size_t activeWrites = IndexSpec_GetActiveWrites(sp);
    if (activeQueries) info.num_active_indexes_querying++;
    if (activeWrites) info.num_active_indexes_indexing++;
    if (activeQueries || activeWrites) info.num_active_indexes++;
    info.total_active_queries += activeQueries;
    info.total_active_writes += activeWrites;

    // Index errors metrics
    size_t index_error_count = IndexSpec_GetIndexErrorCount(sp);
    info.indexing_failures += index_error_count;
    if (info.max_indexing_failures < index_error_count) {
      info.max_indexing_failures = index_error_count;
    }

    pthread_rwlock_unlock(&sp->rwlock);
  }
  dictReleaseIterator(iter);
  if (info.min_mem == -1) info.min_mem = 0;  // No index found
  return info;
}
