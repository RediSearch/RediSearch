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

/**
 * @brief Accumulates disk column family metrics from source to destination
 *
 * @param dest Destination metrics structure to accumulate into
 * @param src Source metrics structure to accumulate from
 */
static void AccumulateDiskMetrics(DiskColumnFamilyMetrics *dest, const DiskColumnFamilyMetrics *src) {
  // Memtable metrics
  dest->num_immutable_memtables += src->num_immutable_memtables;
  dest->num_immutable_memtables_flushed += src->num_immutable_memtables_flushed;
  dest->mem_table_flush_pending += src->mem_table_flush_pending;
  dest->active_memtable_size += src->active_memtable_size;
  dest->size_all_mem_tables += src->size_all_mem_tables;
  dest->num_entries_active_memtable += src->num_entries_active_memtable;
  dest->num_entries_imm_memtables += src->num_entries_imm_memtables;
  dest->num_deletes_active_memtable += src->num_deletes_active_memtable;
  dest->num_deletes_imm_memtables += src->num_deletes_imm_memtables;

  // Compaction metrics
  dest->compaction_pending += src->compaction_pending;
  dest->num_running_compactions += src->num_running_compactions;
  dest->num_running_flushes += src->num_running_flushes;
  dest->estimate_pending_compaction_bytes += src->estimate_pending_compaction_bytes;

  // Data size estimates
  dest->estimate_num_keys += src->estimate_num_keys;
  dest->estimate_live_data_size += src->estimate_live_data_size;
  dest->live_sst_files_size += src->live_sst_files_size;

  // Version tracking
  dest->num_live_versions += src->num_live_versions;

  // Memory usage
  dest->estimate_table_readers_mem += src->estimate_table_readers_mem;
}

// Returns the total memory used by the disk components of the index.
// We currently take into account:
//  1. The mem-tables' size
//  2. The estimate of the tables' readers memory
//  3. SST files' size.
// TODO: Add memory used for the deleted-ids set (relevant for doc-table only).
static inline size_t updateTotalMemFromDiskMetrics(TotalIndexesInfo *info, DiskColumnFamilyMetrics *diskMetrics) {
  return diskMetrics->size_all_mem_tables +
         diskMetrics->estimate_table_readers_mem +
         diskMetrics->live_sst_files_size;
}

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

    size_t cur_mem = IndexSpec_TotalMemUsage(sp, 0, 0, 0, vec_info.memory);
    size_t prev_total_mem = info.total_mem;
    info.total_mem += cur_mem;

    info.indexing_time += sp->stats.totalIndexTime;

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
    if (activeWrites || sp->scan_in_progress) info.num_active_indexes_indexing++;
    if (activeQueries || activeWrites || sp->scan_in_progress) info.num_active_indexes++;
    info.total_active_queries += activeQueries;
    info.total_active_write_threads += activeWrites;
    BGIndexerInProgress |= sp->scan_in_progress;
    info.total_num_docs_in_indexes += sp->stats.numDocuments;

    // Index errors metrics
    size_t index_error_count = IndexSpec_GetIndexErrorCount(sp);
    info.indexing_failures += index_error_count;
    if (info.max_indexing_failures < index_error_count) {
      info.max_indexing_failures = index_error_count;
    }
    info.background_indexing_failures_OOM += sp->scan_failed_OOM;

    // Collect disk metrics if disk API is enabled (otherwise all are `0`s).
    if (sp->diskSpec) {
      DiskColumnFamilyMetrics doc_table_metrics = {0};
      if (SearchDisk_CollectDocTableMetrics(sp->diskSpec, &doc_table_metrics)) {
        AccumulateDiskMetrics(&info.disk_doc_table, &doc_table_metrics);
        // Update info.total_mem with disk related objects.
        info.total_mem += updateTotalMemFromDiskMetrics(&info, &doc_table_metrics);
      } else {
        RedisModule_Log(RSDummyContext, "warning", "Could not collect disk related info for index %s", HiddenString_GetUnsafe(sp->specName, NULL));
      }

      DiskColumnFamilyMetrics inverted_index_metrics = {0};
      if (SearchDisk_CollectTextInvertedIndexMetrics(sp->diskSpec, &inverted_index_metrics)) {
        AccumulateDiskMetrics(&info.disk_inverted_index, &inverted_index_metrics);
        // Update info.total_mem with disk related objects.
        info.total_mem += updateTotalMemFromDiskMetrics(&info, &inverted_index_metrics);
      } else {
        RedisModule_Log(RSDummyContext, "warning", "Could not collect disk related info for index %s", HiddenString_GetUnsafe(sp->specName, NULL));
      }
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
