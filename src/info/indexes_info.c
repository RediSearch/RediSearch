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

    size_t cur_mem = IndexSpec_TotalMemUsage(sp, 0, 0, 0, vec_info.memory);
    info.total_mem += cur_mem;

    if (info.min_mem > cur_mem) info.min_mem = cur_mem;
    if (info.max_mem < cur_mem) info.max_mem = cur_mem;
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
        // Memtable metrics
        info.disk_doc_table.num_immutable_memtables += doc_table_metrics.num_immutable_memtables;
        info.disk_doc_table.num_immutable_memtables_flushed += doc_table_metrics.num_immutable_memtables_flushed;
        info.disk_doc_table.mem_table_flush_pending += doc_table_metrics.mem_table_flush_pending;
        info.disk_doc_table.active_memtable_size += doc_table_metrics.active_memtable_size;
        info.disk_doc_table.all_memtables_size += doc_table_metrics.all_memtables_size;
        info.disk_doc_table.size_all_mem_tables += doc_table_metrics.size_all_mem_tables;
        info.disk_doc_table.num_entries_active_memtable += doc_table_metrics.num_entries_active_memtable;
        info.disk_doc_table.num_entries_imm_memtables += doc_table_metrics.num_entries_imm_memtables;
        info.disk_doc_table.num_deletes_active_memtable += doc_table_metrics.num_deletes_active_memtable;
        info.disk_doc_table.num_deletes_imm_memtables += doc_table_metrics.num_deletes_imm_memtables;

        // Compaction metrics
        info.disk_doc_table.compaction_pending += doc_table_metrics.compaction_pending;
        info.disk_doc_table.num_running_compactions += doc_table_metrics.num_running_compactions;
        info.disk_doc_table.num_running_flushes += doc_table_metrics.num_running_flushes;
        info.disk_doc_table.estimate_pending_compaction_bytes += doc_table_metrics.estimate_pending_compaction_bytes;

        // Data size estimates
        info.disk_doc_table.estimate_num_keys += doc_table_metrics.estimate_num_keys;
        info.disk_doc_table.estimate_live_data_size += doc_table_metrics.estimate_live_data_size;
        info.disk_doc_table.live_sst_files_size += doc_table_metrics.live_sst_files_size;

        // Version tracking
        info.disk_doc_table.num_live_versions += doc_table_metrics.num_live_versions;

        // Memory usage
        info.disk_doc_table.estimate_table_readers_mem += doc_table_metrics.estimate_table_readers_mem;
      }

      DiskColumnFamilyMetrics inverted_index_metrics = {0};
      if (SearchDisk_CollectTextInvertedIndexMetrics(sp->diskSpec, &inverted_index_metrics)) {
        // Memtable metrics
        info.disk_inverted_index.num_immutable_memtables += inverted_index_metrics.num_immutable_memtables;
        info.disk_inverted_index.num_immutable_memtables_flushed += inverted_index_metrics.num_immutable_memtables_flushed;
        info.disk_inverted_index.mem_table_flush_pending += inverted_index_metrics.mem_table_flush_pending;
        info.disk_inverted_index.active_memtable_size += inverted_index_metrics.active_memtable_size;
        info.disk_inverted_index.all_memtables_size += inverted_index_metrics.all_memtables_size;
        info.disk_inverted_index.size_all_mem_tables += inverted_index_metrics.size_all_mem_tables;
        info.disk_inverted_index.num_entries_active_memtable += inverted_index_metrics.num_entries_active_memtable;
        info.disk_inverted_index.num_entries_imm_memtables += inverted_index_metrics.num_entries_imm_memtables;
        info.disk_inverted_index.num_deletes_active_memtable += inverted_index_metrics.num_deletes_active_memtable;
        info.disk_inverted_index.num_deletes_imm_memtables += inverted_index_metrics.num_deletes_imm_memtables;

        // Compaction metrics
        info.disk_inverted_index.compaction_pending += inverted_index_metrics.compaction_pending;
        info.disk_inverted_index.num_running_compactions += inverted_index_metrics.num_running_compactions;
        info.disk_inverted_index.num_running_flushes += inverted_index_metrics.num_running_flushes;
        info.disk_inverted_index.estimate_pending_compaction_bytes += inverted_index_metrics.estimate_pending_compaction_bytes;

        // Data size estimates
        info.disk_inverted_index.estimate_num_keys += inverted_index_metrics.estimate_num_keys;
        info.disk_inverted_index.estimate_live_data_size += inverted_index_metrics.estimate_live_data_size;
        info.disk_inverted_index.live_sst_files_size += inverted_index_metrics.live_sst_files_size;

        // Version tracking
        info.disk_inverted_index.num_live_versions += inverted_index_metrics.num_live_versions;

        // Memory usage
        info.disk_inverted_index.estimate_table_readers_mem += inverted_index_metrics.estimate_table_readers_mem;
      }
    }

    pthread_rwlock_unlock(&sp->rwlock);
  }
  dictReleaseIterator(iter);
  if (info.min_mem == -1) info.min_mem = 0;             // No index found
  if (BGIndexerInProgress) info.total_active_write_threads++;  // BG indexer is currently active
  return info;
}
