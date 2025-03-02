/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "info_redis.h"
#include "version.h"
#include "global_stats.h"
#include "cursor.h"
#include "indexes_info.h"
#include "util/units.h"
#include "active_queries/active_queries.h"
#include "active_queries/thread_info.h"

/* ========================== PROTOTYPES ============================ */
// Fields statistics
static inline void AddToInfo_Fields(RedisModuleInfoCtx *ctx, TotalIndexesFieldsInfo *aggregatedFieldsStats);

// General sections info
static inline void AddToInfo_Indexes(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info);
static inline void AddToInfo_Memory(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info);
static inline void AddToInfo_Cursors(RedisModuleInfoCtx *ctx);
static inline void AddToInfo_GC(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info);
static inline void AddToInfo_Queries(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info);
static inline void AddToInfo_ErrorsAndWarnings(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info);
static inline void AddToInfo_Dialects(RedisModuleInfoCtx *ctx);
static inline void AddToInfo_RSConfig(RedisModuleInfoCtx *ctx);
static inline void AddToInfo_ActiveQueries(RedisModuleInfoCtx *ctx);
static inline void AddToInfo_CurrentThread(RedisModuleInfoCtx *ctx);
/* ========================== MAIN FUNC ============================ */

void RS_moduleInfoFunc(RedisModuleInfoCtx *ctx, int for_crash_report) {
  // Module version
  RedisModule_InfoAddSection(ctx, "version");
  char ver[64];
  // RediSearch version
  sprintf(ver, "%d.%d.%d", REDISEARCH_VERSION_MAJOR, REDISEARCH_VERSION_MINOR,
          REDISEARCH_VERSION_PATCH);
  RedisModule_InfoAddFieldCString(ctx, "version", ver);
  // Redis version
  GetFormattedRedisVersion(ver, sizeof(ver));
  RedisModule_InfoAddFieldCString(ctx, "redis_version", ver);
  // Redis Enterprise version
  if (IsEnterprise()) {
    GetFormattedRedisEnterpriseVersion(ver, sizeof(ver));
    RedisModule_InfoAddFieldCString(ctx, "redis_enterprise_version", ver);
  }

  TotalIndexesInfo total_info = IndexesInfo_TotalInfo();

  // Indexes related statistics
  AddToInfo_Indexes(ctx, &total_info);

  // Fields statistics
  AddToInfo_Fields(ctx, &total_info.fields_stats);

  // Memory
  AddToInfo_Memory(ctx, &total_info);

  // Cursors
  AddToInfo_Cursors(ctx);

  // GC stats
  AddToInfo_GC(ctx, &total_info);

  // Query statistics
  AddToInfo_Queries(ctx, &total_info);

  // Errors statistics
  AddToInfo_ErrorsAndWarnings(ctx, &total_info);

  // Dialect statistics
  AddToInfo_Dialects(ctx);

  // Run time configuration
  AddToInfo_RSConfig(ctx);

  // Active threads
  if (for_crash_report) {
    AddToInfo_CurrentThread(ctx);
    AddToInfo_ActiveQueries(ctx);
  }
}

/* ========================== IMP ============================ */

// Assuming that the GIL is already acquired
void AddToInfo_Fields(RedisModuleInfoCtx *ctx, TotalIndexesFieldsInfo *aggregatedFieldsStats) {

  RedisModule_InfoAddSection(ctx, "fields_statistics");

  if (RSGlobalStats.fieldsStats.numTextFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_text");
    RedisModule_InfoAddFieldLongLong(ctx, "Text", RSGlobalStats.fieldsStats.numTextFields);
    if (RSGlobalStats.fieldsStats.numTextFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable",
                                       RSGlobalStats.fieldsStats.numTextFieldsSortable);
    if (RSGlobalStats.fieldsStats.numTextFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex",
                                       RSGlobalStats.fieldsStats.numTextFieldsNoIndex);
    RedisModule_InfoAddFieldLongLong(ctx, "IndexErrors",
                                     FieldsGlobalStats_GetIndexErrorCount(INDEXFLD_T_FULLTEXT));
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numNumericFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_numeric");
    RedisModule_InfoAddFieldLongLong(ctx, "Numeric", RSGlobalStats.fieldsStats.numNumericFields);
    if (RSGlobalStats.fieldsStats.numNumericFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable",
                                       RSGlobalStats.fieldsStats.numNumericFieldsSortable);
    if (RSGlobalStats.fieldsStats.numNumericFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex",
                                       RSGlobalStats.fieldsStats.numNumericFieldsNoIndex);
    RedisModule_InfoAddFieldLongLong(ctx, "IndexErrors",
                                     FieldsGlobalStats_GetIndexErrorCount(INDEXFLD_T_NUMERIC));
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numTagFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_tag");
    RedisModule_InfoAddFieldLongLong(ctx, "Tag", RSGlobalStats.fieldsStats.numTagFields);
    if (RSGlobalStats.fieldsStats.numTagFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable",
                                       RSGlobalStats.fieldsStats.numTagFieldsSortable);
    if (RSGlobalStats.fieldsStats.numTagFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex",
                                       RSGlobalStats.fieldsStats.numTagFieldsNoIndex);
    if (RSGlobalStats.fieldsStats.numTagFieldsCaseSensitive > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "CaseSensitive",
                                       RSGlobalStats.fieldsStats.numTagFieldsCaseSensitive);
    RedisModule_InfoAddFieldLongLong(ctx, "IndexErrors",
                                     FieldsGlobalStats_GetIndexErrorCount(INDEXFLD_T_TAG));
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numGeoFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geo");
    RedisModule_InfoAddFieldLongLong(ctx, "Geo", RSGlobalStats.fieldsStats.numGeoFields);
    if (RSGlobalStats.fieldsStats.numGeoFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable",
                                       RSGlobalStats.fieldsStats.numGeoFieldsSortable);
    if (RSGlobalStats.fieldsStats.numGeoFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex",
                                       RSGlobalStats.fieldsStats.numGeoFieldsNoIndex);
    RedisModule_InfoAddFieldLongLong(ctx, "IndexErrors",
                                     FieldsGlobalStats_GetIndexErrorCount(INDEXFLD_T_GEO));
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numVectorFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_vector");
    RedisModule_InfoAddFieldLongLong(ctx, "Vector", RSGlobalStats.fieldsStats.numVectorFields);
    if (RSGlobalStats.fieldsStats.numVectorFieldsFlat > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Flat", RSGlobalStats.fieldsStats.numVectorFieldsFlat);
    if (RSGlobalStats.fieldsStats.numVectorFieldsHNSW > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "HNSW", RSGlobalStats.fieldsStats.numVectorFieldsHNSW);
    RedisModule_InfoAddFieldLongLong(ctx, "IndexErrors",
                                     FieldsGlobalStats_GetIndexErrorCount(INDEXFLD_T_VECTOR));
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numGeometryFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geoshape");
    RedisModule_InfoAddFieldLongLong(ctx, "Geoshape", RSGlobalStats.fieldsStats.numGeometryFields);
    if (RSGlobalStats.fieldsStats.numGeometryFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable",
                                       RSGlobalStats.fieldsStats.numGeometryFieldsSortable);
    if (RSGlobalStats.fieldsStats.numGeometryFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex",
                                       RSGlobalStats.fieldsStats.numGeometryFieldsNoIndex);
    RedisModule_InfoAddFieldLongLong(ctx, "IndexErrors",
                                     FieldsGlobalStats_GetIndexErrorCount(INDEXFLD_T_GEOMETRY));
    RedisModule_InfoEndDictField(ctx);
  }
}

void AddToInfo_Indexes(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info) {
  RedisModule_InfoAddSection(ctx, "indexes");
  RedisModule_InfoAddFieldULongLong(ctx, "number_of_indexes", dictSize(specDict_g));
  RedisModule_InfoAddFieldULongLong(ctx, "number_of_active_indexes", total_info->num_active_indexes);
  RedisModule_InfoAddFieldULongLong(ctx, "number_of_active_indexes_running_queries", total_info->num_active_indexes_querying);
  RedisModule_InfoAddFieldULongLong(ctx, "number_of_active_indexes_indexing", total_info->num_active_indexes_indexing);
  RedisModule_InfoAddFieldULongLong(ctx, "total_active_write_threads", total_info->total_active_write_threads);
  RedisModule_InfoAddFieldDouble(ctx, "total_indexing_time", (float)total_info->indexing_time / (float)CLOCKS_PER_MILLISEC);
}

void AddToInfo_Memory(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info) {
  RedisModule_InfoAddSection(ctx, "memory");

	// Total
  RedisModule_InfoAddFieldDouble(ctx, "used_memory_indexes", total_info->total_mem);
  RedisModule_InfoAddFieldDouble(ctx, "used_memory_indexes_human", MEMORY_MB(total_info->total_mem));
	// Min
  RedisModule_InfoAddFieldDouble(ctx, "smallest_memory_index", total_info->min_mem);
  RedisModule_InfoAddFieldDouble(ctx, "smallest_memory_index_human", MEMORY_MB(total_info->min_mem));
	// Max
  RedisModule_InfoAddFieldDouble(ctx, "largest_memory_index", total_info->max_mem);
  RedisModule_InfoAddFieldDouble(ctx, "largest_memory_index_human", MEMORY_MB(total_info->max_mem));

	// Vector memory
  RedisModule_InfoAddFieldDouble(ctx, "used_memory_vector_index", total_info->fields_stats.total_vector_idx_mem);
}

void AddToInfo_Cursors(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "cursors");
  CursorsInfoStats cursorsStats = Cursors_GetInfoStats();
  RedisModule_InfoAddFieldLongLong(ctx, "global_idle_user", cursorsStats.total_idle_user);
  RedisModule_InfoAddFieldLongLong(ctx, "global_idle_internal", cursorsStats.total_idle_internal);
  RedisModule_InfoAddFieldLongLong(ctx, "global_total_user", cursorsStats.total_user);
  RedisModule_InfoAddFieldLongLong(ctx, "global_total_internal", cursorsStats.total_internal);
}

void AddToInfo_GC(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info) {
  RedisModule_InfoAddSection(ctx, "garbage collector");
  InfoGCStats stats = total_info->gc_stats;
  RedisModule_InfoAddFieldDouble(ctx, "gc_bytes_collected", stats.totalCollectedBytes);
  RedisModule_InfoAddFieldDouble(ctx, "gc_total_cycles", stats.totalCycles);
  RedisModule_InfoAddFieldDouble(ctx, "gc_total_ms_run", stats.totalTime);
  RedisModule_InfoAddFieldULongLong(ctx, "gc_total_docs_not_collected", IndexesGlobalStats_GetLogicallyDeletedDocs());
  RedisModule_InfoAddFieldULongLong(ctx, "gc_marked_deleted_vectors", total_info->fields_stats.total_mark_deleted_vectors);
}

void AddToInfo_Queries(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info) {
  RedisModule_InfoAddSection(ctx, "queries");
  QueriesGlobalStats stats = TotalGlobalStats_GetQueryStats();
  RedisModule_InfoAddFieldULongLong(ctx, "total_queries_processed", stats.total_queries_processed);
  RedisModule_InfoAddFieldULongLong(ctx, "total_query_commands", stats.total_query_commands);
  RedisModule_InfoAddFieldULongLong(ctx, "total_query_execution_time_ms", stats.total_query_execution_time);
  RedisModule_InfoAddFieldULongLong(ctx, "total_active_queries", total_info->total_active_queries);
}

void AddToInfo_ErrorsAndWarnings(RedisModuleInfoCtx *ctx, TotalIndexesInfo *total_info) {
  RedisModule_InfoAddSection(ctx, "warnings_and_errors");
  RedisModule_InfoAddFieldDouble(ctx, "errors_indexing_failures", total_info->indexing_failures);
  // highest number of failures out of all specs
  RedisModule_InfoAddFieldDouble(ctx, "errors_for_index_with_max_failures", total_info->max_indexing_failures);
}

void AddToInfo_Dialects(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "dialect_statistics");
  for (int dialect = MIN_DIALECT_VERSION; dialect <= MAX_DIALECT_VERSION; ++dialect) {
    char field[16] = {0};
    snprintf(field, sizeof field, "dialect_%d", dialect);
    // extract the d'th bit of the dialects bitfield.
    RedisModule_InfoAddFieldULongLong(ctx, field, GET_DIALECT(RSGlobalStats.totalStats.used_dialects, dialect));
  }
}

void AddToInfo_RSConfig(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "runtime_configurations");

  if (RSGlobalConfig.extLoad != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "extension_load", (char *)RSGlobalConfig.extLoad);
  }
  if (RSGlobalConfig.frisoIni != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "friso_ini", (char *)RSGlobalConfig.frisoIni);
  }
  RedisModule_InfoAddFieldCString(ctx, "enableGC",
                                  RSGlobalConfig.gcConfigParams.enableGC ? "ON" : "OFF");
  RedisModule_InfoAddFieldLongLong(ctx, "minimal_term_prefix",
                                   RSGlobalConfig.iteratorsConfigParams.minTermPrefix);
  RedisModule_InfoAddFieldLongLong(ctx, "minimal_stem_length",
                                   RSGlobalConfig.iteratorsConfigParams.minStemLength);
  RedisModule_InfoAddFieldLongLong(ctx, "maximal_prefix_expansions",
                                   RSGlobalConfig.iteratorsConfigParams.maxPrefixExpansions);
  RedisModule_InfoAddFieldLongLong(ctx, "query_timeout_ms",
                                   RSGlobalConfig.requestConfigParams.queryTimeoutMS);
  RedisModule_InfoAddFieldCString(ctx, "timeout_policy",
																	(char *)TimeoutPolicy_ToString(RSGlobalConfig.requestConfigParams.timeoutPolicy));
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_read_size", RSGlobalConfig.cursorReadSize);
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_max_idle_time", RSGlobalConfig.cursorMaxIdle);

  RedisModule_InfoAddFieldLongLong(ctx, "max_doc_table_size", RSGlobalConfig.maxDocTableSize);
  RedisModule_InfoAddFieldLongLong(ctx, "max_search_results", RSGlobalConfig.maxSearchResults);
  RedisModule_InfoAddFieldLongLong(ctx, "max_aggregate_results",
                                   RSGlobalConfig.maxAggregateResults);
  RedisModule_InfoAddFieldLongLong(ctx, "gc_scan_size", RSGlobalConfig.gcConfigParams.gcScanSize);
  RedisModule_InfoAddFieldLongLong(ctx, "min_phonetic_term_length",
                                   RSGlobalConfig.minPhoneticTermLen);
}

void AddToInfo_CurrentThread(RedisModuleInfoCtx *ctx) {
  ThreadInfo *currentThread = CurrentThread_GetInfo();
  if (!currentThread) {
    return;
  }
  RedisModule_InfoAddSection(ctx, "current_thread");
  StrongRef strong = WeakRef_Promote(currentThread->specRef);
  IndexSpec *spec = StrongRef_Get(strong);
  if (spec) {
    RedisModule_InfoAddFieldCString(ctx, "index", spec->name);
  } else {
    RedisModule_InfoAddFieldCString(ctx, "index", "n/a");
  }
}

void AddToInfo_ActiveQueries(RedisModuleInfoCtx *ctx) {
  ActiveQueries *activeQueries = GetActiveQueries();
  // If we are not the main thread then do not output the current queries
  if (!activeQueries) {
    return;
  }
  RedisModule_InfoAddSection(ctx, "active_queries");
  // Assumes no other thread is currently accessing the active-threads container
  DLLIST_FOREACH(node, &(activeQueries->queries)) {
    ActiveQueryNode *at = DLLIST_ITEM(node, ActiveQueryNode, llnode);
    IndexSpec *spec = StrongRef_Get(at->spec);
    RedisModule_InfoAddFieldULongLong(ctx, (const char *)spec->name, (unsigned long long)at->start);
  }
  RedisModule_InfoAddSection(ctx, "active_cursors");
  // Assumes no other thread is currently accessing the active-threads container
  DLLIST_FOREACH(node, &(activeQueries->cursors)) {
    ActiveCursorNode *at = DLLIST_ITEM(node, ActiveCursorNode, llnode);
    char buffer[21]; // max uint64_t text length is 20 in base 10, so 21 bytes should be enough
    sprintf(buffer, "%lu", at->cursorId);
    RedisModule_InfoAddFieldULongLong(ctx, buffer, (unsigned long long)at->start);
  }
}
