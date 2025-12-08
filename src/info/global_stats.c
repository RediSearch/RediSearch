/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "global_stats.h"
#include "aggregate/aggregate.h"
#include "util/units.h"
#include "rs_wall_clock.h"
#include "util/workers.h"
#include "concurrent_ctx.h"

#define INCR_BY(x,y) __atomic_add_fetch(&(x), (y), __ATOMIC_RELAXED)
#define INCR(x) INCR_BY(x, 1)
#define READ(x) __atomic_load_n(&(x), __ATOMIC_RELAXED)

GlobalStats RSGlobalStats = {0};
size_t FieldIndexErrorCounter[INDEXFLD_NUM_TYPES] = {0};

// Assuming that the GIL is already acquired
void FieldsGlobalStats_UpdateStats(FieldSpec *fs, int toAdd) {
  if (fs->types & INDEXFLD_T_FULLTEXT) {  // text field
    RSGlobalStats.fieldsStats.numTextFields += toAdd;
  } else if (fs->types & INDEXFLD_T_NUMERIC) {  // numeric field
    RSGlobalStats.fieldsStats.numNumericFields += toAdd;
  } else if (fs->types & INDEXFLD_T_GEO) {  // geo field
    RSGlobalStats.fieldsStats.numGeoFields += toAdd;
  } else if (fs->types & INDEXFLD_T_VECTOR) {  // vector field
    RSGlobalStats.fieldsStats.numVectorFields += toAdd;
    if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_BF)
      RSGlobalStats.fieldsStats.numVectorFieldsFlat += toAdd;
    else if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_TIERED) {
      if (fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB)
        RSGlobalStats.fieldsStats.numVectorFieldsHNSW += toAdd;
    }
  } else if (fs->types & INDEXFLD_T_TAG) {  // tag field
    RSGlobalStats.fieldsStats.numTagFields += toAdd;
    if (fs->tagOpts.tagFlags & TagField_CaseSensitive) {
      RSGlobalStats.fieldsStats.numTagFieldsCaseSensitive += toAdd;
    }
  } else if (fs->types & INDEXFLD_T_GEOMETRY) {  // geometry field
    RSGlobalStats.fieldsStats.numGeometryFields += toAdd;
  }

  if (fs->options & FieldSpec_Sortable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalStats.fieldsStats.numTextFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalStats.fieldsStats.numNumericFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalStats.fieldsStats.numGeoFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalStats.fieldsStats.numTagFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_GEOMETRY) RSGlobalStats.fieldsStats.numGeometryFieldsSortable += toAdd;
  }
  if (fs->options & FieldSpec_NotIndexable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalStats.fieldsStats.numTextFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalStats.fieldsStats.numNumericFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalStats.fieldsStats.numGeoFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalStats.fieldsStats.numTagFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_GEOMETRY) RSGlobalStats.fieldsStats.numGeometryFieldsNoIndex += toAdd;
  }
}

void FieldsGlobalStats_UpdateIndexError(FieldType field_type, int toAdd) {
  FieldIndexErrorCounter[INDEXTYPE_TO_POS(field_type)] += toAdd;
}

size_t FieldsGlobalStats_GetIndexErrorCount(FieldType field_type) {
  return FieldIndexErrorCounter[INDEXTYPE_TO_POS(field_type)];
}

void TotalGlobalStats_CountQuery(uint32_t reqflags, rs_wall_clock_ns_t duration) {
  if (reqflags & QEXEC_F_INTERNAL) return; // internal queries are not counted

  INCR(RSGlobalStats.totalStats.queries.total_query_commands);

  // Implicit conversion from ns type to ms type, but it is the same type (uint64_t)
  INCR_BY(RSGlobalStats.totalStats.queries.total_query_execution_time, duration);

  if (!(QEXEC_F_IS_CURSOR & reqflags) || (QEXEC_F_IS_AGGREGATE & reqflags)) {
    // Count only unique queries, not iterations of a previous query (FT.CURSOR READ)
    INCR(RSGlobalStats.totalStats.queries.total_queries_processed);
  }
}

QueriesGlobalStats TotalGlobalStats_GetQueryStats() {
  QueriesGlobalStats stats = {0};
  stats.total_queries_processed = READ(RSGlobalStats.totalStats.queries.total_queries_processed);
  stats.total_query_commands = READ(RSGlobalStats.totalStats.queries.total_query_commands);
  stats.total_query_execution_time = rs_wall_clock_convert_ns_to_ms(READ(RSGlobalStats.totalStats.queries.total_query_execution_time));
  return stats;
}

void IndexsGlobalStats_UpdateLogicallyDeleted(int64_t toAdd) {
    INCR_BY(RSGlobalStats.totalStats.logically_deleted, toAdd);
}

size_t IndexesGlobalStats_GetLogicallyDeletedDocs() {
  return READ(RSGlobalStats.totalStats.logically_deleted);
}

// Update the number of active io threads.
void GlobalStats_UpdateActiveIoThreads(int toAdd) {
#ifdef ENABLE_ASSERT
  RS_LOG_ASSERT(toAdd != 0, "Attempt to change active_io_threads by 0");
  size_t current = READ(RSGlobalStats.totalStats.multi_threading.active_io_threads);
  RS_LOG_ASSERT_FMT(toAdd > 0 || current > 0,
    "Cannot decrease active_io_threads below 0. toAdd: %d, current: %zu", toAdd, current);
#endif
  INCR_BY(RSGlobalStats.totalStats.multi_threading.active_io_threads, toAdd);
}

#ifdef RS_COORDINATOR
// Function pointer for coordinator thread count (NULL by default, set by coordinator at init)
size_t (*CoordThreadCount_Func)(void) = NULL;
#endif

// Get multiThreadingStats
MultiThreadingStats GlobalStats_GetMultiThreadingStats() {
  MultiThreadingStats stats = {0};
  stats.active_io_threads = READ(RSGlobalStats.totalStats.multi_threading.active_io_threads);
#ifdef MT_BUILD
  if (RSGlobalConfig.numWorkerThreads) {
    stats.active_worker_threads = workersThreadPool_WorkingThreadCount();
  }
#endif // MT_BUILD

#ifdef RS_COORDINATOR
  if (CoordThreadCount_Func) {
    stats.active_coord_threads = CoordThreadCount_Func();
  }
#endif
  return stats;
}
