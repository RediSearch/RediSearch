/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "global_stats.h"
#include "aggregate/aggregate.h"
#include "util/units.h"
#include "rs_wall_clock.h"
#include "util/workers.h"
#include "concurrent_ctx.h"

#define INCR_BY(x,y) __atomic_add_fetch(&(x), (y), __ATOMIC_RELAXED)
#define DECR_BY(x,y) __atomic_sub_fetch(&(x), (y), __ATOMIC_RELAXED)
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
      if (fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_SVS) {
        RSGlobalStats.fieldsStats.numVectorFieldsSvsVamana += toAdd;
        if (fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.svsParams.quantBits)
          RSGlobalStats.fieldsStats.numVectorFieldsSvsVamanaCompressed += toAdd;
      }
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

void TotalGlobalStats_AddCoordDispatchTime(rs_wall_clock_ns_t duration) {
  INCR_BY(RSGlobalStats.totalStats.queries.total_coord_dispatch_time, duration);
}

QueriesGlobalStats TotalGlobalStats_GetQueryStats() {
  QueriesGlobalStats stats = {0};
  stats.total_queries_processed = READ(RSGlobalStats.totalStats.queries.total_queries_processed);
  stats.total_query_commands = READ(RSGlobalStats.totalStats.queries.total_query_commands);
  stats.total_query_execution_time = rs_wall_clock_convert_ns_to_ms(READ(RSGlobalStats.totalStats.queries.total_query_execution_time));
  stats.total_coord_dispatch_time = READ(RSGlobalStats.totalStats.queries.total_coord_dispatch_time);
  // Errors
  stats.shard_errors.syntax = READ(RSGlobalStats.totalStats.queries.shard_errors.syntax);
  stats.shard_errors.arguments = READ(RSGlobalStats.totalStats.queries.shard_errors.arguments);
  stats.shard_errors.timeout = READ(RSGlobalStats.totalStats.queries.shard_errors.timeout);
  stats.coord_errors.syntax = READ(RSGlobalStats.totalStats.queries.coord_errors.syntax);
  stats.coord_errors.arguments = READ(RSGlobalStats.totalStats.queries.coord_errors.arguments);
  stats.coord_errors.timeout = READ(RSGlobalStats.totalStats.queries.coord_errors.timeout);
  stats.shard_errors.oom = READ(RSGlobalStats.totalStats.queries.shard_errors.oom);
  stats.coord_errors.oom = READ(RSGlobalStats.totalStats.queries.coord_errors.oom);
  stats.shard_errors.unavailableSlots = READ(RSGlobalStats.totalStats.queries.shard_errors.unavailableSlots);
  stats.coord_errors.unavailableSlots = READ(RSGlobalStats.totalStats.queries.coord_errors.unavailableSlots);

  // Warnings
  stats.shard_warnings.timeout = READ(RSGlobalStats.totalStats.queries.shard_warnings.timeout);
  stats.coord_warnings.timeout = READ(RSGlobalStats.totalStats.queries.coord_warnings.timeout);
  stats.shard_warnings.oom = READ(RSGlobalStats.totalStats.queries.shard_warnings.oom);
  stats.coord_warnings.oom = READ(RSGlobalStats.totalStats.queries.coord_warnings.oom);
  stats.shard_warnings.maxPrefixExpansion = READ(RSGlobalStats.totalStats.queries.shard_warnings.maxPrefixExpansion);
  stats.coord_warnings.maxPrefixExpansion = READ(RSGlobalStats.totalStats.queries.coord_warnings.maxPrefixExpansion);
  stats.shard_warnings.asm_inaccuracy = READ(RSGlobalStats.totalStats.queries.shard_warnings.asm_inaccuracy);
  stats.coord_warnings.asm_inaccuracy = READ(RSGlobalStats.totalStats.queries.coord_warnings.asm_inaccuracy);
  return stats;
}

void IndexsGlobalStats_IncreaseLogicallyDeleted(int64_t toAdd) {
  INCR_BY(RSGlobalStats.totalStats.logically_deleted, toAdd);
}

void IndexsGlobalStats_DecreaseLogicallyDeleted(int64_t toSubtract) {
  DECR_BY(RSGlobalStats.totalStats.logically_deleted, toSubtract);
}

size_t IndexesGlobalStats_GetLogicallyDeletedDocs() {
  return READ(RSGlobalStats.totalStats.logically_deleted);
}

// Updates the global query errors statistics.
// `coord` indicates whether the error occurred on the coordinator or on a shard.
// Standalone shards are considered as coords
// Will ignore not supported error codes.
// Currently supports : syntax, parse_args, timeout
// `toAdd` can be negative to decrease the counter.
void QueryErrorsGlobalStats_UpdateError(QueryErrorCode code, int toAdd, bool coord) {
  QueryErrorsGlobalStats *queries_errors = coord ? &RSGlobalStats.totalStats.queries.coord_errors : &RSGlobalStats.totalStats.queries.shard_errors;
  switch (code) {
    case QUERY_ERROR_CODE_SYNTAX:
      INCR_BY(queries_errors->syntax, toAdd);
      break;
    case QUERY_ERROR_CODE_PARSE_ARGS:
    case QUERY_ERROR_CODE_ARG_UNRECOGNIZED:
      INCR_BY(queries_errors->arguments, toAdd);
      break;
    case QUERY_ERROR_CODE_TIMED_OUT:
      INCR_BY(queries_errors->timeout, toAdd);
      break;
    case QUERY_ERROR_CODE_OUT_OF_MEMORY:
      INCR_BY(queries_errors->oom, toAdd);
      break;
    case QUERY_ERROR_CODE_UNAVAILABLE_SLOTS:
      INCR_BY(queries_errors->unavailableSlots, toAdd);
      break;
  }
}

// Updates the global query warnings statistics.
// `coord` indicates whether the warning occurred on the coordinator or on a shard.
// Standalone shards are considered as coords
// Will ignore not supported warning codes.
// Currently supports : timeout
// `toAdd` can be negative to decrease the counter.
void QueryWarningsGlobalStats_UpdateWarning(QueryWarningCode code, int toAdd, bool coord) {
  QueryWarningGlobalStats *queries_warnings = coord ? &RSGlobalStats.totalStats.queries.coord_warnings : &RSGlobalStats.totalStats.queries.shard_warnings;
  switch (code) {
    case QUERY_WARNING_CODE_TIMED_OUT:
      INCR_BY(queries_warnings->timeout, toAdd);
      break;
    case QUERY_WARNING_CODE_OUT_OF_MEMORY_SHARD:
      INCR_BY(queries_warnings->oom, toAdd);
      break;
    case QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD:
      INCR_BY(queries_warnings->oom, toAdd);
      break;
    case QUERY_WARNING_CODE_REACHED_MAX_PREFIX_EXPANSIONS:
      INCR_BY(queries_warnings->maxPrefixExpansion, toAdd);
      break;
    case QUERY_WARNING_CODE_ASM_INACCURATE_RESULTS:
      INCR_BY(queries_warnings->asm_inaccuracy, toAdd);
      break;
  }
}

// Update the number of active io threads.
void GlobalStats_UpdateUvRunningQueries(int toAdd) {
#ifdef ENABLE_ASSERT
  RS_LOG_ASSERT(toAdd != 0, "Attempt to change uv_threads_running_queries by 0");
  size_t current = READ(RSGlobalStats.totalStats.multi_threading.uv_threads_running_queries);
  RS_LOG_ASSERT_FMT(toAdd > 0 || current > 0,
    "Cannot decrease uv_threads_running_queries below 0. toAdd: %d, current: %zu", toAdd, current);
#endif
  INCR_BY(RSGlobalStats.totalStats.multi_threading.uv_threads_running_queries, toAdd);
}

void GlobalStats_UpdateUvRunningTopoUpdate(int toAdd) {
#ifdef ENABLE_ASSERT
  RS_LOG_ASSERT(toAdd != 0, "Attempt to change uv_threads_running_topology_update by 0");
  size_t current = READ(RSGlobalStats.totalStats.multi_threading.uv_threads_running_topology_update);
  RS_LOG_ASSERT_FMT(toAdd > 0 || current > 0,
    "Cannot decrease uv_threads_running_topology_update below 0. toAdd: %d, current: %zu", toAdd, current);
#endif
  INCR_BY(RSGlobalStats.totalStats.multi_threading.uv_threads_running_topology_update, toAdd);
}

// Get multiThreadingStats
MultiThreadingStats GlobalStats_GetMultiThreadingStats() {
  MultiThreadingStats stats;
  stats.uv_threads_running_queries = READ(RSGlobalStats.totalStats.multi_threading.uv_threads_running_queries);
  stats.uv_threads_running_topology_update = READ(RSGlobalStats.totalStats.multi_threading.uv_threads_running_topology_update);

  // Workers stats
  // We don't use workersThreadPool_getStats here to avoid the overhead of locking the thread pool.
  stats.active_worker_threads = workersThreadPool_WorkingThreadCount();
  stats.workers_low_priority_pending_jobs = workersThreadPool_LowPriorityPendingJobsCount();
  stats.workers_high_priority_pending_jobs = workersThreadPool_HighPriorityPendingJobsCount();
  stats.workers_admin_priority_pending_jobs = workersThreadPool_AdminPriorityPendingJobsCount();

  // Coordinator stats
  stats.active_coord_threads = ConcurrentSearchPool_WorkingThreadCount();
  stats.coord_high_priority_pending_jobs = ConcurrentSearchPool_HighPriorityPendingJobsCount();
  return stats;
}

void FieldsGlobalStats_UpdateFieldDocsIndexed(FieldType field_types, int toAdd) {
  // Indexing documents happens only in the main thread or with the GIL locked.
  // Therefore, there is no need for atomic operations.
  switch (field_types) {
    case INDEXFLD_T_FULLTEXT:
      RSGlobalStats.fieldsStats.textTotalDocsIndexed += toAdd;
      break;
    case INDEXFLD_T_NUMERIC:
      RSGlobalStats.fieldsStats.numericTotalDocsIndexed += toAdd;
      break;
    case INDEXFLD_T_GEO:
      RSGlobalStats.fieldsStats.geoTotalDocsIndexed += toAdd;
      break;
    case INDEXFLD_T_TAG:
      RSGlobalStats.fieldsStats.tagTotalDocsIndexed += toAdd;
      break;
    case INDEXFLD_T_VECTOR:
      RSGlobalStats.fieldsStats.vectorTotalDocsIndexed += toAdd;
      break;
    case INDEXFLD_T_GEOMETRY:
      RSGlobalStats.fieldsStats.geometryTotalDocsIndexed += toAdd;
      break;
  }
}
