/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "global_stats.h"
#include "aggregate/aggregate.h"
#include "util/units.h"

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
    if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_BF) {
      RSGlobalStats.fieldsStats.numVectorFieldsFlat += toAdd;
    } else if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_HNSWLIB) {
        RSGlobalStats.fieldsStats.numVectorFieldsHNSW += toAdd;
    }
  } else if (fs->types & INDEXFLD_T_TAG) {  // tag field
    RSGlobalStats.fieldsStats.numTagFields += toAdd;
    if (fs->tagOpts.tagFlags & TagField_CaseSensitive) {
      RSGlobalStats.fieldsStats.numTagFieldsCaseSensitive += toAdd;
    }
  }

  if (fs->options & FieldSpec_Sortable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalStats.fieldsStats.numTextFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalStats.fieldsStats.numNumericFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalStats.fieldsStats.numGeoFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalStats.fieldsStats.numTagFieldsSortable += toAdd;
  }
  if (fs->options & FieldSpec_NotIndexable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalStats.fieldsStats.numTextFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalStats.fieldsStats.numNumericFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalStats.fieldsStats.numGeoFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalStats.fieldsStats.numTagFieldsNoIndex += toAdd;
  }
}

void FieldsGlobalStats_UpdateIndexError(FieldType field_type, int toAdd) {
  FieldIndexErrorCounter[INDEXTYPE_TO_POS(field_type)] += toAdd;
}

size_t FieldsGlobalStats_GetIndexErrorCount(FieldType field_type) {
  return FieldIndexErrorCounter[INDEXTYPE_TO_POS(field_type)];
}

void TotalGlobalStats_CountQuery(uint32_t reqflags, clock_t duration) {
  if (reqflags & QEXEC_F_INTERNAL) return; // internal queries are not counted

  INCR(RSGlobalStats.totalStats.queries.total_query_commands);
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
  stats.total_query_execution_time = READ(RSGlobalStats.totalStats.queries.total_query_execution_time) / CLOCKS_PER_MILLISEC;
  return stats;
}

void IndexsGlobalStats_UpdateLogicallyDeleted(int64_t toAdd) {
    INCR_BY(RSGlobalStats.totalStats.logically_deleted, toAdd);
}

size_t IndexesGlobalStats_GetLogicallyDeletedDocs() {
  return READ(RSGlobalStats.totalStats.logically_deleted);
}
