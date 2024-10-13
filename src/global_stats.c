/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "global_stats.h"
#include "aggregate/aggregate.h"

#define INCR(x) __atomic_add_fetch(&(x), 1, __ATOMIC_RELAXED)
#define READ(x) __atomic_load_n(&(x), __ATOMIC_RELAXED)

GlobalStats RSGlobalStats = {0};

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

// Assuming that the GIL is already acquired
void FieldsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "fields_statistics");

  if (RSGlobalStats.fieldsStats.numTextFields > 0){
    RedisModule_InfoBeginDictField(ctx, "fields_text");
    RedisModule_InfoAddFieldLongLong(ctx, "Text", RSGlobalStats.fieldsStats.numTextFields);
    if (RSGlobalStats.fieldsStats.numTextFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalStats.fieldsStats.numTextFieldsSortable);
    if (RSGlobalStats.fieldsStats.numTextFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalStats.fieldsStats.numTextFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numNumericFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_numeric");
    RedisModule_InfoAddFieldLongLong(ctx, "Numeric", RSGlobalStats.fieldsStats.numNumericFields);
    if (RSGlobalStats.fieldsStats.numNumericFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalStats.fieldsStats.numNumericFieldsSortable);
    if (RSGlobalStats.fieldsStats.numNumericFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalStats.fieldsStats.numNumericFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numTagFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_tag");
    RedisModule_InfoAddFieldLongLong(ctx, "Tag", RSGlobalStats.fieldsStats.numTagFields);
    if (RSGlobalStats.fieldsStats.numTagFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalStats.fieldsStats.numTagFieldsSortable);
    if (RSGlobalStats.fieldsStats.numTagFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalStats.fieldsStats.numTagFieldsNoIndex);
    if (RSGlobalStats.fieldsStats.numTagFieldsCaseSensitive > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "CaseSensitive", RSGlobalStats.fieldsStats.numTagFieldsCaseSensitive);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numGeoFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geo");
    RedisModule_InfoAddFieldLongLong(ctx, "Geo", RSGlobalStats.fieldsStats.numGeoFields);
    if (RSGlobalStats.fieldsStats.numGeoFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalStats.fieldsStats.numGeoFieldsSortable);
    if (RSGlobalStats.fieldsStats.numGeoFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalStats.fieldsStats.numGeoFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numVectorFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_vector");
    RedisModule_InfoAddFieldLongLong(ctx, "Vector", RSGlobalStats.fieldsStats.numVectorFields);
    if (RSGlobalStats.fieldsStats.numVectorFieldsFlat > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Flat", RSGlobalStats.fieldsStats.numVectorFieldsFlat);
    if (RSGlobalStats.fieldsStats.numVectorFieldsHNSW > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "HNSW", RSGlobalStats.fieldsStats.numVectorFieldsHNSW);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalStats.fieldsStats.numGeometryFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geoshape");
    RedisModule_InfoAddFieldLongLong(ctx, "Geoshape", RSGlobalStats.fieldsStats.numGeometryFields);
    if (RSGlobalStats.fieldsStats.numGeometryFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalStats.fieldsStats.numGeometryFieldsSortable);
    if (RSGlobalStats.fieldsStats.numGeometryFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalStats.fieldsStats.numGeometryFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }
}

void TotalGlobalStats_CountQuery(uint32_t reqflags) {
  if (reqflags & QEXEC_F_INTERNAL) return; // internal queries are not counted

  INCR(RSGlobalStats.totalStats.total_query_commands);

  if (!(QEXEC_F_IS_CURSOR & reqflags) || (QEXEC_F_IS_AGGREGATE & reqflags)) {
    // Count only unique queries, not iterations of a previous query (FT.CURSOR READ)
    INCR(RSGlobalStats.totalStats.total_queries_processed);
  }
}

void TotalGlobalStats_Queries_AddToInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "queries");
  RedisModule_InfoAddFieldLongLong(ctx, "total_queries_processed", READ(RSGlobalStats.totalStats.total_queries_processed));
  RedisModule_InfoAddFieldLongLong(ctx, "total_query_commands", READ(RSGlobalStats.totalStats.total_query_commands));
}

void DialectsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "dialect_statistics");
  for (int dialect = MIN_DIALECT_VERSION; dialect <= MAX_DIALECT_VERSION; ++dialect) {
    char field[16] = {0};
    snprintf(field, sizeof field, "dialect_%d", dialect);
    // extract the d'th bit of the dialects bitfield.
    RedisModule_InfoAddFieldULongLong(ctx, field, GET_DIALECT(RSGlobalStats.totalStats.used_dialects, dialect));
  }
}
