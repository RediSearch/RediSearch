/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "fields_global_stats.h"
#include "config.h"

// TODO: multithreaded: additions should be atomic
void FieldsGlobalStats_UpdateStats(FieldSpec *fs, int toAdd) {
  if (fs->types & INDEXFLD_T_FULLTEXT) {  // text field
    RSGlobalConfig.fieldsStats.numTextFields += toAdd;
  } else if (fs->types & INDEXFLD_T_NUMERIC) {  // numeric field
    RSGlobalConfig.fieldsStats.numNumericFields += toAdd;
  } else if (fs->types & INDEXFLD_T_GEO) {  // geo field
    RSGlobalConfig.fieldsStats.numGeoFields += toAdd;
  } else if (fs->types & INDEXFLD_T_VECTOR) {  // vector field
    RSGlobalConfig.fieldsStats.numVectorFields += toAdd;
    if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_BF)
      RSGlobalConfig.fieldsStats.numVectorFieldsFlat += toAdd;
    else if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_TIERED) {
      if (fs->vectorOpts.vecSimParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB)
        RSGlobalConfig.fieldsStats.numVectorFieldsHNSW += toAdd;
    }
  } else if (fs->types & INDEXFLD_T_TAG) {  // tag field
    RSGlobalConfig.fieldsStats.numTagFields += toAdd;
    if (fs->tagOpts.tagFlags & TagField_CaseSensitive) {
      RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive += toAdd;
    }
  } else if (fs->types & INDEXFLD_T_GEOMETRY) {  // geometry field
    RSGlobalConfig.fieldsStats.numGeometryFields += toAdd;
  } 

  if (fs->options & FieldSpec_Sortable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalConfig.fieldsStats.numTextFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalConfig.fieldsStats.numNumericFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalConfig.fieldsStats.numGeoFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalConfig.fieldsStats.numTagFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_GEOMETRY) RSGlobalConfig.fieldsStats.numGeometryFieldsSortable += toAdd;
  }
  if (fs->options & FieldSpec_NotIndexable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalConfig.fieldsStats.numTextFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalConfig.fieldsStats.numTagFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_GEOMETRY) RSGlobalConfig.fieldsStats.numGeometryFieldsNoIndex += toAdd;
  }
}

void FieldsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "fields_statistics");

  if (RSGlobalConfig.fieldsStats.numTextFields > 0){
    RedisModule_InfoBeginDictField(ctx, "fields_text");
    RedisModule_InfoAddFieldLongLong(ctx, "Text", RSGlobalConfig.fieldsStats.numTextFields);
    if (RSGlobalConfig.fieldsStats.numTextFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numTextFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numTextFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numTextFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numNumericFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_numeric");
    RedisModule_InfoAddFieldLongLong(ctx, "Numeric", RSGlobalConfig.fieldsStats.numNumericFields);
    if (RSGlobalConfig.fieldsStats.numNumericFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numNumericFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numTagFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_tag");
    RedisModule_InfoAddFieldLongLong(ctx, "Tag", RSGlobalConfig.fieldsStats.numTagFields);
    if (RSGlobalConfig.fieldsStats.numTagFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numTagFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numTagFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numTagFieldsNoIndex);
    if (RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "CaseSensitive", RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numGeoFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geo");
    RedisModule_InfoAddFieldLongLong(ctx, "Geo", RSGlobalConfig.fieldsStats.numGeoFields);
    if (RSGlobalConfig.fieldsStats.numGeoFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numGeoFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numVectorFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_vector");
    RedisModule_InfoAddFieldLongLong(ctx, "Vector", RSGlobalConfig.fieldsStats.numVectorFields);
    if (RSGlobalConfig.fieldsStats.numVectorFieldsFlat > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Flat", RSGlobalConfig.fieldsStats.numVectorFieldsFlat);
    if (RSGlobalConfig.fieldsStats.numVectorFieldsHNSW > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "HNSW", RSGlobalConfig.fieldsStats.numVectorFieldsHNSW);
    RedisModule_InfoEndDictField(ctx);
  }

  if (RSGlobalConfig.fieldsStats.numGeometryFields > 0) {
    RedisModule_InfoBeginDictField(ctx, "fields_geometry");
    RedisModule_InfoAddFieldLongLong(ctx, "Geometry", RSGlobalConfig.fieldsStats.numGeometryFields);
    if (RSGlobalConfig.fieldsStats.numGeometryFieldsSortable > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "Sortable", RSGlobalConfig.fieldsStats.numGeometryFieldsSortable);
    if (RSGlobalConfig.fieldsStats.numGeometryFieldsNoIndex > 0)
      RedisModule_InfoAddFieldLongLong(ctx, "NoIndex", RSGlobalConfig.fieldsStats.numGeometryFieldsNoIndex);
    RedisModule_InfoEndDictField(ctx);
  }
}
