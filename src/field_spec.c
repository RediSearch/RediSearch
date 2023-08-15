/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "field_spec.h"
#include "rdb.h"
#include "indexer.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"

RSValueType fieldTypeToValueType(FieldType ft) {
  switch (ft) {
    case INDEXFLD_T_NUMERIC:
      return RSValue_Number;

    case INDEXFLD_T_FULLTEXT:
    case INDEXFLD_T_TAG:
    case INDEXFLD_T_GEO:
      return RSValue_String;

    case INDEXFLD_T_VECTOR: // TODO:
    case INDEXFLD_T_GEOMETRY: // TODO: GEOMETRY
      return RSValue_Null;
  }
  return RSValue_Null;
}

void FieldSpec_Cleanup(FieldSpec* fs) {
  // if `AS` was not used, name and path are pointing at the same string
  if (fs->path && fs->name != fs->path) {
    rm_free(fs->path);
  }
  fs->path = NULL;
  if (fs->name) {
    rm_free(fs->name);
    fs->name = NULL;
  }

  if (fs->types & INDEXFLD_T_VECTOR) {
    VecSimParams_Cleanup(&fs->vectorOpts.vecSimParams);
  }

  // Free delimiter list
  if (fs->delimiters != NULL) {
    DelimiterList_Unref(fs->delimiters);
    fs->delimiters = NULL;
  }
}

void FieldSpec_SetSortable(FieldSpec* fs) {
  RS_LOG_ASSERT(!(fs->options & FieldSpec_Dynamic), "dynamic fields cannot be sortable");
  fs->options |= FieldSpec_Sortable;
}

const char *FieldSpec_GetTypeNames(int idx) {
  switch (idx) {
  case IXFLDPOS_FULLTEXT: return SPEC_TEXT_STR;
  case IXFLDPOS_TAG:      return SPEC_TAG_STR;
  case IXFLDPOS_NUMERIC:  return SPEC_NUMERIC_STR;
  case IXFLDPOS_GEO:      return SPEC_GEO_STR;
  case IXFLDPOS_VECTOR:   return SPEC_VECTOR_STR;
  case IXFLDPOS_GEOMETRY: return SPEC_GEOMETRY_STR;

  default:
    RS_LOG_ASSERT(0, "oops");
    break;
  }
}

// given a field mask with one bit lit, it returns its offset
static int bit(t_fieldMask id) {
  for (int i = 0; i < sizeof(t_fieldMask) * 8; i++) {
    if (((id >> i) & 1) == 1) {
      return i;
    }
  }
  return 0;
}

// Backwards compat version of load for rdbs with version < 8
int FieldSpec_RdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver) {
  LoadStringBufferAlloc_IOErrors(rdb, f->name, NULL, goto fail);

  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->ftId = bit(LoadUnsigned_IOError(rdb, goto fail));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
  }
  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  f->tagOpts.tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagOpts.tagSep = TAG_FIELD_DEFAULT_HASH_SEP;
  if (encver >= 4) {
    f->options = LoadUnsigned_IOError(rdb, goto fail);
    f->sortIdx = LoadSigned_IOError(rdb, goto fail);
  }
  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name) + 1);
  if (f->path != f->name) {
    RedisModule_SaveUnsigned(rdb, 1);
    RedisModule_SaveStringBuffer(rdb, f->path, strlen(f->path) + 1);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  RedisModule_SaveUnsigned(rdb, f->types);
  RedisModule_SaveUnsigned(rdb, f->options);
  RedisModule_SaveSigned(rdb, f->sortIdx);
  // Save text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->ftId);
    RedisModule_SaveDouble(rdb, f->ftWeight);
    
    if (FieldSpec_HasCustomDelimiters(f)){
      DelimiterList_RdbSave(rdb, f->delimiters);
    }
  }
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->tagOpts.tagFlags);
    RedisModule_SaveStringBuffer(rdb, &f->tagOpts.tagSep, 1);
  }
  if (FIELD_IS(f, INDEXFLD_T_VECTOR)) {
    RedisModule_SaveUnsigned(rdb, f->vectorOpts.expBlobSize);
    VecSim_RdbSave(rdb, &f->vectorOpts.vecSimParams);
  }
  if (FIELD_IS(f, INDEXFLD_T_GEOMETRY) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->geometryOpts.geometryCoords);
  }
}

static const FieldType fieldTypeMap[] = {[IDXFLD_LEGACY_FULLTEXT] = INDEXFLD_T_FULLTEXT,
                                         [IDXFLD_LEGACY_NUMERIC] = INDEXFLD_T_NUMERIC,
                                         [IDXFLD_LEGACY_GEO] = INDEXFLD_T_GEO,
                                         [IDXFLD_LEGACY_TAG] = INDEXFLD_T_TAG};
                                         // CHECKED: Not related to new data types - legacy code

int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, StrongRef sp_ref, int encver) {

  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  LoadStringBufferAlloc_IOErrors(rdb, f->name, NULL, goto fail);
  f->path = f->name;
  if (encver >= INDEX_JSON_VERSION) {
    if (LoadUnsigned_IOError(rdb, goto fail) == 1) {
      LoadStringBufferAlloc_IOErrors(rdb, f->path, NULL,goto fail);
    }
  }

  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->options = LoadUnsigned_IOError(rdb, goto fail);
  f->sortIdx = LoadSigned_IOError(rdb, goto fail);

  if (encver < INDEX_MIN_MULTITYPE_VERSION) {
    RS_LOG_ASSERT(f->types <= IDXFLD_LEGACY_MAX, "field type should be string or numeric");
    f->types = fieldTypeMap[f->types];
  }

  // Load text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
    f->ftWeight = LoadDouble_IOError(rdb, goto fail);

    if (encver >= INDEX_DELIMITERS_VERSION) {
      if (FieldSpec_HasCustomDelimiters(f)) {
        f->delimiters = DelimiterList_RdbLoad(rdb);
        if (f->delimiters == NULL) {
          goto fail;
        }
      } else {
        f->delimiters = DefaultDelimiterList();
      }
    }
  }
  // Load tag specific options
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagOpts.tagFlags = LoadUnsigned_IOError(rdb, goto fail);
    // Load the separator
    size_t l;
    char *s = LoadStringBuffer_IOError(rdb, &l, goto fail);
    RS_LOG_ASSERT(l == 1, "buffer length should be 1");
    f->tagOpts.tagSep = *s;
    RedisModule_Free(s);
  }
  // Load vector specific options
  if (encver >= INDEX_VECSIM_VERSION && FIELD_IS(f, INDEXFLD_T_VECTOR)) {
    if (encver >= INDEX_VECSIM_2_VERSION) {
      f->vectorOpts.expBlobSize = LoadUnsigned_IOError(rdb, goto fail);
    }
    if (encver >= INDEX_VECSIM_TIERED_VERSION) {
      if (VecSim_RdbLoad_v3(rdb, &f->vectorOpts.vecSimParams, sp_ref, f->name) != REDISMODULE_OK) {
        goto fail;
      }
    } else {
      if (encver >= INDEX_VECSIM_MULTI_VERSION) {
        if (VecSim_RdbLoad_v2(rdb, &f->vectorOpts.vecSimParams) != REDISMODULE_OK) {
          goto fail;
        }
      } else {
        if (VecSim_RdbLoad(rdb, &f->vectorOpts.vecSimParams) != REDISMODULE_OK) {
          goto fail;
        }
      }
      // If we're loading an old (< 2.8) rdb, we need to convert an HNSW index to a tiered index
      VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
      logCtx->index_field_name = f->name;
      f->vectorOpts.vecSimParams.logCtx = logCtx;
      if (f->vectorOpts.vecSimParams.algo == VecSimAlgo_HNSWLIB) {
        VecSimParams hnswParams = f->vectorOpts.vecSimParams;

        f->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
        VecSim_TieredParams_Init(&f->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);
        f->vectorOpts.vecSimParams.algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = 0;
        memcpy(f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams, &hnswParams, sizeof(VecSimParams));
      }
    }
    // Calculate blob size limitation on lower encvers.
    if(encver < INDEX_VECSIM_2_VERSION) {
      switch (f->vectorOpts.vecSimParams.algo) {
      case VecSimAlgo_HNSWLIB:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.hnswParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.hnswParams.type);
        break;
      case VecSimAlgo_BF:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.bfParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.bfParams.type);
        break;
      case VecSimAlgo_TIERED:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams.type);
        break;
      }
    }
  }

  // Load geometry specific options
  if (FIELD_IS(f, INDEXFLD_T_GEOMETRY) || (f->options & FieldSpec_Dynamic)) {
    if (encver >= INDEX_GEOMETRY_VERSION) {
      f->geometryOpts.geometryCoords = LoadUnsigned_IOError(rdb, goto fail);
    } else {
      // In RedisSearch RC (2.8.1 - 2.8.3) we supported default coordinate system which was not written to RDB
      f->geometryOpts.geometryCoords = GEOMETRY_COORDS_Cartesian;
    }
  }

  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}
