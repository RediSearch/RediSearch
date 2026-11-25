/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "field_spec.h"
#include "json.h"

#include <sys/param.h>

#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"
#include "geometry/geometry_api.h"
#include "tag_index.h"
#include "numeric_range_tree_ffi.h"
#include "sparse_vector_index_ffi.h"
#include "nested_index_ffi.h"
#include "info/global_stats.h"
#include "obfuscation/obfuscation_api.h"
#include "search_disk.h"
#include "inverted_index.h"
#include "reply.h"
#include "search_disk_api.h"
#include "spec.h"

void FieldSpec_Cleanup(FieldSpec* fs) {
  if (fs->compiledPath) {
    japi->pathFree(fs->compiledPath);
    fs->compiledPath = NULL;
  }
  // if `AS` was not used, name and path are pointing at the same string
  if (fs->fieldPath && fs->fieldName != fs->fieldPath) {
    HiddenString_Free(fs->fieldPath, true);
  }
  fs->fieldPath = NULL;
  if (fs->fieldName) {
    HiddenString_Free(fs->fieldName, true);
    fs->fieldName = NULL;
  }

  if (FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
    VecSimParams_Cleanup(&fs->vectorOpts.vecSimParams);
    if (fs->vectorOpts.vecSimIndex) {
      // diskCtx.indexName is set for every disk-backed field (when its
      // storage is opened, or earlier by FieldSpec_RdbLoad) and stays
      // set even before storage is bound. Use it as the disk-vs-RAM
      // discriminator so an empty-but-unbound vector index loaded from RDB
      // is still torn down via the disk path.
      if (fs->vectorOpts.diskCtx.indexName) {
        SearchDisk_FreeVectorIndex(fs->vectorOpts.vecSimIndex);
      } else {
        VecSimIndex_Free(fs->vectorOpts.vecSimIndex);
      }
      fs->vectorOpts.vecSimIndex = NULL;
    }
    if (fs->vectorOpts.diskCtx.indexName) {
      rm_free((void *)fs->vectorOpts.diskCtx.indexName);
      fs->vectorOpts.diskCtx.indexName = NULL;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_GEOMETRY)) {
    if (fs->geometryOpts.geometryIndex) {
      GeometryApi_Get(fs->geometryOpts.geometryIndex)->freeIndex(fs->geometryOpts.geometryIndex);
      fs->geometryOpts.geometryIndex = NULL;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
    if (fs->tagOpts.tagIndex) {
      TagIndex_Free(fs->tagOpts.tagIndex);
      fs->tagOpts.tagIndex = NULL;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO)) {
    if (fs->tree) {
      NumericRangeTree_Free(fs->tree);
      fs->tree = NULL;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_SPARSE_VECTOR)) {
    SparseVectorIndex_Free(fs->sparseVecOpts.sparseVecIndex);
    fs->sparseVecOpts.sparseVecIndex = NULL;
  }
  if (FIELD_IS(fs, INDEXFLD_T_NESTED)) {
    NestedIndex_Free(fs->nestedOpts.nestedIndex);
    fs->nestedOpts.nestedIndex = NULL;
    NestedDef *def = fs->nestedOpts.def;
    for (uint8_t i = 0; def && i < def->numSubFields; i++) {
      NestedSubField *sub = &def->subFields[i];
      if (sub->compiledPath) {
        japi->pathFree(sub->compiledPath);
      }
      rm_free(sub->name);
      rm_free(sub->path);
    }
    rm_free(def);
    fs->nestedOpts.def = NULL;
  }
}

void FieldSpec_InitNested(FieldSpec *fs) {
  // Field creation writes TAG defaults into the shared union; overwrite them before anything
  // can fail and reach FieldSpec_Cleanup.
  fs->nestedOpts.def = rm_calloc(1, sizeof(*fs->nestedOpts.def));
  fs->nestedOpts.nestedIndex = NULL;
  fs->types |= INDEXFLD_T_NESTED;
}

bool FieldSpec_BuildNestedIndex(FieldSpec *fs, char *err, size_t errLen) {
  RS_ASSERT(FIELD_IS(fs, INDEXFLD_T_NESTED));
  RS_ASSERT(fs->nestedOpts.def->numLevels >= 1);
  // Every level but the last routes, on a TAG sub-field; the last is the terminal.
  uint8_t numRouting = fs->nestedOpts.def->numLevels - 1;
  size_t maxCardinality[NESTED_MAX_SUBFIELDS] = {0};
  for (uint8_t i = 0; i < numRouting; i++) {
    const NestedSubField *sub = &fs->nestedOpts.def->subFields[fs->nestedOpts.def->levels[i]];
    RS_ASSERT(sub->type == INDEXFLD_T_TAG);
    maxCardinality[i] = sub->maxCardinality;
  }
  const NestedSubField *terminal =
      &fs->nestedOpts.def->subFields[fs->nestedOpts.def->levels[fs->nestedOpts.def->numLevels - 1]];
  NestedTerminalKind kind =
      terminal->type == INDEXFLD_T_TAG ? NestedTerminalKind_Tag : NestedTerminalKind_Numeric;
  struct NestedIndex *index = NestedIndex_New(maxCardinality, numRouting, kind,
                                              RSGlobalConfig.numericCompress, err, errLen);
  if (!index) {
    return false;
  }
  NestedIndex_Free(fs->nestedOpts.nestedIndex);
  fs->nestedOpts.nestedIndex = index;
  return true;
}

bool FieldSpec_CompileNestedPaths(FieldSpec *fs) {
  RS_ASSERT(FIELD_IS(fs, INDEXFLD_T_NESTED));
  for (uint8_t i = 0; i < fs->nestedOpts.def->numSubFields; i++) {
    NestedSubField *sub = &fs->nestedOpts.def->subFields[i];
    if (!sub->compiledPath) {
      sub->compiledPath = japi->pathParseNoErrorMsg(sub->path);
      if (!sub->compiledPath) {
        return false;
      }
    }
  }
  return true;
}

struct SparseVectorIndex *FieldSpec_OpenSparseVectorIndex(FieldSpec *fs, bool create_if_missing) {
  RS_ASSERT(FIELD_IS(fs, INDEXFLD_T_SPARSE_VECTOR));
  if (!fs->sparseVecOpts.sparseVecIndex && create_if_missing) {
    fs->sparseVecOpts.sparseVecIndex =
        SparseVectorIndex_New(fs->sparseVecOpts.dim, fs->sparseVecOpts.metric);
  }
  return fs->sparseVecOpts.sparseVecIndex;
}

void FieldSpec_SetSortable(FieldSpec* fs) {
  RS_LOG_ASSERT(!(fs->options & FieldSpec_Dynamic), "dynamic fields cannot be sortable");
  fs->options |= FieldSpec_Sortable;
}

const char *FieldSpec_GetTypeNames(int idx) {
  switch (idx) {
  case IXFLDPOS_FULLTEXT:      return SPEC_TEXT_STR;
  case IXFLDPOS_TAG:           return SPEC_TAG_STR;
  case IXFLDPOS_NUMERIC:       return SPEC_NUMERIC_STR;
  case IXFLDPOS_GEO:           return SPEC_GEO_STR;
  case IXFLDPOS_VECTOR:        return SPEC_VECTOR_STR;
  case IXFLDPOS_GEOMETRY:      return SPEC_GEOMETRY_STR;
  case IXFLDPOS_SPARSE_VECTOR: return SPEC_SPARSE_VECTOR_STR;
  case IXFLDPOS_NESTED:
    return SPEC_NESTED_STR;

  default:
    RS_ABORT_ALWAYS("oops");
  }
}

/**
 * True iff `changedFields` names the document field `fs` is fed from
 * */
bool FieldSpec_IsInChangeSet(const FieldSpec *fs, RedisModuleString **changedFields,
                             size_t numChangedFields) {
  if (!changedFields) {
    return false;
  }
  for (size_t i = 0; i < numChangedFields; ++i) {
    size_t length = 0;
    const char *field = RedisModule_StringPtrLen(changedFields[i], &length);
    if (FieldSpec_PathEquals(fs, field, length)) {
      return true;
    }
  }
  return false;
}

void FieldSpec_AddError(FieldSpec *fs, ConstErrorMessage withoutUserData, ConstErrorMessage withUserData, RedisModuleString *key) {
  IndexError_AddError(&fs->indexError, withoutUserData, withUserData, key);
  FieldsGlobalStats_UpdateIndexError(fs->types, 1);
}

size_t FieldSpec_GetIndexErrorCount(const FieldSpec *fs) {
  return IndexError_ErrorCount(&fs->indexError);
}

static char *FormatFieldNameOrPath(t_uniqueId fieldId, HiddenString* name, void (*callback)(t_uniqueId, char*), bool obfuscate) {
  char obfuscated[MAX(MAX_OBFUSCATED_FIELD_NAME, MAX_OBFUSCATED_PATH_NAME)];
  const char* value = obfuscated;
  if (obfuscate) {
    callback(fieldId, obfuscated);
  } else {
    value = HiddenString_GetUnsafe(name, NULL);
  }
  if (isUnsafeForSimpleString(value)) {
    return escapeSimpleString(value);
  } else {
    return rm_strdup(value);
  }
}

char *FieldSpec_FormatName(const FieldSpec *fs, bool obfuscate) {
  return FormatFieldNameOrPath(fs->index, fs->fieldName, Obfuscate_Field, obfuscate);
}

char *FieldSpec_FormatPath(const FieldSpec *fs, bool obfuscate) {
  return FormatFieldNameOrPath(fs->index, fs->fieldPath, Obfuscate_FieldPath, obfuscate);
}
