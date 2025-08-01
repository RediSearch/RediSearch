/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "field_spec.h"
#include "indexer.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"
#include "info/global_stats.h"
#include "obfuscation/obfuscation_api.h"
#include "spec.h"
#include "tag_index.h"
#include "numeric_index.h"
#include "module.h"  // For RSDummyContext, RedisModule_Log, REDISMODULE_OK/ERR
#include "config.h"
#include "redis_index.h"  // For DONT_CREATE_INDEX constant

void FieldSpec_Cleanup(FieldSpec* fs) {
  // if `AS` was not used, name and path are pointing at the same string
  if (fs->fieldPath && fs->fieldName != fs->fieldPath) {
    HiddenString_Free(fs->fieldPath, true);
  }
  fs->fieldPath = NULL;
  if (fs->fieldName) {
    HiddenString_Free(fs->fieldName, true);
    fs->fieldName = NULL;
  }

  if (fs->types & INDEXFLD_T_VECTOR) {
    VecSimParams_Cleanup(&fs->vectorOpts.vecSimParams);
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
    RS_ABORT_ALWAYS("oops");
  }
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

//---------------------------------------------------------------------------------------------
// FieldSpec Replication Functions
//---------------------------------------------------------------------------------------------

// Field iteration is now handled within IndexSpec-level functions

int FieldSpec_PrepareForFork(FieldSpec *field, IndexSpec *parent_spec) {
  if (!field || !parent_spec) {
    return REDISMODULE_ERR;
  }

  RedisModule_Log(RSDummyContext, "debug",
                "RediSearch: Preparing field '%s' for fork",
                FieldSpec_FormatName(field, RSGlobalConfig.hideUserDataFromLog));

  // Handle different field types
  if (FIELD_IS(field, INDEXFLD_T_NUMERIC)) {
    // Get the numeric range tree for this field
    RedisModuleString *keyName = IndexSpec_GetFormattedKey(parent_spec, field, INDEXFLD_T_NUMERIC);
    NumericRangeTree *tree = openNumericKeysDict(parent_spec, keyName, DONT_CREATE_INDEX);
    if (tree) {
      return NumericRangeTree_PrepareForFork(tree);
    }
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_TAG)) {
    // Get the tag index for this field
    RedisModuleString *keyName = TagIndex_FormatName(parent_spec, field->fieldName);
    TagIndex *tagIndex = TagIndex_Open(parent_spec, keyName, DONT_CREATE_INDEX);
    RedisModule_FreeString(RSDummyContext, keyName);
    if (tagIndex) {
      return TagIndex_PrepareForFork(tagIndex);
    }
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_VECTOR)) {
    // For vector fields, we need to access the VecSim index through the spec
    // For now, just return OK as vector index handling is more complex
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_FULLTEXT)) {
    // Text fields are handled at the IndexSpec level via TextIndex_PrepareForFork
    return REDISMODULE_OK;
  }

  // Default case for other field types
  return REDISMODULE_OK;
}

int FieldSpec_OnForkCreated(FieldSpec *field, IndexSpec *parent_spec) {
  if (!field || !parent_spec) {
    return REDISMODULE_ERR;
  }

  // Handle different field types
  if (FIELD_IS(field, INDEXFLD_T_NUMERIC)) {
    RedisModuleString *keyName = IndexSpec_GetFormattedKey(parent_spec, field, INDEXFLD_T_NUMERIC);
    NumericRangeTree *tree = openNumericKeysDict(parent_spec, keyName, DONT_CREATE_INDEX);
    if (tree) {
      return NumericRangeTree_OnForkCreated(tree);
    }
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_TAG)) {
    RedisModuleString *keyName = TagIndex_FormatName(parent_spec, field->fieldName);
    TagIndex *tagIndex = TagIndex_Open(parent_spec, keyName, DONT_CREATE_INDEX);
    RedisModule_FreeString(RSDummyContext, keyName);
    if (tagIndex) {
      return TagIndex_OnForkCreated(tagIndex);
    }
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_VECTOR)) {
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_FULLTEXT)) {
    return REDISMODULE_OK;
  }

  return REDISMODULE_OK;
}

int FieldSpec_OnForkComplete(FieldSpec *field, IndexSpec *parent_spec) {
  if (!field || !parent_spec) {
    return REDISMODULE_ERR;
  }

  // Handle different field types
  if (FIELD_IS(field, INDEXFLD_T_NUMERIC)) {
    RedisModuleString *keyName = IndexSpec_GetFormattedKey(parent_spec, field, INDEXFLD_T_NUMERIC);
    NumericRangeTree *tree = openNumericKeysDict(parent_spec, keyName, DONT_CREATE_INDEX);
    if (tree) {
      return NumericRangeTree_OnForkComplete(tree);
    }
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_TAG)) {
    RedisModuleString *keyName = TagIndex_FormatName(parent_spec, field->fieldName);
    TagIndex *tagIndex = TagIndex_Open(parent_spec, keyName, DONT_CREATE_INDEX);
    RedisModule_FreeString(RSDummyContext, keyName);
    if (tagIndex) {
      return TagIndex_OnForkComplete(tagIndex);
    }
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_VECTOR)) {
    return REDISMODULE_OK;
  } else if (FIELD_IS(field, INDEXFLD_T_FULLTEXT)) {
    return REDISMODULE_OK;
  }

  return REDISMODULE_OK;
}
