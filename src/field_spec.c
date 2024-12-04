/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "field_spec.h"
#include "indexer.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "vector_index.h"
#include "global_stats.h"
#include "obfuscation/obfuscation_api.h"

RSValueType fieldTypeToValueType(FieldType ft) {
  switch (ft) {
    case INDEXFLD_T_NUMERIC:
      return RSValue_Number;

    case INDEXFLD_T_FULLTEXT:
    case INDEXFLD_T_TAG:
    case INDEXFLD_T_GEO:
      return RSValue_String;

    // Currently not supported
    case INDEXFLD_T_VECTOR:
    case INDEXFLD_T_GEOMETRY:
      return RSValue_Null;
  }
  return RSValue_Null;
}

void FieldSpec_Cleanup(FieldSpec* fs) {
  // if `AS` was not used, name and path are pointing at the same string
  if (fs->fieldPath && fs->fieldName != fs->fieldPath) {
    HiddenName_Free(fs->fieldPath, true);
  }
  fs->fieldPath = NULL;
  if (fs->fieldName) {
    HiddenName_Free(fs->fieldName, true);
    fs->fieldName = NULL;
  }

  if (fs->types & INDEXFLD_T_VECTOR) {
    VecSimParams_Cleanup(&fs->vectorOpts.vecSimParams);
  }

  IndexError_Clear(fs->indexError);
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

FieldSpecInfo FieldSpec_GetInfo(const FieldSpec *fs, bool obfuscate) {
  FieldSpecInfo info = {0};
  FieldSpecInfo_SetIdentifier(&info, FieldSpec_FormatPath(fs, obfuscate));
  FieldSpecInfo_SetAttribute(&info, FieldSpec_FormatName(fs, obfuscate));
  FieldSpecInfo_SetIndexError(&info, fs->indexError);
  return info;
}

void FieldSpec_AddError(FieldSpec *fs, const char *error_message, RedisModuleString *key) {
  IndexError_AddError(&fs->indexError, error_message, key);
  FieldsGlobalStats_UpdateIndexError(fs->types, 1);
}

size_t FieldSpec_GetIndexErrorCount(const FieldSpec *fs) {
  return IndexError_ErrorCount(&fs->indexError);
}

RedisModuleString *FormatFieldNameOrPath(t_uniqueId fieldId, HiddenName* name, void (*callback)(t_uniqueId, char*), bool obfuscate) {
  char obfuscated[MAX(MAX_OBFUSCATED_FIELD_NAME, MAX_OBFUSCATED_PATH_NAME)];
  const char* value = obfuscated;
  if (obfuscate) {
    callback(fieldId, obfuscated);
  } else {
    value = HiddenName_GetUnsafe(name, NULL);
  }
  if (!isUnsafeForSimpleString(value)) {
    return RedisModule_CreateString(NULL, value, strlen(value));
  } else {
    char *escaped = escapeSimpleString(value);
    RedisModuleString *ret = RedisModule_CreateString(NULL, escaped, strlen(escaped));
    rm_free(escaped);
    return ret;
  }
}

RedisModuleString *FieldSpec_FormatName(const FieldSpec *fs, bool obfuscate) {
  return FormatFieldNameOrPath(fs->ftId, fs->fieldName, Obfuscate_Field, obfuscate);
}

RedisModuleString *FieldSpec_FormatPath(const FieldSpec *fs, bool obfuscate) {
  return FormatFieldNameOrPath(fs->ftId, fs->fieldPath, Obfuscate_FieldPath, obfuscate);
}