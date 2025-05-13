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
