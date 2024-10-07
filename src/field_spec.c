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
  if (fs->path && fs->name != fs->path) {
    HiddenString_Free(fs->path, true);
    fs->path = NULL;
  }
  fs->path = NULL;
  if (fs->name) {
    HiddenString_Free(fs->name, true);
    fs->name = NULL;
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
  FieldSpecInfo_SetIdentifier(&info, HiddenString_Get(fs->path, obfuscate));
  FieldSpecInfo_SetAttribute(&info, HiddenString_Get(fs->name, obfuscate));
  FieldSpecInfo_SetIndexError(&info, fs->indexError);
  return info;
}
