#include "field_spec.h"
#include "indexer.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

RSValueType fieldTypeToValueType(FieldType ft) {
  switch (ft) {
    case INDEXFLD_T_NUMERIC:
      return RSValue_Number;

    case INDEXFLD_T_FULLTEXT:
    case INDEXFLD_T_TAG:
    case INDEXFLD_T_GEO:
      return RSValue_String;

    case INDEXFLD_T_VECTOR: // TODO:
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
}

void FieldSpec_SetSortable(FieldSpec* fs) {
  RS_LOG_ASSERT(!(fs->options & FieldSpec_Dynamic), "dynamic fields cannot be sortable");
  fs->options |= FieldSpec_Sortable;
}

void FieldSpec_UpdateGlobalStat(FieldSpec *fs, int toAdd) {
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
    else if (fs->vectorOpts.vecSimParams.algo == VecSimAlgo_HNSWLIB)
      RSGlobalConfig.fieldsStats.numVectorFieldsHSNW += toAdd;
  } else if (fs->types & INDEXFLD_T_TAG) {  // tag field
    RSGlobalConfig.fieldsStats.numTagFields += toAdd;
    if (fs->tagOpts.tagFlags & TagField_CaseSensitive) {
      RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive += toAdd;
    }
  }

  if (fs->options & FieldSpec_Sortable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalConfig.fieldsStats.numTextFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalConfig.fieldsStats.numNumericFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalConfig.fieldsStats.numGeoFieldsSortable += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalConfig.fieldsStats.numTagFieldsSortable += toAdd;
  }
  if (fs->options & FieldSpec_NotIndexable) {
    if (fs->types & INDEXFLD_T_FULLTEXT) RSGlobalConfig.fieldsStats.numTextFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_NUMERIC) RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_GEO) RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex += toAdd;
    else if (fs->types & INDEXFLD_T_TAG) RSGlobalConfig.fieldsStats.numTagFieldsNoIndex += toAdd;
  }
}

const char *FieldSpec_GetTypeNames(int idx) {
  switch (idx) {
  case IXFLDPOS_FULLTEXT: return SPEC_TEXT_STR;
  case IXFLDPOS_TAG:      return SPEC_TAG_STR;
  case IXFLDPOS_NUMERIC:  return SPEC_NUMERIC_STR;
  case IXFLDPOS_GEO:      return SPEC_GEO_STR;
  case IXFLDPOS_VECTOR:   return SPEC_VECTOR_STR;

  default:
    RS_LOG_ASSERT(0, "oops");
    break;
  }
}
