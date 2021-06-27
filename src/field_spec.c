#include "field_spec.h"
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
  if (fs->path) {
    if (fs->name != fs->path) {
      rm_free(fs->path);
    }
    fs->path = NULL;
  }
  if (fs->name) {
    if (fs->name != fs->path) {
      rm_free(fs->name);
    }
    fs->name = NULL;
  }
  if (fs->path) {
    rm_free(fs->path);
    fs->path = NULL;
  }
}

void FieldSpec_SetSortable(FieldSpec* fs) {
  RS_LOG_ASSERT(!(fs->options & FieldSpec_Dynamic), "dynamic fields cannot be sortable");
  fs->options |= FieldSpec_Sortable;
}