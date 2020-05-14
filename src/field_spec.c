#include "field_spec.h"
#include "rmalloc.h"

RSValueType fieldTypeToValueType(FieldType ft) {
  switch (ft) {
    case INDEXFLD_T_NUMERIC:
      return RSValue_Number;
    case INDEXFLD_T_FULLTEXT:
    case INDEXFLD_T_TAG:
      return RSValue_String;
    case INDEXFLD_T_GEO:
    default:
      // geo is not sortable so we don't care as of now...
      return RSValue_Null;
  }
}

void FieldSpec_Cleanup(FieldSpec* fs) {
  if (fs->name) {
    rm_free(fs->name);
    fs->name = NULL;
  }
}

void FieldSpec_SetSortable(FieldSpec* fs) {
  assert(!(fs->options & FieldSpec_Dynamic) && "dynamic fields cannot be sortable");
  fs->options |= FieldSpec_Sortable;
}