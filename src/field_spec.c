#include "field_spec.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

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

//---------------------------------------------------------------------------------------------

void FieldSpec::Cleanup() {
  delete name;
}

void FieldSpec::SetSortable() {
  RS_LOG_ASSERT(!(options & FieldSpec_Dynamic), "dynamic fields cannot be sortable");
  options |= FieldSpec_Sortable;
}

void FieldSpec::Initialize(FieldType type) {
  types |= type;
  if (IsFieldType(INDEXFLD_T_TAG)) {
    tagFlags = TAG_FIELD_DEFAULT_FLAGS;
    tagSep = TAG_FIELD_DEFAULT_SEP;
  }
}
