
#pragma once

#include "redisearch.h"
#include "value.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
#define RS_ENUM_BITWISE_HELPER(T)   \
  inline T operator|=(T a, int b) { \
    return (T)((int)a | b);         \
  }
#else
#define RS_ENUM_BITWISE_HELPER(T)
#endif

//---------------------------------------------------------------------------------------------

enum FieldType {
  // Newline
  INDEXFLD_T_FULLTEXT = 0x01,
  INDEXFLD_T_NUMERIC = 0x02,
  INDEXFLD_T_GEO = 0x04,
  INDEXFLD_T_TAG = 0x08
};

#define INDEXFLD_NUM_TYPES 4

// clang-format off
// otherwise, it looks h o r r i b l e
#define INDEXTYPE_TO_POS(T)           \
  (T == INDEXFLD_T_FULLTEXT   ? 0 : \
  (T == INDEXFLD_T_NUMERIC    ? 1 : \
  (T == INDEXFLD_T_GEO        ? 2 : \
  (T == INDEXFLD_T_TAG        ? 3 : -1))))

#define INDEXTYPE_FROM_POS(P) (1<<(P))
// clang-format on

#define IXFLDPOS_FULLTEXT INDEXTYPE_TO_POS(INDEXFLD_T_FULLTEXT)
#define IXFLDPOS_NUMERIC INDEXTYPE_TO_POS(INDEXFLD_T_NUMERIC)
#define IXFLDPOS_GEO INDEXTYPE_TO_POS(INDEXFLD_T_GEO)
#define IXFLDPOS_TAG INDEXTYPE_TO_POS(INDEXFLD_T_TAG)

RS_ENUM_BITWISE_HELPER(FieldType)

//---------------------------------------------------------------------------------------------

enum FieldSpecOptions {
  FieldSpec_Sortable = 0x01,
  FieldSpec_NoStemming = 0x02,
  FieldSpec_NotIndexable = 0x04,
  FieldSpec_Phonetics = 0x08,
  FieldSpec_Dynamic = 0x10
};

RS_ENUM_BITWISE_HELPER(FieldSpecOptions)

//---------------------------------------------------------------------------------------------

// Flags for tag fields
enum TagFieldFlags {
  TagField_CaseSensitive = 0x01,
  TagField_TrimSpace = 0x02,
  TagField_RemoveAccents = 0x04,
};

RS_ENUM_BITWISE_HELPER(TagFieldFlags)

//---------------------------------------------------------------------------------------------

// The fieldSpec represents a single field in the document's field spec.
// Each field has a unique id that's a power of two, so we can filter fields by a bit mask.
// Each field has a type, allowing us to add non text fields in the future.

struct FieldSpec {
  char* name;
  FieldType types : 8;
  FieldSpecOptions options : 8;

  // If this field is sortable, the sortable index
  int16_t sortIdx;

  // Unique field index. Each field has a unique index regardless of its type
  uint16_t index;

  // Flags for tag options
  TagFieldFlags tagFlags : 16;
  char tagSep;

  // weight in frequency calculations
  double ftWeight;
  // ID used to identify the field within the field mask
  t_fieldId ftId;

  // TODO: More options here..

  void SetSortable();
  void Cleanup();

  bool IsSortable() const { return options & FieldSpec_Sortable; }
  bool IsNoStem() const { return options & FieldSpec_NoStemming; }
  bool IsPhonetics() const { return options & FieldSpec_Phonetics; }
  bool IsIndexable() const { return 0 == (options & FieldSpec_NotIndexable); }
};

//---------------------------------------------------------------------------------------------

#define FIELD_IS(f, t) (((f)->types) & (t))
#define FIELD_CHKIDX(fmask, ix) ((fmask) & (ix))

#define TAG_FIELD_DEFAULT_FLAGS (TagFieldFlags)(TagField_TrimSpace | TagField_RemoveAccents);
#define TAG_FIELD_DEFAULT_SEP ','

RSValueType fieldTypeToValueType(FieldType ft);

///////////////////////////////////////////////////////////////////////////////////////////////
