#ifndef SRC_FIELD_SPEC_H_
#define SRC_FIELD_SPEC_H_

#include "redisearch.h"
#include "value.h"

typedef enum fieldType { FIELD_FULLTEXT, FIELD_NUMERIC, FIELD_GEO, FIELD_TAG } FieldType;

typedef enum {
  FieldSpec_Sortable = 0x01,
  FieldSpec_NoStemming = 0x02,
  FieldSpec_NotIndexable = 0x04,
  FieldSpec_Phonetics = 0x08,
} FieldSpecOptions;

// Specific options for text fields
typedef struct {
  // weight in frequency calculations
  double weight;
  // bitwise id for field masks
  t_fieldId id;
} TextFieldOptions;

// Flags for tag fields
typedef enum {
  TagField_CaseSensitive = 0x01,
  TagField_TrimSpace = 0x02,
  TagField_RemoveAccents = 0x04,
} TagFieldFlags;

// Specific options for tag fields
typedef struct {
  char separator;
  TagFieldFlags flags;
} TagFieldOptions;

/* The fieldSpec represents a single field in the document's field spec.
Each field has a unique id that's a power of two, so we can filter fields
by a bit mask.
Each field has a type, allowing us to add non text fields in the future */
typedef struct FieldSpec {
  char* name;
  FieldType type;
  FieldSpecOptions options;

  int sortIdx;

  /**
   * Unique field index. Each field has a unique index regardless of its type
   */
  uint16_t index;

  union {
    TextFieldOptions textOpts;
    TagFieldOptions tagOpts;
  };

  // TODO: More options here..
} FieldSpec;

#define TAG_FIELD_DEFAULT_FLAGS TagField_TrimSpace& TagField_RemoveAccents;

#define FieldSpec_IsSortable(fs) ((fs)->options & FieldSpec_Sortable)
#define FieldSpec_IsNoStem(fs) ((fs)->options & FieldSpec_NoStemming)
#define FieldSpec_IsPhonetics(fs) ((fs)->options & FieldSpec_Phonetics)
#define FieldSpec_IsIndexable(fs) (0 == ((fs)->options & FieldSpec_NotIndexable))

FieldSpec* FieldSpec_CreateText();
FieldSpec* FieldSpec_CreateNumeric();
FieldSpec* FieldSpec_CreateGeo();
FieldSpec* FieldSpec_CreateTag();

void FieldSpec_InitializeText(FieldSpec* fs);
void FieldSpec_InitializeNumeric(FieldSpec* fs);
void FieldSpec_InitializeGeo(FieldSpec* fs);
void FieldSpec_InitializeTag(FieldSpec* fs);

void FieldSpec_SetName(FieldSpec* fs, const char* name);
void FieldSpec_SetIndex(FieldSpec* fs, uint16_t index);
void FieldSpec_TextNoStem(FieldSpec* fs);
void FieldSpec_TextSetWeight(FieldSpec* fs, double w);
void FieldSpec_TextPhonetic(FieldSpec* fs);
void FieldSpec_TagSetSeparator(FieldSpec* fs, char sep);
void FieldSpec_SetSortable(FieldSpec* fs);
void FieldSpec_SetNoIndex(FieldSpec* fs);

void FieldSpec_Cleanup(FieldSpec* fs);
void FieldSpec_Free(FieldSpec* fs);

RSValueType fieldTypeToValueType(FieldType ft);

#endif /* SRC_FIELD_SPEC_H_ */
