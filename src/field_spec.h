/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef SRC_FIELD_SPEC_H_
#define SRC_FIELD_SPEC_H_

#include "redisearch.h"
#include "value.h"
#include "delimiters.h"
#include "VecSim/vec_sim.h"
#include "util/references.h"
#include "geometry/geometry_types.h"

#ifdef __cplusplus
#define RS_ENUM_BITWISE_HELPER(T)   \
  inline T operator|=(T a, int b) { \
    return (T)((int)a | b);         \
  }
#else
#define RS_ENUM_BITWISE_HELPER(T)
#endif

typedef enum {
  // Newline
  INDEXFLD_T_FULLTEXT = 0x01,
  INDEXFLD_T_NUMERIC = 0x02,
  INDEXFLD_T_GEO = 0x04,
  INDEXFLD_T_TAG = 0x08,
  INDEXFLD_T_VECTOR = 0x10,
  INDEXFLD_T_GEOMETRY = 0x20,
} FieldType;

#define INDEXFLD_NUM_TYPES 6

// clang-format off
// otherwise, it looks h o r r i b l e
#define INDEXTYPE_TO_POS(T)         \
  (T == INDEXFLD_T_FULLTEXT   ? 0 : \
  (T == INDEXFLD_T_NUMERIC    ? 1 : \
  (T == INDEXFLD_T_GEO        ? 2 : \
  (T == INDEXFLD_T_TAG        ? 3 : \
  (T == INDEXFLD_T_VECTOR     ? 4 : \
  (T == INDEXFLD_T_GEOMETRY   ? 5 : -1))))))

#define INDEXTYPE_FROM_POS(P) (1<<(P))
// clang-format on

#define IXFLDPOS_FULLTEXT INDEXTYPE_TO_POS(INDEXFLD_T_FULLTEXT)
#define IXFLDPOS_NUMERIC INDEXTYPE_TO_POS(INDEXFLD_T_NUMERIC)
#define IXFLDPOS_GEO INDEXTYPE_TO_POS(INDEXFLD_T_GEO)
#define IXFLDPOS_TAG INDEXTYPE_TO_POS(INDEXFLD_T_TAG)
#define IXFLDPOS_VECTOR INDEXTYPE_TO_POS(INDEXFLD_T_VECTOR)
#define IXFLDPOS_GEOMETRY INDEXTYPE_TO_POS(INDEXFLD_T_GEOMETRY)

RS_ENUM_BITWISE_HELPER(FieldType)

typedef enum {
  FieldSpec_Sortable = 0x01,
  FieldSpec_NoStemming = 0x02,
  FieldSpec_NotIndexable = 0x04,
  FieldSpec_Phonetics = 0x08,
  FieldSpec_Dynamic = 0x10,
  FieldSpec_UNF = 0x20,
  FieldSpec_WithSuffixTrie = 0x40,
  FieldSpec_UndefinedOrder = 0x80,
  FieldSpec_WithCustomDelimiters = 0x100,
} FieldSpecOptions;

RS_ENUM_BITWISE_HELPER(FieldSpecOptions)

// Flags for tag fields
typedef enum {
  TagField_CaseSensitive = 0x01,
  TagField_TrimSpace = 0x02,
  TagField_RemoveAccents = 0x04,
} TagFieldFlags;

#define TAG_FIELD_IS(f, t) (FIELD_IS((f), INDEXFLD_T_TAG) && (((f)->tagOpts.tagFlags) & (t)))

RS_ENUM_BITWISE_HELPER(TagFieldFlags)

/* The fieldSpec represents a single field in the document's field spec.
Each field has a unique id that's a power of two, so we can filter fields
by a bit mask.
Each field has a type, allowing us to add non text fields in the future */
typedef struct FieldSpec {
  char *name;
  char *path;
  FieldType types : 8;
  FieldSpecOptions options : 16;

  /** If this field is sortable, the sortable index */
  int16_t sortIdx;

  /** Unique field index. Each field has a unique index regardless of its type */
  uint16_t index;

  union {
    struct {
      // Flags for tag options
      TagFieldFlags tagFlags : 16;
      char tagSep;
    } tagOpts;
    struct {
      // Vector similarity index parameters.
      VecSimParams vecSimParams;
      // expected size of vector blob.
      size_t expBlobSize;
    } vectorOpts;
    struct {
      // Geometry index parameters
      GEOMETRY_COORDS geometryCoords;
    } geometryOpts;
  };

  // weight in frequency calculations
  double ftWeight;
  // ID used to identify the field within the field mask
  t_fieldId ftId;

  // Delimiters for this field
  DelimiterList *delimiters;

  // TODO: More options here..
} FieldSpec;

#define FIELD_IS(f, t) (((f)->types) & (t))
#define FIELD_CHKIDX(fmask, ix) (fmask & ix)

#define TAG_FIELD_DEFAULT_FLAGS (TagFieldFlags)(TagField_TrimSpace | TagField_RemoveAccents);
#define TAG_FIELD_DEFAULT_HASH_SEP ','
#define TAG_FIELD_DEFAULT_JSON_SEP '\0' // by default, JSON fields have no separetor

#define FieldSpec_IsSortable(fs) ((fs)->options & FieldSpec_Sortable)
#define FieldSpec_IsNoStem(fs) ((fs)->options & FieldSpec_NoStemming)
#define FieldSpec_IsPhonetics(fs) ((fs)->options & FieldSpec_Phonetics)
#define FieldSpec_IsIndexable(fs) (0 == ((fs)->options & FieldSpec_NotIndexable))
#define FieldSpec_HasSuffixTrie(fs) ((fs)->options & FieldSpec_WithSuffixTrie)
#define FieldSpec_IsUndefinedOrder(fs) ((fs)->options & FieldSpec_UndefinedOrder)
#define FieldSpec_IsUnf(fs) ((fs)->options & FieldSpec_UNF)
#define FieldSpec_HasCustomDelimiters(fs) ((fs)->options & FieldSpec_WithCustomDelimiters)

void FieldSpec_SetSortable(FieldSpec* fs);
void FieldSpec_Cleanup(FieldSpec* fs);
/**
 * Convert field type given by integer to the name type in string form.
 */
const char *FieldSpec_GetTypeNames(int idx);

RSValueType fieldTypeToValueType(FieldType ft);

int FieldSpec_RdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver);

void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f);

int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, StrongRef sp_ref,
  int encver);

#endif /* SRC_FIELD_SPEC_H_ */
