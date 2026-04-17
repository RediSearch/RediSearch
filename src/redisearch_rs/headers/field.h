#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redisearch_types.h"

/**
 * Field expiration predicate used when checking fields.
 * cbindgen:prefix-with-name
 * cbindgen:rename-all=ScreamingSnakeCase
 */
typedef enum FieldExpirationPredicate {
  FIELD_EXPIRATION_PREDICATE_DEFAULT = 0,
  FIELD_EXPIRATION_PREDICATE_MISSING = 1,
} FieldExpirationPredicate;

/**
 * cbindgen:prefix-with-name=true
 * Type representing either a field mask or field index.
 */
enum FieldMaskOrIndex_Tag
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  FieldMaskOrIndex_Index,
  FieldMaskOrIndex_Mask,
};
#ifndef __cplusplus
typedef uint8_t FieldMaskOrIndex_Tag;
#endif // __cplusplus

typedef union FieldMaskOrIndex {
  FieldMaskOrIndex_Tag tag;
  struct {
    FieldMaskOrIndex_Tag index_tag;
    t_fieldIndex index;
  };
  struct {
    FieldMaskOrIndex_Tag mask_tag;
    t_fieldMask mask;
  };
} FieldMaskOrIndex;

/**
 * Field filter context used when querying fields.
 */
typedef struct FieldFilterContext {
  union FieldMaskOrIndex field;
  enum FieldExpirationPredicate predicate;
} FieldFilterContext;
