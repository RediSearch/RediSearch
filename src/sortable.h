/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "value.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Sortables - embedded sorting fields. When creating a schema we can specify fields that will be
 * sortable.
 * A sortable field means that its data will get copied into an inline table inside the index. right
 * now we copy strings in full so you should be careful about string length of sortable fields*/

// Maximum number of sortables
#define RS_SORTABLES_MAX 1024 // aligned with SPEC_MAX_FIELDS

#define RS_SORTABLE_NUM 1
// #define RS_SORTABLE_EMBEDDED_STR 2
#define RS_SORTABLE_STR 3
// nil value means the value is empty
#define RS_SORTABLE_NIL 4
#define RS_SORTABLE_RSVAL 5

/* RSSortingVector is a vector of sortable values. All documents in a schema where sortable fields
 * are defined will have such a vector. */
#pragma pack(2)
typedef struct RSSortingVector {
  uint16_t len;      // Should be able to hold RS_SORTABLES_MAX-1 (requires 10 bits today)
  RSValue *values[];
} RSSortingVector;

#pragma pack()

/* Put a value in the sorting vector */
void RSSortingVector_Put(RSSortingVector *vec, int idx, const void *p, int type, int unf);

/* Returns the value for a given index. Does not increment the refcount */
static inline RSValue *RSSortingVector_Get(RSSortingVector *v, size_t index) {
  return v->len > index ? v->values[index] : NULL;
}

size_t RSSortingVector_GetMemorySize(RSSortingVector *v);

/* Create a sorting vector of a given length for a document */
RSSortingVector *NewSortingVector(int len);

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v);

/* Load a sorting vector from RDB. Used by legacy RDB load only */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb, int encver);

#ifdef __cplusplus
}
#endif
