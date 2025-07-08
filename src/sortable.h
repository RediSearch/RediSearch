/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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

/* Put a number in the sorting vector */
void RSSortingVector_PutNum(RSSortingVector *vec, size_t idx, double num);

/* Put a string in the sorting vector, the caller has to ensure that the String is normalized, see normalizeStr() */
void RSSortingVector_PutStr(RSSortingVector* vec, size_t idx, const char* str);

/* Put another RSValue instance in the sorting vector */
void RSSortingVector_PutRSVal(RSSortingVector* vec, size_t idx, RSValue* val);

/* Returns the value for a given index. Does not increment the refcount */
static inline RSValue *RSSortingVector_Get(const RSSortingVector *v, size_t index) {
  return v->len > index ? v->values[index] : NULL;
}

static inline size_t RSSortingVector_Length(const RSSortingVector* vec) {
    return vec->len;
}

size_t RSSortingVector_GetMemorySize(RSSortingVector *v);

/* Create a sorting vector of a given length for a document */
RSSortingVector *NewSortingVector(size_t len);

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v);

/* Load a sorting vector from RDB. Used by legacy RDB load only */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb);

/* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str);

#ifdef __cplusplus
}
#endif
