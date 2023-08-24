/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __RS_SORTABLE_H__
#define __RS_SORTABLE_H__
#include "redismodule.h"
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
#pragma pack(1)
typedef struct RSSortingVector {
  unsigned char len;
  RSValue *values[];
} RSSortingVector;
#pragma pack()

/* RSSortingTable defines the length and names of the fields in a sorting vector. It is saved as
 * part of the spec */
typedef struct {
  const char *name;
  RSValueType type;
} RSSortField;

typedef struct {
  uint16_t len;
  uint16_t cap;
  RSSortField fields[1];
} RSSortingTable;

/* RSSortingKey describes the sorting of a query and is parsed from the redis command arguments */
typedef struct {
  /* The field index we are sorting by */
  int index : 8;

  /* ASC/DESC flag */
  int ascending;
} RSSortingKey;

/* Create a sorting table. */
RSSortingTable *NewSortingTable();

/* Free a sorting table */
void SortingTable_Free(RSSortingTable *t);

/** Adds a field and returns the ID of the newly-inserted field */
int RSSortingTable_Add(RSSortingTable **tbl, const char *name, RSValueType t);

/* Get the field index by name from the sorting table. Returns -1 if the field was not found */
int RSSortingTable_GetFieldIdx(RSSortingTable *tbl, const char *field);

/* Internal compare function between members of the sorting vectors, sorted by sk */
int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk,
                        QueryError *qerr);

/* Put a value in the sorting vector */
void RSSortingVector_Put(RSSortingVector *tbl, int idx, const void *p, int type, int unf);

/* Returns the value for a given index. Does not increment the refcount */
static inline RSValue *RSSortingVector_Get(RSSortingVector *v, size_t index) {
  if (v->len <= index) {
    return NULL;
  }
  return v->values[index];
}

size_t RSSortingVector_GetMemorySize(RSSortingVector *v);

/* Create a sorting vector of a given length for a document */
RSSortingVector *NewSortingVector(int len);

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v);

/* Save a document's sorting vector into an rdb dump */
void SortingVector_RdbSave(RedisModuleIO *rdb, RSSortingVector *v);

/* Load a sorting vector from RDB */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb, int encver);

#ifdef __cplusplus
}
#endif
#endif
