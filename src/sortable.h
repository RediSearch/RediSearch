#ifndef __RS_SORTABLE_H__
#define __RS_SORTABLE_H__
#include "redismodule.h"
#include "value.h"
/* Sortables - embedded sorting fields. When creating a schema we can specify fields that will be
 * sortable.
 * A sortable field means that its data will get copied into an inline table inside the index. right
 * now we copy strings in full so you should be careful about string length of sortable fields*/

// Maximum number of sortables
#define RS_SORTABLES_MAX 255

#pragma pack(1)

#define RS_SORTABLE_NUM 1
// #define RS_SORTABLE_EMBEDDED_STR 2
#define RS_SORTABLE_STR 3
// nil value means the value is empty
#define RS_SORTABLE_NIL 4

/* RSSortingVector is a vector of sortable values. All documents in a schema where sortable fields
 * are defined will have such a vector. */
typedef struct RSSortingVector {
  unsigned int len : 8;
  RSValue *values[];
} RSSortingVector;

#pragma pack()

/* RSSortingTable defines the length and names of the fields in a sorting vector. It is saved as
 * part of the spec */
typedef struct {
  int len : 8;
  struct sortField {
    const char *name;
    RSValueType type;
  } fields[];
} RSSortingTable;

/* RSSortingKey describes the sorting of a query and is parsed from the redis command arguments */
typedef struct {
  /* The field index we are sorting by */
  int index : 8;

  /* ASC/DESC flag */
  int ascending;
} RSSortingKey;

void RSSortingKey_Free(RSSortingKey *k);

/* Create a sorting table of a given length. Length can be up to RS_SORTABLES_MAX (255) */
RSSortingTable *NewSortingTable(int len);

/* Free a sorting table */
void SortingTable_Free(RSSortingTable *t);

/* Set a field in the table by index. This is called during the schema parsing */
void SortingTable_SetFieldName(RSSortingTable *tbl, int idx, const char *name, RSValueType t);

/* Parse the sorting key of a query from redis arguments. We expect SORTBY {filed} [ASC/DESC]. The
 * default is ASC if not specified.  This function returns 1 if we found sorting args, they are
 * valid and the field name exists */
int RSSortingTable_ParseKey(RSSortingTable *tbl, RSSortingKey *k, RedisModuleString **argv,
                            int argc);
/* Get the field index by name from the sorting table. Returns -1 if the field was not found */
int RSSortingTable_GetFieldIdx(RSSortingTable *tbl, const char *field);

/* Get the type of the field by its name. If it doesn't exist, return deflt */
RSValueType SortingTable_GetFieldType(RSSortingTable *tbl, const char *name, RSValueType delt);

/* Internal compare function between members of the sorting vectors, sorted by sk */
int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk);

/* Put a value in the sorting vector */
void RSSortingVector_Put(RSSortingVector *tbl, int idx, void *p, int type);

RSValue *RSSortingVector_Get(RSSortingVector *v, RSSortingKey *k);

size_t RSSortingVector_GetMemorySize(RSSortingVector *v);

/* Create a sorting vector of a given length for a document */
RSSortingVector *NewSortingVector(int len);

/* Free a sorting vector */
void SortingVector_Free(RSSortingVector *v);

/* Save a document's sorting vector into an rdb dump */
void SortingVector_RdbSave(RedisModuleIO *rdb, RSSortingVector *v);

/* Load a sorting vector from RDB */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb, int encver);

#endif