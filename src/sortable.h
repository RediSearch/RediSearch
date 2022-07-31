#pragma once

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

/* RSSortingKey describes the sorting of a query and is parsed from the redis command arguments */
struct RSSortingKey {
  /* The field index we are sorting by */
  int index : 8;

  /* ASC/DESC flag */
  int ascending;
};


/* RSSortingVector is a vector of sortable values. All documents in a schema where sortable fields
 * are defined will have such a vector. */
struct RSSortingVector {
  unsigned int len : 8;
  RSValue *values[];

  RSSortingVector(int len_);
  RSSortingVector(RedisModuleIO *rdb, int encver);
  ~RSSortingVector();

  void Put(int idx, const void *p, int type);
  RSValue *RSSortingVector::Get(size_t index);
  size_t memsize() const;

  static int Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk, QueryError *qerr);

  void RdbSave(RedisModuleIO *rdb);
};

#pragma pack()

/* RSSortingTable defines the length and names of the fields in a sorting vector. It is saved as
 * part of the spec */
struct RSSortingTable {
  uint8_t len;
  struct sortField {
    const char *name;
    RSValueType type;
  } fields[RS_SORTABLES_MAX];

  int Add(const char *name, RSValueType t);
  int GetFieldIdx(const char *field);
};
