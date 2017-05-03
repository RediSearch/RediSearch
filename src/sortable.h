#ifndef __RS_SORTABLE_H__
#define __RS_SORTABLE_H__
#include "redismodule.h"
#pragma pack(1)

#define RS_SORTABLE_NUM 1
#define RS_SORTABLE_EMBEDDED_STR 2
#define RS_SORTABLE_STR 3
// nil value means the value is empty
#define RS_SORTABLE_NIL 4

typedef struct {
  unsigned char len;
  char data[7];
} RSEmbeddedStr;

typedef struct {
  union {
    // RSEmbeddedStr embstr;
    char *str;
    double num;
  };
  int type : 8;
} RSSortableValue;

typedef struct RSSortingVector {
  unsigned int len : 8;
  RSSortableValue values[];
} RSSortingVector;

typedef struct {
  int len : 8;
  const char *fields[];
} RSSortingTable;

typedef struct {
  int index : 8;
  const char *field;
  int ascending;
} RSSortingKey;

RSSortingTable *NewSortingTable(int len);
void SortingTable_Free(RSSortingTable *t);
void SortingTable_SetFieldName(RSSortingTable *tbl, int idx, const char *name);

int RSSortingTable_GetFieldIdx(RSSortingTable *tbl, const char *field);

int RSSortingVector_Cmp(RSSortingVector *self, RSSortingVector *other, int idx);

void RSSortingVector_Put(RSSortingVector *tbl, int idx, void *p, int type);

RSSortingVector *NewSortingVector(int len);
void SortingVector_Free(RSSortingVector *v);
void SortingVector_RdbSave(RedisModuleIO *rdb, RSSortingVector *v);
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb, int encver);

#pragma pack()

#endif