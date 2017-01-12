#ifndef __DOC_TABLE_H__
#define __DOC_TABLE_H__
#include <stdlib.h>
#include "types.h"

#pragma pack(1)
typedef struct {
  char *key;
  float score;
  u_char flags;
} DocumentMetadata;
#pragma pack()

typedef struct {
  size_t size;
  t_docId maxDocId;
  size_t cap;
  DocumentMetadata *docs;
} DocTable;

DocTable NewDocTable(size_t cap);
DocumentMetadata *DocTable_Get(DocTable *t, t_docId docId);
// int DocTable_Set(DocTable *t, t_docId docId, double score, u_char flags);
t_docId DocTable_Put(DocTable *t, const char *key, double score, u_char flags);
const char *DocTable_GetKey(DocTable *t, t_docId docId);
float DocTable_GetScore(DocTable *t, t_docId docId);

void DocTable_Free(DocTable *t);

#endif