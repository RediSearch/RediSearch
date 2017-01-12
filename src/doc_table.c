#include "doc_table.h"
#include <sys/param.h>
#include <string.h>
#include <stdio.h>
#include "redismodule.h"

DocTable NewDocTable(size_t cap) {
  return (DocTable){.size = 0,
                    .cap = cap,
                    .maxDocId = 0,
                    .docs = RedisModule_Calloc(cap, sizeof(DocumentMetadata))};
}

inline DocumentMetadata *DocTable_Get(DocTable *t, t_docId docId) {
  if (docId > t->maxDocId) {
    return NULL;
  }
  return &t->docs[docId];
}
t_docId DocTable_Put(DocTable *t, const char *key, double score, u_char flags) {
  t_docId docId = ++t->maxDocId;
  // if needed - grow the table
  if (t->size >= t->cap) {
    printf("old cap %zd ", t->cap);
    t->cap = 1 + (t->cap ? MIN(t->cap * 3 / 2, 1024 * 1024) : 1);
    printf("new cap %zd ", t->cap);
    t->docs = RedisModule_Realloc(t->docs, t->cap * sizeof(DocumentMetadata));
  }

  t->docs[docId] = (DocumentMetadata){
      .key = RedisModule_Strdup(key), .score = score, .flags = flags,
  };
  ++t->size;
  return docId;
}

const char *DocTable_GetKey(DocTable *t, t_docId docId) {
  if (docId > t->maxDocId) {
    return NULL;
  }
  return t->docs[docId].key;
}

float DocTable_GetScore(DocTable *t, t_docId docId) {
  if (docId > t->maxDocId) {
    return 0;
  }
  return t->docs[docId].score;
}

void DocTable_Free(DocTable *t) {
  // we start at docId 1, not 0
  for (int i = 1; i < t->size; i++) {
    RedisModule_Free(t->docs[i].key);
  }
  if (t->docs) {
    RedisModule_Free(t->docs);
  }
}