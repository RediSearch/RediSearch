#include "doc_table.h"
#include <sys/param.h>
#include <string.h>
#include <stdio.h>
#include "redismodule.h"
#include "util/fnv.h"

#include "rmalloc.h"

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap) {
  return (DocTable){.size = 1,
                    .cap = cap,
                    .maxDocId = 0,
                    .memsize = 0,
                    .docs = rm_calloc(cap, sizeof(DocumentMetadata)),
                    .dim = NewDocIdMap()};
}

/* Get the metadata for a doc Id from the DocTable.
*  If docId is not inside the table, we return NULL */
inline DocumentMetadata *DocTable_Get(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return NULL;
  }
  return &t->docs[docId];
}

/** Get the docId of a key if it exists in the table, or 0 if it doesnt */
t_docId DocTable_GetId(DocTable *dt, const char *key) {
  return DocIdMap_Get(&dt->dim, key);
}
/* Put a new document into the table, assign it an incremental id and store the metadata in the
* table.
*
* Return 0 if the document is already in the index  */
t_docId DocTable_Put(DocTable *t, const char *key, double score, u_char flags, const char *payload,
                     size_t payloadSize) {

  t_docId xid = DocIdMap_Get(&t->dim, key);
  // if the document is already in the index, return 0
  if (xid) {
    return 0;
  }
  t_docId docId = ++t->maxDocId;
  // if needed - grow the table
  if (t->maxDocId + 1 >= t->cap) {

    t->cap += 1 + (t->cap ? MIN(t->cap / 2, 1024 * 1024) : 1);
    t->docs = rm_realloc(t->docs, t->cap * sizeof(DocumentMetadata));
  }

  /* Copy the payload since it's probably an input string not retained */
  DocumentPayload *dpl = NULL;
  if (payload && payloadSize) {

    dpl = rm_malloc(sizeof(DocumentPayload));
    dpl->data = rm_calloc(1, payloadSize + 1);
    memcpy(dpl->data, payload, payloadSize);
    dpl->len = payloadSize;
    flags |= Document_HasPayload;
    t->memsize += payloadSize + sizeof(DocumentPayload);
  }

  t->docs[docId] = (DocumentMetadata){
      .key = rm_strdup(key), .score = score, .flags = flags, .payload = dpl, .maxFreq = 1};
  ++t->size;
  t->memsize += sizeof(DocumentMetadata) + strlen(key);
  DocIdMap_Put(&t->dim, key, docId);
  return docId;
}

DocumentPayload *DocTable_GetPayload(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return NULL;
  }
  return t->docs[docId].payload;
}

/* Get the "real" external key for an incremental id. Returns NULL if docId is not in the table. */
inline const char *DocTable_GetKey(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return NULL;
  }
  return t->docs[docId].key;
}

/* Get the score for a document from the table. Returns 0 if docId is not in the table. */
inline float DocTable_GetScore(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return 0;
  }
  return t->docs[docId].score;
}

void dmd_free(DocumentMetadata *md) {
  if (md->payload) {
    rm_free(md->payload->data);
    rm_free(md->payload);
    md->flags &= ~Document_HasPayload;
    md->payload = NULL;
  }
  rm_free(md->key);
}
void DocTable_Free(DocTable *t) {
  // we start at docId 1, not 0
  for (int i = 1; i < t->size; i++) {
    dmd_free(&t->docs[i]);
  }
  if (t->docs) {
    rm_free(t->docs);
  }
  DocIdMap_Free(&t->dim);
}

int DocTable_Delete(DocTable *t, const char *key) {
  t_docId docId = DocIdMap_Get(&t->dim, key);
  if (docId && docId <= t->maxDocId) {

    DocumentMetadata *md = &t->docs[docId];
    if (md->payload) {
      rm_free(md->payload->data);
      rm_free(md->payload);
      md->payload = NULL;
    }

    md->flags |= Document_Deleted;
    return DocIdMap_Delete(&t->dim, key);
  }
  return 0;
}

void DocTable_RdbSave(DocTable *t, RedisModuleIO *rdb) {

  RedisModule_SaveUnsigned(rdb, t->size);
  RedisModule_SaveUnsigned(rdb, t->maxDocId);
  for (int i = 1; i < t->size; i++) {
    RedisModule_SaveStringBuffer(rdb, t->docs[i].key, strlen(t->docs[i].key) + 1);
    RedisModule_SaveUnsigned(rdb, t->docs[i].flags);
    RedisModule_SaveUnsigned(rdb, t->docs[i].maxFreq);
    RedisModule_SaveFloat(rdb, t->docs[i].score);
    if (t->docs[i].flags & Document_HasPayload && t->docs[i].payload) {
      // save an extra space for the null terminator to make the payload null terminated on load
      RedisModule_SaveStringBuffer(rdb, t->docs[i].payload->data, t->docs[i].payload->len + 1);
    }
  }
}
void DocTable_RdbLoad(DocTable *t, RedisModuleIO *rdb, int encver) {
  size_t sz = RedisModule_LoadUnsigned(rdb);
  t->maxDocId = RedisModule_LoadUnsigned(rdb);

  if (sz > t->cap) {
    t->cap = sz;
    t->docs = rm_realloc(t->docs, t->cap * sizeof(DocumentMetadata));
  }
  t->size = t->cap;
  for (size_t i = 1; i < sz; i++) {
    size_t len;
    t->docs[i].key = RedisModule_LoadStringBuffer(rdb, &len);
    t->docs[i].flags = RedisModule_LoadUnsigned(rdb);
    t->docs[i].maxFreq = 0;
    if (encver > 1) {
      t->docs[i].maxFreq = RedisModule_LoadUnsigned(rdb);
    }
    t->docs[i].score = RedisModule_LoadFloat(rdb);
    t->docs[i].payload = NULL;
    // read payload if set
    if (t->docs[i].flags & Document_HasPayload) {
      t->docs[i].payload = RedisModule_Alloc(sizeof(DocumentPayload));
      t->docs[i].payload->data = RedisModule_LoadStringBuffer(rdb, &t->docs[i].payload->len);
      t->docs[i].payload->len--;
      t->memsize += t->docs[i].payload->len + sizeof(DocumentPayload);
    }

    DocIdMap_Put(&t->dim, t->docs[i].key, i);
    t->memsize += sizeof(DocumentMetadata) + len;
  }
}

void DocTable_AOFRewrite(DocTable *t, RedisModuleString *key, RedisModuleIO *aof) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(aof);
  for (int i = 1; i < t->size; i++) {

    RedisModuleString *ss = RedisModule_CreateStringPrintf(ctx, "%f", t->docs[i].score);
    // dump payload if possible
    if (t->docs[i].flags & Document_HasPayload && t->docs[i].payload) {
      RedisModule_EmitAOF(aof, "FT.DTADD", "sclsb", key, t->docs[i].key,
                          (long long)t->docs[i].flags, ss, t->docs[i].payload->data,
                          t->docs[i].payload->len);
    } else {
      RedisModule_EmitAOF(aof, "FT.DTADD", "scls", key, t->docs[i].key, (long long)t->docs[i].flags,
                          ss);
    }
    RedisModule_FreeString(ctx, ss);
  }
}

DocIdMap NewDocIdMap() {

  TrieMapNode *m = NewTrieMap();
  return (DocIdMap){m};
}

t_docId DocIdMap_Get(DocIdMap *m, const char *key) {

  void *val = TrieMapNode_Find(m->tm, (unsigned char *)key, strlen(key));
  if (val) {
    return *((t_docId *)val);
  }
  return 0;
}

void *_docIdMap_replace(void *oldval, void *newval) {
  if (oldval) {
    rm_free(oldval);
  }
  return newval;
}

void DocIdMap_Put(DocIdMap *m, const char *key, t_docId docId) {

  void *val = TrieMapNode_Find(m->tm, (unsigned char *)key, strlen(key));
  t_docId *pd = rm_malloc(sizeof(t_docId));
  *pd = docId;
  TrieMapNode_Add(&m->tm, (unsigned char *)key, strlen(key), pd, _docIdMap_replace);
}

void DocIdMap_Free(DocIdMap *m) {
  TrieMapNode_Free(m->tm, RedisModule_Free);
}

int DocIdMap_Delete(DocIdMap *m, const char *key) {
  return TrieMapNode_Delete(m->tm, (unsigned char *)key, strlen(key), RedisModule_Free);
}