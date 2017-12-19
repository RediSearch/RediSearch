#include "doc_table.h"
#include <sys/param.h>
#include <string.h>
#include <stdio.h>
#include "redismodule.h"
#include "util/fnv.h"
#include "dep/triemap/triemap.h"
#include "sortable.h"
#include "rmalloc.h"
#include "spec.h"

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap) {
  return (DocTable){.size = 1,
                    .cap = cap,
                    .maxDocId = 0,
                    .memsize = 0,
                    .docs = rm_calloc(cap, sizeof(RSDocumentMetadata)),
                    .dim = NewDocIdMap()};
}

/* Get the metadata for a doc Id from the DocTable.
 *  If docId is not inside the table, we return NULL */
inline RSDocumentMetadata *DocTable_Get(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return NULL;
  }
  return &t->docs[docId];
}

/** Get the docId of a key if it exists in the table, or 0 if it doesnt */
t_docId DocTable_GetId(DocTable *dt, RSDocumentKey key) {
  return DocIdMap_Get(&dt->dim, key);
}

/* Set the payload for a document. Returns 1 if we set the payload, 0 if we couldn't find the
 * document */
int DocTable_SetPayload(DocTable *t, t_docId docId, const char *data, size_t len) {
  /* Get the metadata */
  RSDocumentMetadata *dmd = DocTable_Get(t, docId);
  if (!dmd || !data) {
    return 0;
  }

  /* If we already have metadata - clean up the old data */
  if (dmd->payload) {
    /* Free the old payload */
    if (dmd->payload->data) {
      rm_free(dmd->payload->data);
    }
    t->memsize -= dmd->payload->len;
  } else {
    dmd->payload = rm_malloc(sizeof(RSPayload));
  }
  /* Copy it... */
  dmd->payload->data = rm_calloc(1, len + 1);
  dmd->payload->len = len;
  memcpy(dmd->payload->data, data, len);

  dmd->flags |= Document_HasPayload;
  t->memsize += len;
  return 1;
}

/* Set the sorting vector for a document. If the vector is NULL we mark the doc as not having a
 * vector. Returns 1 on success, 0 if the document does not exist. No further validation is done */
int DocTable_SetSortingVector(DocTable *t, t_docId docId, RSSortingVector *v) {
  RSDocumentMetadata *dmd = DocTable_Get(t, docId);
  if (!dmd) {
    return 0;
  }

  /* Null vector means remove the current vector if it exists */
  if (!v) {
    if (dmd->sortVector) {
      SortingVector_Free(dmd->sortVector);
    }
    dmd->flags &= ~Document_HasSortVector;
    return 1;
  }

  /* Set th new vector and the flags accordingly */
  dmd->sortVector = v;
  dmd->flags |= Document_HasSortVector;

  return 1;
}

int DocTable_SetByteOffsets(DocTable *t, t_docId docId, RSByteOffsets *v) {
  RSDocumentMetadata *dmd = DocTable_Get(t, docId);
  if (!dmd) {
    return 0;
  }

  dmd->byteOffsets = v;
  dmd->flags |= Document_HasOffsetVector;
  return 1;
}

/* Put a new document into the table, assign it an incremental id and store the metadata in the
 * table.
 *
 * Return 0 if the document is already in the index  */
t_docId DocTable_Put(DocTable *t, RSDocumentKey key, double score, u_char flags,
                     const char *payload, size_t payloadSize) {

  t_docId xid = DocIdMap_Get(&t->dim, key);
  // if the document is already in the index, return 0
  if (xid) {
    return 0;
  }
  t_docId docId = ++t->maxDocId;
  // if needed - grow the table
  if (t->maxDocId + 1 >= t->cap) {

    t->cap += 1 + (t->cap ? MIN(t->cap / 2, 1024 * 1024) : 1);
    t->docs = rm_realloc(t->docs, t->cap * sizeof(RSDocumentMetadata));
  }

  /* Copy the payload since it's probably an input string not retained */
  RSPayload *dpl = NULL;
  if (payload && payloadSize) {

    dpl = rm_malloc(sizeof(RSPayload));
    dpl->data = rm_calloc(1, payloadSize + 1);
    memcpy(dpl->data, payload, payloadSize);
    dpl->len = payloadSize;
    flags |= Document_HasPayload;
    t->memsize += payloadSize + sizeof(RSPayload);
  }

  sds keyPtr = sdsnewlen(key.str, key.len);

  t->docs[docId] = (RSDocumentMetadata){
      .keyPtr = keyPtr, .score = score, .flags = flags, .payload = dpl, .maxFreq = 1};
  ++t->size;
  t->memsize += sizeof(RSDocumentMetadata) + sdsAllocSize(keyPtr);
  DocIdMap_Put(&t->dim, key, docId);
  return docId;
}

RSPayload *DocTable_GetPayload(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return NULL;
  }
  return t->docs[docId].payload;
}

/* Get the "real" external key for an incremental id. Returns NULL if docId is not in the table. */
inline RSDocumentKey DocTable_GetKey(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return MakeDocKey(NULL, 0);
  }
  return MakeDocKey(t->docs[docId].keyPtr, sdslen(t->docs[docId].keyPtr));
}

/* Get the score for a document from the table. Returns 0 if docId is not in the table. */
inline float DocTable_GetScore(DocTable *t, t_docId docId) {
  if (docId == 0 || docId > t->maxDocId) {
    return 0;
  }
  return t->docs[docId].score;
}

void dmd_free(RSDocumentMetadata *md) {
  if (md->payload) {
    rm_free(md->payload->data);
    rm_free(md->payload);
    md->flags &= ~Document_HasPayload;
    md->payload = NULL;
  }
  if (md->sortVector) {
    SortingVector_Free(md->sortVector);
    md->sortVector = NULL;
    md->flags &= ~Document_HasSortVector;
  }
  if (md->byteOffsets) {
    RSByteOffsets_Free(md->byteOffsets);
    md->byteOffsets = NULL;
    md->flags &= ~Document_HasOffsetVector;
  }
  sdsfree(md->keyPtr);
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

int DocTable_Delete(DocTable *t, RSDocumentKey key) {
  t_docId docId = DocIdMap_Get(&t->dim, key);
  if (docId && docId <= t->maxDocId) {

    RSDocumentMetadata *md = &t->docs[docId];
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
    const RSDocumentMetadata *dmd = t->docs + i;

    RedisModule_SaveStringBuffer(rdb, dmd->keyPtr, sdslen(dmd->keyPtr));
    RedisModule_SaveUnsigned(rdb, dmd->flags);
    RedisModule_SaveUnsigned(rdb, dmd->maxFreq);
    RedisModule_SaveUnsigned(rdb, dmd->len);
    RedisModule_SaveFloat(rdb, dmd->score);
    if (dmd->flags & Document_HasPayload && dmd->payload) {
      // save an extra space for the null terminator to make the payload null terminated on load
      RedisModule_SaveStringBuffer(rdb, dmd->payload->data, dmd->payload->len + 1);
    }

    if (dmd->flags & Document_HasSortVector) {
      SortingVector_RdbSave(rdb, dmd->sortVector);
    }

    if (dmd->flags & Document_HasOffsetVector) {
      Buffer tmp;
      Buffer_Init(&tmp, 16);
      RSByteOffsets_Serialize(dmd->byteOffsets, &tmp);
      RedisModule_SaveStringBuffer(rdb, tmp.data, tmp.offset);
      Buffer_Free(&tmp);
    }
  }
}
void DocTable_RdbLoad(DocTable *t, RedisModuleIO *rdb, int encver) {
  size_t sz = RedisModule_LoadUnsigned(rdb);
  t->maxDocId = RedisModule_LoadUnsigned(rdb);

  if (sz > t->cap) {
    t->cap = sz;
    t->docs = rm_realloc(t->docs, t->cap * sizeof(RSDocumentMetadata));
  }
  t->size = sz;
  for (size_t i = 1; i < sz; i++) {
    size_t len;
    char *tmpPtr = RedisModule_LoadStringBuffer(rdb, &len);
    if (encver < INDEX_MIN_BINKEYS_VERSION) {
      // Previous versions would encode the NUL byte
      len--;
    }
    t->docs[i].keyPtr = sdsnewlen(tmpPtr, len);
    rm_free(tmpPtr);

    t->docs[i].flags = RedisModule_LoadUnsigned(rdb);
    t->docs[i].maxFreq = 1;
    t->docs[i].len = 1;
    if (encver > 1) {
      t->docs[i].maxFreq = RedisModule_LoadUnsigned(rdb);
    }
    if (encver >= INDEX_MIN_DOCLEN_VERSION) {
      t->docs[i].len = RedisModule_LoadUnsigned(rdb);
    } else {
      // In older versions, default the len to max freq to avoid division by zero.
      t->docs[i].len = t->docs[i].maxFreq;
    }

    t->docs[i].score = RedisModule_LoadFloat(rdb);
    t->docs[i].payload = NULL;
    // read payload if set
    if (t->docs[i].flags & Document_HasPayload) {
      t->docs[i].payload = RedisModule_Alloc(sizeof(RSPayload));
      t->docs[i].payload->data = RedisModule_LoadStringBuffer(rdb, &t->docs[i].payload->len);
      t->docs[i].payload->len--;
      t->memsize += t->docs[i].payload->len + sizeof(RSPayload);
    }
    if (t->docs[i].flags & Document_HasSortVector) {
      t->docs[i].sortVector = SortingVector_RdbLoad(rdb, encver);
    }

    if (t->docs[i].flags & Document_HasOffsetVector) {
      size_t nTmp = 0;
      char *tmp = RedisModule_LoadStringBuffer(rdb, &nTmp);
      Buffer *bufTmp = Buffer_Wrap(tmp, nTmp);
      t->docs[i].byteOffsets = LoadByteOffsets(bufTmp);
      free(bufTmp);
      rm_free(tmp);
    }

    // We always save deleted docs to rdb, but we don't want to load them back to the id map
    if (!(t->docs[i].flags & Document_Deleted)) {
      DocIdMap_Put(&t->dim, MakeDocKey(t->docs[i].keyPtr, sdslen(t->docs[i].keyPtr)), i);
    }
    t->memsize += sizeof(RSDocumentMetadata) + len;
  }
}

void DocTable_AOFRewrite(DocTable *t, const char *indexName, RedisModuleIO *aof) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(aof);
  for (int i = 1; i < t->size; i++) {
    const RSDocumentMetadata *dmd = t->docs + i;
    RedisModuleString *ss = RedisModule_CreateStringPrintf(ctx, "%f", dmd->score);
    // dump payload if possible
    const char *payload = NULL;
    size_t payloadLen = 0;
    char *byteOffsets = NULL;
    size_t byteOffsetsLen = 0;

    if ((dmd->flags & Document_HasPayload) && dmd->payload) {
      payload = dmd->payload->data;
      payloadLen = dmd->payload->len;
    }

    Buffer offsetsBuf = {0};
    if ((dmd->flags & Document_HasOffsetVector) && dmd->byteOffsets) {
      Buffer_Init(&offsetsBuf, 16);
      RSByteOffsets_Serialize(dmd->byteOffsets, &offsetsBuf);
      byteOffsets = offsetsBuf.data;
      byteOffsetsLen = offsetsBuf.offset;
    }

    RedisModule_EmitAOF(aof, "FT.DTADD", "cblsbb", indexName, dmd->keyPtr, sdslen(dmd->keyPtr),
                        dmd->flags, ss, payload, payloadLen, byteOffsets, byteOffsetsLen);

    Buffer_Free(&offsetsBuf);
    RedisModule_FreeString(ctx, ss);
  }
}

DocIdMap NewDocIdMap() {

  TrieMap *m = NewTrieMap();
  return (DocIdMap){m};
}

t_docId DocIdMap_Get(DocIdMap *m, RSDocumentKey key) {

  void *val = TrieMap_Find(m->tm, (char *)key.str, key.len);
  if (val && val != TRIEMAP_NOTFOUND) {
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

void DocIdMap_Put(DocIdMap *m, RSDocumentKey key, t_docId docId) {

  t_docId *pd = rm_malloc(sizeof(t_docId));
  *pd = docId;
  TrieMap_Add(m->tm, (char *)key.str, key.len, pd, _docIdMap_replace);
}

void DocIdMap_Free(DocIdMap *m) {
  TrieMap_Free(m->tm, RedisModule_Free);
}

int DocIdMap_Delete(DocIdMap *m, RSDocumentKey key) {
  return TrieMap_Delete(m->tm, (char *)key.str, key.len, RedisModule_Free);
}