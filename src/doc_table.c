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
#include "config.h"

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap, size_t max_size) {
  return (DocTable){.size = 1,
                    .cap = cap,
                    .maxDocId = 0,
                    .memsize = 0,
                    .sortablesSize = 0,
                    .maxSize = max_size,
                    .buckets = rm_calloc(cap, sizeof(DMDChain)),
                    .dim = NewDocIdMap()};
}

static inline uint32_t DocTable_GetBucket(const DocTable *t, t_docId docId) {
  return docId < t->maxSize ? docId : docId % t->maxSize;
}

static inline int DocTable_ValidateDocId(const DocTable *t, t_docId docId) {
  return docId != 0 && docId <= t->maxDocId;
}

/* Get the metadata for a doc Id from the DocTable.
 *  If docId is not inside the table, we return NULL */

int DMDChain_IsEmpty(const DMDChain *dmdChain) {
  return !dmdChain->last;
}

RSDocumentMetadata *DocTable_Get(const DocTable *t, t_docId docId) {
  if (!DocTable_ValidateDocId(t, docId)) {
    return NULL;
  }
  uint32_t bucketIndex = DocTable_GetBucket(t, docId);
  DMDChain *dmdChain = &t->buckets[bucketIndex];
  RSDocumentMetadata *currDmd = dmdChain->first;
  while (currDmd) {
    if (currDmd->id == docId) {
      return currDmd;
    }
    currDmd = currDmd->next;
  }
  return NULL;
}

int DocTable_Exists(const DocTable *t, t_docId docId) {
  if (!docId || docId > t->maxDocId) {
    return 0;
  }
  uint32_t ix = DocTable_GetBucket(t, docId);
  const DMDChain *chain = t->buckets + ix;
  if (chain == NULL) {
    return 0;
  }
  for (const RSDocumentMetadata *md = chain->first; md; md = md->next) {
    if (md->id == docId && !(md->flags & Document_Deleted)) {
      return 1;
    }
  }
  return 0;
}

RSDocumentMetadata *DocTable_GetByKeyR(const DocTable *t, RedisModuleString *s) {
  RSDocumentKey k;
  k.str = RedisModule_StringPtrLen(s, &k.len);
  t_docId id = DocTable_GetId(t, k);
  return DocTable_Get(t, id);
}

static inline void DocTable_Set(DocTable *t, t_docId docId, RSDocumentMetadata *dmd) {
  uint32_t bucket = DocTable_GetBucket(t, docId);
  if (bucket >= t->cap && t->cap < t->maxSize) {
    /* We have to grow the array capacity.
     * We only grow till we reach maxSize, then we starts to add the dmds to
     * the already existing chains.
     */
    size_t oldcap = t->cap;
    // We grow by half of the current capacity with maximum of 1m
    t->cap += 1 + (t->cap ? MIN(t->cap / 2, 1024 * 1024) : 1);
    t->cap = MIN(t->cap, t->maxSize);  // make sure we do not excised maxSize
    t->cap = MAX(t->cap, bucket + 1);  // docs[bucket] needs to be valid, so t->cap > bucket

    t->buckets = rm_realloc(t->buckets, t->cap * sizeof(DMDChain));
    for (; oldcap < t->cap; oldcap++) {
      t->buckets[oldcap].first = NULL;
      t->buckets[oldcap].last = NULL;
    }
  }

  DMDChain *chain = &t->buckets[bucket];
  DMD_Incref(dmd);

  // Adding the dmd to the chain
  if (DMDChain_IsEmpty(chain)) {
    chain->first = chain->last = dmd;
  } else {
    chain->last->next = dmd;
    dmd->prev = chain->last;
    dmd->next = NULL;
    chain->last = dmd;
  }
}

/** Get the docId of a key if it exists in the table, or 0 if it doesnt */
t_docId DocTable_GetId(const DocTable *dt, RSDocumentKey key) {
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
 * vector. Returns 1 on success, 0 if the document does not exist. No further validation is done
 */
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
    dmd->sortVector = NULL;
    dmd->flags &= ~Document_HasSortVector;
    return 1;
  }

  /* Set th new vector and the flags accordingly */
  dmd->sortVector = v;
  dmd->flags |= Document_HasSortVector;
  t->sortablesSize += RSSortingVector_GetMemorySize(v);

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

  RSDocumentMetadata *dmd = rm_calloc(1, sizeof(RSDocumentMetadata));
  dmd->keyPtr = keyPtr;
  dmd->score = score;
  dmd->flags = flags;
  dmd->payload = dpl;
  dmd->maxFreq = 1;
  dmd->id = docId;
  dmd->sortVector = NULL;

  DocTable_Set(t, docId, dmd);
  ++t->size;
  t->memsize += sizeof(RSDocumentMetadata) + sdsAllocSize(keyPtr);
  DocIdMap_Put(&t->dim, key, docId);
  return docId;
}

RSPayload *DocTable_GetPayload(DocTable *t, t_docId docId) {
  RSDocumentMetadata *dmd = DocTable_Get(t, docId);
  return dmd ? dmd->payload : NULL;
}

/* Get the "real" external key for an incremental id. Returns NULL if docId is not in the table.
 */
inline RSDocumentKey DocTable_GetKey(DocTable *t, t_docId docId) {
  RSDocumentMetadata *dmd = DocTable_Get(t, docId);
  if (!dmd) {
    return MakeDocKey(NULL, 0);
  }
  return MakeDocKey(dmd->keyPtr, sdslen(dmd->keyPtr));
}

/* Get the score for a document from the table. Returns 0 if docId is not in the table. */
inline float DocTable_GetScore(DocTable *t, t_docId docId) {
  RSDocumentMetadata *dmd = DocTable_Get(t, docId);
  return dmd ? dmd->score : 0;
}

void DMD_Free(RSDocumentMetadata *md) {
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
  rm_free(md);
}

void DocTable_Free(DocTable *t) {
  for (int i = 0; i < t->cap; ++i) {
    DMDChain *chain = &t->buckets[i];
    if (DMDChain_IsEmpty(chain)) {
      continue;
    }
    RSDocumentMetadata *md = chain->first;
    while (md) {
      RSDocumentMetadata *next = md->next;
      DMD_Free(md);
      md = next;
    }
  }
  rm_free(t->buckets);
  DocIdMap_Free(&t->dim);
}

static void DocTable_DmdUnchain(DocTable *t, RSDocumentMetadata *md) {
  uint32_t bucketIndex = DocTable_GetBucket(t, md->id);
  DMDChain *dmdChain = &t->buckets[bucketIndex];

  if (dmdChain->first == md) {
    dmdChain->first = md->next;
  }

  if (dmdChain->last == md) {
    dmdChain->last = md->prev;
  }

  if (md->prev) {
    md->prev->next = md->next;
  }
  if (md->next) {
    md->next->prev = md->prev;
  }
  md->next = NULL;
  md->prev = NULL;
}

int DocTable_Delete(DocTable *t, RSDocumentKey key) {
  RSDocumentMetadata *md = DocTable_Pop(t, key);
  if (md) {
    DMD_Decref(md);
    return 1;
  }
  return 0;
}

RSDocumentMetadata *DocTable_Pop(DocTable *t, RSDocumentKey key) {
  t_docId docId = DocIdMap_Get(&t->dim, key);
  if (docId && docId <= t->maxDocId) {

    RSDocumentMetadata *md = DocTable_Get(t, docId);
    if (!md) {
      return NULL;
    }

    md->flags |= Document_Deleted;

    DocTable_DmdUnchain(t, md);
    DocIdMap_Delete(&t->dim, key);
    --t->size;

    return md;
  }
  return NULL;
}

void DocTable_RdbSave(DocTable *t, RedisModuleIO *rdb) {

  RedisModule_SaveUnsigned(rdb, t->size);
  RedisModule_SaveUnsigned(rdb, t->maxDocId);
  RedisModule_SaveUnsigned(rdb, t->maxSize);

  uint32_t elements_written = 0;
  for (uint32_t i = 0; i < t->cap; ++i) {
    if (DMDChain_IsEmpty(&t->buckets[i])) {
      continue;
    }
    RSDocumentMetadata *dmd = t->buckets[i].first;
    while (dmd) {

      RedisModule_SaveStringBuffer(rdb, dmd->keyPtr, sdslen(dmd->keyPtr));
      RedisModule_SaveUnsigned(rdb, dmd->id);
      RedisModule_SaveUnsigned(rdb, dmd->flags);
      RedisModule_SaveUnsigned(rdb, dmd->maxFreq);
      RedisModule_SaveUnsigned(rdb, dmd->len);
      RedisModule_SaveFloat(rdb, dmd->score);
      if (dmd->flags & Document_HasPayload) {
        if (dmd->payload) {
          // save an extra space for the null terminator to make the payload null terminated on
          RedisModule_SaveStringBuffer(rdb, dmd->payload->data, dmd->payload->len + 1);
        } else {
          RedisModule_SaveStringBuffer(rdb, "", 1);
        }
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
      ++elements_written;
      dmd = dmd->next;
    }
  }
  assert(elements_written + 1 == t->size);
}

void DocTable_RdbLoad(DocTable *t, RedisModuleIO *rdb, int encver) {
  t->size = RedisModule_LoadUnsigned(rdb);
  t->maxDocId = RedisModule_LoadUnsigned(rdb);
  if (encver >= INDEX_MIN_COMPACTED_DOCTABLE_VERSION) {
    t->maxSize = RedisModule_LoadUnsigned(rdb);
  } else {
    t->maxSize = MIN(RSGlobalConfig.maxDocTableSize, t->maxDocId);
  }

  for (size_t i = 1; i < t->size; i++) {
    size_t len;

    RSDocumentMetadata *dmd = rm_calloc(1, sizeof(RSDocumentMetadata));
    char *tmpPtr = RedisModule_LoadStringBuffer(rdb, &len);
    if (encver < INDEX_MIN_BINKEYS_VERSION) {
      // Previous versions would encode the NUL byte
      len--;
    }
    dmd->id = encver < INDEX_MIN_COMPACTED_DOCTABLE_VERSION ? i : RedisModule_LoadUnsigned(rdb);
    dmd->keyPtr = sdsnewlen(tmpPtr, len);
    rm_free(tmpPtr);

    dmd->flags = RedisModule_LoadUnsigned(rdb);
    dmd->maxFreq = 1;
    dmd->len = 1;
    if (encver > 1) {
      dmd->maxFreq = RedisModule_LoadUnsigned(rdb);
    }
    if (encver >= INDEX_MIN_DOCLEN_VERSION) {
      dmd->len = RedisModule_LoadUnsigned(rdb);
    } else {
      // In older versions, default the len to max freq to avoid division by zero.
      dmd->len = dmd->maxFreq;
    }

    dmd->score = RedisModule_LoadFloat(rdb);
    dmd->payload = NULL;
    // read payload if set
    if (dmd->flags & Document_HasPayload) {
      dmd->payload = RedisModule_Alloc(sizeof(RSPayload));
      dmd->payload->data = RedisModule_LoadStringBuffer(rdb, &dmd->payload->len);
      dmd->payload->len--;
      t->memsize += dmd->payload->len + sizeof(RSPayload);
    }
    dmd->sortVector = NULL;
    if (dmd->flags & Document_HasSortVector) {
      dmd->sortVector = SortingVector_RdbLoad(rdb, encver);
      t->sortablesSize += RSSortingVector_GetMemorySize(dmd->sortVector);
    }

    if (dmd->flags & Document_HasOffsetVector) {
      size_t nTmp = 0;
      char *tmp = RedisModule_LoadStringBuffer(rdb, &nTmp);
      Buffer *bufTmp = Buffer_Wrap(tmp, nTmp);
      dmd->byteOffsets = LoadByteOffsets(bufTmp);
      free(bufTmp);
      rm_free(tmp);
    }

    // We always save deleted docs to rdb, but we don't want to load them back to the id map
    if (!(dmd->flags & Document_Deleted)) {
      DocIdMap_Put(&t->dim, MakeDocKey(dmd->keyPtr, sdslen(dmd->keyPtr)), dmd->id);
    }
    DocTable_Set(t, dmd->id, dmd);
    t->memsize += sizeof(RSDocumentMetadata) + len;
  }
}

DocIdMap NewDocIdMap() {

  TrieMap *m = NewTrieMap();
  return (DocIdMap){m};
}

t_docId DocIdMap_Get(const DocIdMap *m, RSDocumentKey key) {

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
  return TrieMap_Delete(m->tm, (char *)key.str, key.len, rm_free);
}
