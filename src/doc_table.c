/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "doc_table.h"
#include <sys/param.h>
#include <string.h>
#include <stdio.h>
#include "redismodule.h"
#include "util/fnv.h"
#include "triemap.h"
#include "sortable.h"
#include "rmalloc.h"
#include "spec.h"
#include "config.h"

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap, size_t max_size) {
  DocTable ret = {
      .size = 1,
      .cap = cap,
      .maxDocId = 0,
      .memsize = 0,
      .sortablesSize = 0,
      .maxSize = max_size,
      .dim = NewDocIdMap(),
  };
  ret.buckets = rm_calloc(cap, sizeof(*ret.buckets));
  ret.memsize = cap * sizeof(*ret.buckets) + sizeof(DocTable);
  return ret;
}

static inline uint32_t DocTable_GetBucket(const DocTable *t, t_docId docId) {
  return docId < t->maxSize ? docId : docId % t->maxSize;
}

static inline int DocTable_ValidateDocId(const DocTable *t, t_docId docId) {
  return docId != 0 && docId <= t->maxDocId;
}

static RSDocumentMetadata *DocTable_GetOwn(const DocTable *t, t_docId docId) {
  if (!DocTable_ValidateDocId(t, docId)) {
    return NULL;
  }
  uint32_t bucketIndex = DocTable_GetBucket(t, docId);
  if (bucketIndex >= t->cap) {
    return NULL;
  }
  // While we iterate over the chain, we have locked the index spec (R/W), so we either a writer alone or
  // multiple readers. In any case, we can safely iterate over the chain without a lock and
  // increment the ref count of the document metadata when we find it.
  DMDChain *dmdChain = &t->buckets[bucketIndex];
  DLLIST2_FOREACH(it, &dmdChain->lroot) {
    RSDocumentMetadata *dmd = DLLIST2_ITEM(it, RSDocumentMetadata, llnode);

    if (dmd->id == docId) {
      if (dmd->flags & Document_Deleted) {
        return NULL;
      }
      return dmd;
    }
  }
  return NULL;
}

const RSDocumentMetadata *DocTable_Borrow(const DocTable *t, t_docId docId) {
  RSDocumentMetadata *dmd = DocTable_GetOwn(t, docId);
  if (dmd) {
    DMD_Incref(dmd);
  }
  return dmd;
}

bool DocTable_Exists(const DocTable *t, t_docId docId) {
  if (!docId || docId > t->maxDocId) {
    return 0;
  }
  uint32_t ix = DocTable_GetBucket(t, docId);
  if (ix >= t->cap) {
    return 0;
  }
  const DMDChain *chain = t->buckets + ix;
  if (chain == NULL) {
    return 0;
  }
  DLLIST2_FOREACH(it, &chain->lroot) {
    const RSDocumentMetadata *md = DLLIST2_ITEM(it, RSDocumentMetadata, llnode);
    if (md->id == docId) {
      return !(md->flags & Document_Deleted);
    }
  }
  return 0;
}

const RSDocumentMetadata *DocTable_BorrowByKeyR(const DocTable *t, RedisModuleString *s) {
  const char *kstr;
  size_t klen;
  kstr = RedisModule_StringPtrLen(s, &klen);
  t_docId id = DocTable_GetId(t, kstr, klen);
  return DocTable_Borrow(t, id);
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
    t->memsize += (t->cap - oldcap) * sizeof(DMDChain);

    // We clear new extra allocation to Null all list pointers
    size_t memsetSize = (t->cap - oldcap) * sizeof(DMDChain);

    // Log DocTable capacity growth to help diagnose cases where a small number of documents
    // combined with frequent updates cause disproportionate memory usage.
    // This allows us to confirm if unexpected memory spikes are due to capacity increases.
    // Note: We do not shrink the DocTable to avoid the cost of rehashing.
    // To adjust its size, lower the search-max-doctablesize configuration value.
    RedisModule_Log(RSDummyContext, "notice", "DocTable capacity increase from %zu to %zu", oldcap, t->cap);

    memset(&t->buckets[oldcap], 0, memsetSize);
  }

  DMDChain *chain = &t->buckets[bucket];
  dmd->ref_count = 1; // Index reference

  // Adding the dmd to the chain
  dllist2_append(&chain->lroot, &dmd->llnode);
}

/** Get the docId of a key if it exists in the table, or 0 if it doesn't */
t_docId DocTable_GetId(const DocTable *dt, const char *s, size_t n) {
  return DocIdMap_Get(&dt->dim, s, n);
}

/* Set the payload for a document. Returns 1 if we set the payload, 0 if we couldn't find the
 * document */
int DocTable_SetPayload(DocTable *t, RSDocumentMetadata *dmd, const char *data, size_t len) {
  /* Get the metadata */
  if (!dmd || !data) {
    return 0;
  }

  /* If we already have metadata - clean up the old data */
  if (hasPayload(dmd->flags)) {
    /* Free the old payload */
    if (dmd->payload->data) {
      rm_free((void *)dmd->payload->data);
    }
    t->memsize -= dmd->payload->len;
  } else {
    dmd->payload = rm_malloc(sizeof(RSPayload));
    t->memsize += sizeof(RSPayload);
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
int DocTable_SetSortingVector(DocTable *t, RSDocumentMetadata *dmd, RSSortingVector *v) {
  if (!dmd) {
    return 0;
  }

  // LCOV_EXCL_START
  /* Null vector means remove the current vector if it exists */
  /*if (!v) {
    if (dmd->sortVector) {
      SortingVector_Free(dmd->sortVector);
    }
    dmd->sortVector = NULL;
    dmd->flags &= ~Document_HasSortVector;
    return 1;
  }*/
  // LCOV_EXCL_STOP
  RS_LOG_ASSERT(v, "Sorting vector does not exist");  // tested in doAssignIds()

  /* Set th new vector and the flags accordingly */
  dmd->sortVector = v;
  dmd->flags |= Document_HasSortVector;
  t->sortablesSize += RSSortingVector_GetMemorySize(v);

  return 1;
}

void DocTable_SetByteOffsets(RSDocumentMetadata *dmd, RSByteOffsets *v) {
  if (!dmd) {
    return;
  }

  dmd->byteOffsets = v;
  dmd->flags |= Document_HasOffsetVector;
}

void DocTable_UpdateExpiration(DocTable *t, RSDocumentMetadata* dmd, t_expirationTimePoint ttl, arrayof(FieldExpiration) sortedFieldWithExpiration) {
  if (hasExpirationTimeInformation(dmd->flags)) {
    TimeToLiveTable_VerifyInit(&t->ttl);
    TimeToLiveTable_Add(t->ttl, dmd->id, ttl, sortedFieldWithExpiration);
  }
}

bool DocTable_IsDocExpired(DocTable* t, const RSDocumentMetadata* dmd, struct timespec* expirationPoint) {
  if (!hasExpirationTimeInformation(dmd->flags)) {
      return false;
  }
  RS_LOG_ASSERT(t->ttl, "Document has expiration time information but no TTL table");
  return TimeToLiveTable_HasDocExpired(t->ttl, dmd->id, expirationPoint);
}

/* Put a new document into the table, assign it an incremental id and store the metadata in the
 * table.
 *
 * Return 0 if the document is already in the index  */
RSDocumentMetadata *DocTable_Put(DocTable *t, const char *s, size_t n, double score, RSDocumentFlags flags,
                                 const char *payload, size_t payloadSize, DocumentType type) {

  t_docId xid = DocIdMap_Get(&t->dim, s, n);
  // if the document is already in the index, return 0
  if (xid) {
    return (RSDocumentMetadata *)DocTable_Borrow(t, xid);
  }
  t_docId docId = ++t->maxDocId;

  RSDocumentMetadata *dmd;
  if (payload && payloadSize) {
    dmd = rm_calloc(1, sizeof(*dmd));
    flags |= Document_HasPayload;
    t->memsize += sizeof(RSDocumentMetadata);
  } else {
    size_t leanSize = sizeof(*dmd) - sizeof(RSPayload *);
    dmd = rm_calloc(1, leanSize);
    t->memsize += leanSize;
  }

  sds keyPtr = sdsnewlen(s, n);
  dmd->keyPtr = keyPtr;
  dmd->score = score;
  dmd->flags = flags;
  dmd->maxFreq = 1;
  dmd->id = docId;
  dmd->sortVector = NULL;
  dmd->type = type;

  if (hasPayload(flags)) {
    /* Copy the payload since it's probably an input string not retained */
    RSPayload *dpl = rm_malloc(sizeof(RSPayload));
    dpl->data = rm_calloc(1, payloadSize + 1);
    memcpy(dpl->data, payload, payloadSize);
    dpl->len = payloadSize;
    t->memsize += payloadSize + sizeof(RSPayload);

    dmd->payload = dpl;
  }

  DocTable_Set(t, docId, dmd);
  ++t->size;
  t->memsize += sdsAllocSize(keyPtr);
  DocIdMap_Put(&t->dim, s, n, docId);
  DMD_Incref(dmd); // Reference for the caller
  return dmd;
}

/*
 * Get the "real" external key for an incremental id. Returns NULL if docId is not in the table.
 * The returned string is allocated on the heap and must be freed by the caller.
 */
sds DocTable_GetKey(const DocTable *t, t_docId docId, size_t *lenp) {
  size_t len_s = 0;
  if (!lenp) {
    lenp = &len_s;
  }

  const RSDocumentMetadata *dmd = DocTable_Borrow(t, docId);
  if (!dmd) {
    *lenp = 0;
    return NULL;
  }
  sds key = sdsdup(dmd->keyPtr);
  DMD_Return(dmd);
  *lenp = sdslen(key);
  return key;
}

void DMD_Free(const RSDocumentMetadata *cmd) {
  RSDocumentMetadata * md = (RSDocumentMetadata *)cmd;
  if (hasPayload(md->flags)) {
    rm_free(md->payload->data);
    rm_free(md->payload);
    md->flags &= ~Document_HasPayload;
    md->payload = NULL;
  }
  if (md->sortVector) {
    RSSortingVector_Free(md->sortVector);
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
    if (DLLIST2_IS_EMPTY(&chain->lroot)) {
      continue;
    }
    DLLIST2_node *nn = chain->lroot.head;
    while (nn) {
      RSDocumentMetadata *md = DLLIST2_ITEM(nn, RSDocumentMetadata, llnode);
      nn = nn->next;
      DMD_Return(md);
    }
  }
  rm_free(t->buckets);
  TimeToLiveTable_Destroy(&t->ttl);
  DocIdMap_Free(&t->dim);
}

static void DocTable_DmdUnchain(DocTable *t, RSDocumentMetadata *md) {
  uint32_t bucketIndex = DocTable_GetBucket(t, md->id);
  DMDChain *dmdChain = &t->buckets[bucketIndex];
  dllist2_delete(&dmdChain->lroot, &md->llnode);
}

RSDocumentMetadata *DocTable_Pop(DocTable *t, const char *s, size_t n) {
  t_docId docId = DocIdMap_Get(&t->dim, s, n);

  if (docId && docId <= t->maxDocId) {

    RSDocumentMetadata *md = (RSDocumentMetadata *)DocTable_Borrow(t, docId);
    if (!md) {
      return NULL;
    }

    if (t->ttl && hasExpirationTimeInformation(md->flags)) {
      TimeToLiveTable_Remove(t->ttl, md->id);
      if (TimeToLiveTable_IsEmpty(t->ttl)) {
        TimeToLiveTable_Destroy(&t->ttl);
      }
    }

    // Assuming we already locked the spec for write, and we don't have multiple writers,
    // all the next operations don't need to be atomic
    md->flags |= Document_Deleted;

    t->memsize -= sdsAllocSize(md->keyPtr);
    if (!hasPayload(md->flags)) {
      t->memsize -= sizeof(RSDocumentMetadata) - sizeof(RSPayload *);
    } else {
      t->memsize -= sizeof(RSDocumentMetadata);
      t->memsize -= md->payload->len + sizeof(RSPayload);
    }
    if (md->sortVector) {
      t->sortablesSize -= RSSortingVector_GetMemorySize(md->sortVector);
    }

    DocTable_DmdUnchain(t, md);
    DocIdMap_Delete(&t->dim, s, n);
    --t->size;
    DMD_Return(md); // Return the ref from the `Borrow` call. The caller needs to release the index ref.

    return md;
  }
  return NULL;
}

// Not thread safe. Assumes the caller has locked the spec for write
int DocTable_Replace(DocTable *t, const char *from_str, size_t from_len, const char *to_str,
                     size_t to_len) {
  t_docId id = DocIdMap_Get(&t->dim, from_str, from_len);
  if (id == 0) {
    return REDISMODULE_ERR;
  }
  DocIdMap_Delete(&t->dim, from_str, from_len);
  DocIdMap_Put(&t->dim, to_str, to_len, id);
  RSDocumentMetadata *dmd = DocTable_GetOwn(t, id);
  t->memsize -= sdsAllocSize(dmd->keyPtr);
  sdsfree(dmd->keyPtr);
  dmd->keyPtr = sdsnewlen(to_str, to_len);
  t->memsize += sdsAllocSize(dmd->keyPtr);
  return REDISMODULE_OK;
}

void DocTable_LegacyRdbLoad(DocTable *t, RedisModuleIO *rdb, int encver) {
  long long deletedElements = 0;
  t->size = RedisModule_LoadUnsigned(rdb);
  t->maxDocId = RedisModule_LoadUnsigned(rdb);
  if (encver >= INDEX_MIN_COMPACTED_DOCTABLE_VERSION) {
    t->maxSize = RedisModule_LoadUnsigned(rdb);
  } else {
    t->maxSize = MIN(RSGlobalConfig.maxDocTableSize, t->maxDocId);
  }

  if (t->maxDocId > t->maxSize) {
    /**
     * If the maximum doc id is greater than the maximum cap size
     * then it means there is a possibility that any index under maxId can
     * be accessed. However, it is possible that this bucket does not have
     * any documents inside it (and thus might not be populated below), but
     * could still be accessed for simple queries (e.g. get, exist). Ensure
     * we don't have to rely on Set/Put to ensure the doc table array.
     */
    t->memsize -= t->cap * sizeof(DMDChain);
    t->cap = t->maxSize;
    rm_free(t->buckets);
    t->buckets = rm_calloc(t->cap, sizeof(*t->buckets));
    t->memsize += t->cap * sizeof(DMDChain);
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
    RedisModule_Free(tmpPtr);

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
    // read payload if set
    if (hasPayload(dmd->flags)) {
      dmd->payload = NULL;
      if (!(dmd->flags & Document_Deleted)) {
        dmd->payload = rm_malloc(sizeof(RSPayload));
        dmd->payload->data = RedisModule_LoadStringBuffer(rdb, &dmd->payload->len);
        char *buf = rm_malloc(dmd->payload->len);
        memcpy(buf, dmd->payload->data, dmd->payload->len);
        RedisModule_Free(dmd->payload->data);
        dmd->payload->data = buf;
        dmd->payload->len--;
        t->memsize += dmd->payload->len + sizeof(RSPayload);
      } else if ((dmd->flags & Document_Deleted) && (encver == INDEX_MIN_EXPIRE_VERSION)) {
        RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL));  // throw this string to garbage
      }
    }
    dmd->sortVector = NULL;
    if (dmd->flags & Document_HasSortVector) {
      dmd->sortVector = SortingVector_RdbLoad(rdb);
      t->sortablesSize += RSSortingVector_GetMemorySize(dmd->sortVector);
    }

    if (dmd->flags & Document_HasOffsetVector) {
      size_t nTmp = 0;
      char *tmp = RedisModule_LoadStringBuffer(rdb, &nTmp);
      Buffer *bufTmp = Buffer_Wrap(tmp, nTmp);
      dmd->byteOffsets = LoadByteOffsets(bufTmp);
      rm_free(bufTmp);
      RedisModule_Free(tmp);
    }

    if (dmd->flags & Document_Deleted) {
      ++deletedElements;
      DMD_Free(dmd);
    } else {
      DocIdMap_Put(&t->dim, dmd->keyPtr, sdslen(dmd->keyPtr), dmd->id);
      DocTable_Set(t, dmd->id, dmd);
      t->memsize += sizeof(RSDocumentMetadata) + len;
    }
  }
  t->size -= deletedElements;
}

DocIdMap NewDocIdMap() {

  TrieMap *m = NewTrieMap();
  return (DocIdMap){m};
}

t_docId DocIdMap_Get(const DocIdMap *m, const char *s, size_t n) {

  void *val = TrieMap_Find(m->tm, (char *)s, n);
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

void DocIdMap_Put(DocIdMap *m, const char *s, size_t n, t_docId docId) {

  t_docId *pd = rm_malloc(sizeof(t_docId));
  *pd = docId;
  TrieMap_Add(m->tm, s, n, pd, _docIdMap_replace);
}

void DocIdMap_Free(DocIdMap *m) {
  TrieMap_Free(m->tm, rm_free);
}

int DocIdMap_Delete(DocIdMap *m, const char *s, size_t n) {
  return TrieMap_Delete(m->tm, s, n, rm_free);
}
