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
#include "fnv_ffi.h"
#include "sortable.h"
#include "sorting_vector_ffi.h"
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
  for (RSDocumentMetadata *dmd = dmdChain->root; dmd != NULL; dmd = dmd->nextInChain) {
    if (dmd->id == docId) {
      return (dmd->flags & Document_Deleted) ? NULL : dmd;
    }
  }
  return NULL;
}

// Like `DocTable_GetOwn` but also removes the dmd from the doc table
static RSDocumentMetadata *DocTable_DmdUnchain(DocTable *t, t_docId docId) {
  if (!DocTable_ValidateDocId(t, docId)) {
    return NULL;
  }
  uint32_t bucketIndex = DocTable_GetBucket(t, docId);
  if (bucketIndex >= t->cap) {
    return NULL;
  }
  DMDChain *dmdChain = &t->buckets[bucketIndex];
  RSDocumentMetadata **prev_next = &dmdChain->root;
  for (RSDocumentMetadata *md = dmdChain->root; md != NULL; md = md->nextInChain) {
    if (md->id == docId) {
      *prev_next = md->nextInChain;
      md->nextInChain = NULL;
      return md;
    }
    prev_next = &md->nextInChain;
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
    return false;
  }
  uint32_t ix = DocTable_GetBucket(t, docId);
  if (ix >= t->cap) {
    return false;
  }
  const DMDChain *chain = t->buckets + ix;
  if (chain == NULL) {
    return false;
  }
  for (const RSDocumentMetadata *md = chain->root; md != NULL; md = md->nextInChain) {
    if (md->id == docId) {
      return !(md->flags & Document_Deleted);
    }
  }
  return false;
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
  dmd->nextInChain = chain->root;
  chain->root = dmd;
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

/* Set the sorting vector for a document. If the vector is empty we mark the doc as not having a
 * vector. Returns 1 on success, 0 if the document does not exist. No further validation is done
 */
int DocTable_SetSortingVector(DocTable *t, RSDocumentMetadata *dmd, RSSortingVector v) {
  if (!dmd) {
    return 0;
  }

  RS_LOG_ASSERT(RSSortingVector_Length(&v), "Sorting vector does not exist");  // tested in doAssignIds()

  /* Set the new vector and the flags accordingly */
  dmd->sortVector = v;
  dmd->flags |= Document_HasSortVector;
  t->sortablesSize += RSSortingVector_GetMemorySize(&dmd->sortVector);

  return 1;
}

void DocTable_SetByteOffsets(RSDocumentMetadata *dmd, RSByteOffsets *v) {
  if (!dmd) {
    return;
  }

  dmd->byteOffsets = v;
  dmd->flags |= Document_HasOffsetVector;
}

// Pack a t_expirationTimePoint into nanoseconds since the epoch, preserving
// the {0,0} "no expiration" sentinel as 0 so callers can use a single scalar
// compare on the result-processor hot path.
//
// `t_expirationTimePoint` is a POSIX `struct timespec` (IEEE Std 1003.1), which
// is the OS-level resolution ceiling: `tv_nsec` is in [0, 999999999], and any
// finer-grained clock would require a different type. `int64_t` of nanoseconds
// covers ~292 years from the epoch (year 2262), far beyond any TTL Redis can
// produce — `RM_GetAbsExpire` and `RM_HashFieldMinExpire` both return an
// `mstime_t` (signed milliseconds since epoch), which we expand into a
// `timespec` in `document_basic.c::timespecFromMilliseconds` before reaching
// here. The debug assert traps any future caller that violates the timespec
// invariant.
static inline int64_t expirationTimePointToNs(t_expirationTimePoint t) {
  RS_LOG_ASSERT(t.tv_nsec >= 0 && t.tv_nsec < 1000000000L,
                "tv_nsec out of POSIX timespec range");
  return (int64_t)t.tv_sec * 1000000000LL + (int64_t)t.tv_nsec;
}

void DocTable_SetDocExpiration(RSDocumentMetadata *dmd, t_expirationTimePoint ttl) {
  __atomic_store_n(&dmd->expirationTimeNs, expirationTimePointToNs(ttl), __ATOMIC_RELAXED);
}

// Sets the doc-level TTL on the DMD and delegates the per-field entry to
// DocTable_UpdateFieldExpiration. The doc-level TTL is inlined on the DMD so
// the result-processor can skip the TTL-table lookup; the table itself stays
// strictly an HFE store, which lets iterators use `t->ttl == NULL` as their
// per-spec gate. Takes ownership of `sortedFieldWithExpiration`.
void DocTable_UpdateExpiration(DocTable *t, RSDocumentMetadata* dmd, t_expirationTimePoint ttl, FieldExpirations sortedFieldWithExpiration) {
  DocTable_SetDocExpiration(dmd, ttl);
  DocTable_UpdateFieldExpiration(t, dmd, sortedFieldWithExpiration);
}

void DocTable_UpdateFieldExpiration(DocTable *t, RSDocumentMetadata *dmd,
                                    FieldExpirations sortedFieldWithExpiration) {
  // Drop any prior entry before reinserting; TimeToLiveTable_Add asserts on
  // duplicate ids. Remove is a no-op when the docId is not registered.
  if (t->ttl) {
    TimeToLiveTable_Remove(t->ttl, dmd->id);
  }
  if (FieldExpirations_Len(&sortedFieldWithExpiration) > 0) {
    TimeToLiveTable_VerifyInit(&t->ttl, t->maxSize);
    TimeToLiveTable_Add(t->ttl, dmd->id, sortedFieldWithExpiration);
  } else {
    FieldExpirations_Free(&sortedFieldWithExpiration);
    if (t->ttl && TimeToLiveTable_IsEmpty(t->ttl)) {
      TimeToLiveTable_Destroy(&t->ttl);
    }
  }
}

bool DocTable_IsDocExpired(DocTable* t, const RSDocumentMetadata* dmd, struct timespec* expirationPoint) {
  // Relaxed atomic load: pairs with the relaxed store in DocTable_UpdateExpiration
  // so reader/writer concurrency under the spec read lock is well-defined under
  // the C abstract machine. Ordering relative to other index mutations is not
  // required — the value is a self-contained timestamp compared against `now`.
  int64_t exp = __atomic_load_n(&dmd->expirationTimeNs, __ATOMIC_RELAXED);
  if (exp == 0) {
    return false;
  }
  return exp <= expirationTimePointToNs(*expirationPoint);
}

void DocTable_ClearExpirationData(DocTable *t) {
  // Walk every DMD: doc-level TTL lives inline on the DMD (not in the TTL
  // table), and field-level TTL is only present for docs that are also in
  // the table. Either may be set, so a single sweep over all DMDs is the
  // simplest correct path. Caller holds the write lock (see header), but use
  // the relaxed atomic store to match the access pattern everywhere else.
  DOCTABLE_FOREACH(t, __atomic_store_n(&dmd->expirationTimeNs, 0, __ATOMIC_RELAXED));
  TimeToLiveTable_Destroy(&t->ttl);
}

/* Put a new document into the table, assign it a fresh incremental id and store the
 * metadata in the table. The key -> docId mapping is published separately by the caller
 * via DocIdMeta (key metadata); this function only allocates the docId and the DMD.
 *
 * Callers are responsible for detecting an existing document (via DocIdMeta) before
 * calling this: there is no key-based deduplication here, so calling it twice for the
 * same key produces two DMDs with distinct docIds. */
RSDocumentMetadata *DocTable_Put(DocTable *t, const char *s, size_t n, double score, RSDocumentFlags flags,
                                 const char *payload, size_t payloadSize, DocumentType type) {

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
  dmd->maxTermFreq = 1;
  dmd->id = docId;
  dmd->sortVector = RSSortingVector_Empty();
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
  if (RSSortingVector_Length(&md->sortVector)) {
    RSSortingVector_ClearAndDeAlloc(&md->sortVector);
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
    RSDocumentMetadata *md, *next;
    md = chain->root;
    while (md) {
      next = md->nextInChain;
      DMD_Return(md);
      md = next;
    }
  }
  rm_free(t->buckets);
  TimeToLiveTable_Destroy(&t->ttl);
}

// Remove a document from the table by its internal docId, for the unified delete
// path driven by the DocIdMeta `unlink` callback. Unchains the DMD, marks it deleted,
// drops its per-field TTL entry and updates size/memory accounting. Ownership of
// the returned DMD is moved to the caller (ref count unchanged); returns NULL if
// the docId is not present.
RSDocumentMetadata *DocTable_DeleteById(DocTable *t, t_docId docId) {
  if (!docId || docId > t->maxDocId) {
    return NULL;
  }

  RSDocumentMetadata *md = DocTable_DmdUnchain(t, docId);
  if (!md) {
    return NULL;
  }

  // Drop the doc's per-field TTL entry, if any, and tear down the table
  // once the last entry is gone so iterators can use the NULL gate again.
  if (t->ttl) {
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
  if (RSSortingVector_Length(&md->sortVector)) {
    t->sortablesSize -= RSSortingVector_GetMemorySize(&md->sortVector);
  }

  --t->size;
  // Move ownership of the metadata to the caller, without changing the ref count
  return md;
}

// Not thread safe. Assumes the caller has locked the spec for write.
// Update the stored key of an existing document (addressed by docId) after a
// RENAME. The key -> docId mapping is not touched here: it rides with the Redis
// key metadata (DocIdMeta) across the rename.
void DocTable_SetKeyById(DocTable *t, t_docId docId, const char *to_str, size_t to_len) {
  RSDocumentMetadata *dmd = DocTable_GetOwn(t, docId);
  if (!dmd) {
    return;
  }
  t->memsize -= sdsAllocSize(dmd->keyPtr);
  sdsfree(dmd->keyPtr);
  dmd->keyPtr = sdsnewlen(to_str, to_len);
  t->memsize += sdsAllocSize(dmd->keyPtr);
}

int DocTable_LegacyRdbLoad(DocTable *t, RedisModuleIO *rdb, int encver) {
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
    t->buckets = NULL;
    size_t alloc_size;
    if (__builtin_mul_overflow(t->cap, sizeof(DMDChain), &alloc_size)) {
      RedisModule_LogIOError(rdb, "warning", "DocTable_LegacyRdbLoad: allocation overflow");
      t->cap = 0;
      return REDISMODULE_ERR;
    }
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
    dmd->maxTermFreq = 1;
    dmd->docLen = 1;
    if (encver > 1) {
      dmd->maxTermFreq = RedisModule_LoadUnsigned(rdb);
    }
    if (encver >= INDEX_MIN_DOCLEN_VERSION) {
      dmd->docLen = RedisModule_LoadUnsigned(rdb);
    } else {
      // In older versions, default the docLen to maxTermFreq to avoid division by zero.
      dmd->docLen = dmd->maxTermFreq;
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
    dmd->sortVector = RSSortingVector_Empty();
    if (dmd->flags & Document_HasSortVector) {
      dmd->sortVector = SortingVector_RdbLoad(rdb);
      t->sortablesSize += RSSortingVector_GetMemorySize(&dmd->sortVector);
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
      // Legacy RDB load (memory mode only). The key -> docId mapping is not
      // rebuilt here: legacy indexes are re-scanned/re-indexed after the load
      // completes (Indexes_EndRDBLoadingEvent), which repopulates DocIdMeta.
      DocTable_Set(t, dmd->id, dmd);
      t->memsize += sizeof(RSDocumentMetadata) + len;
    }
  }
  t->size -= deletedElements;
  return REDISMODULE_OK;
}

t_docId DocTable_GetMaxDocId(const DocTable *t) {
  return t->maxDocId;
}
