#include "doc_table.h"
#include "redismodule.h"
#include "sortable.h"
#include "rmalloc.h"
#include "spec.h"
#include "config.h"

#include "util/fnv.h"
#include "triemap/triemap.h"
#include "rmutil/rm_assert.h"

#include <sys/param.h>
#include <string.h>
#include <stdio.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Creates a new DocTable with a given capacity
void DocTable::ctor(size_t cap, size_t max_size) {
  size = 1;
  maxSize = max_size;
  maxDocId = 0;
  cap = cap;
  memsize = 0;
  sortablesSize = 0;
  buckets = NULL;
}

//---------------------------------------------------------------------------------------------

inline uint32_t DocTable::GetBucket(t_docId docId) const {
  return docId < axSize ? docId : docId % maxSize;
}

//---------------------------------------------------------------------------------------------

inline int DocTable::ValidateDocId(t_docId docId) const {
  return docId != 0 && docId <= maxDocId;
}

//---------------------------------------------------------------------------------------------

// Get the metadata for a doc Id from the DocTable. If docId is not inside the table, we return NULL
RSDocumentMetadata *DocTable::Get(t_docId docId) const {
  if (!ValidateDocId(docId)) {
    return NULL;
  }
  uint32_t bucketIndex = GetBucket(docId);
  if (bucketIndex >= cap) {
    return NULL;
  }
  DMDChain *dmdChain = &buckets[bucketIndex];
  DLLIST2_FOREACH(it, &dmdChain->lroot) {
    RSDocumentMetadata *dmd = DLLIST2_ITEM(it, RSDocumentMetadata, llnode);
    if (dmd->id == docId) {
      return dmd;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

int DocTable::Exists(t_docId docId) const {
  if (!docId || docId > maxDocId) {
    return 0;
  }
  uint32_t ix = GetBucket(docId);
  if (ix >= cap) {
    return 0;
  }
  const DMDChain *chain = buckets + ix;
  if (chain == NULL) {
    return 0;
  }
  DLLIST2_FOREACH(it, &chain->lroot) {
    const RSDocumentMetadata *md = DLLIST2_ITEM(it, RSDocumentMetadata, llnode);
    if (md->id == docId && !(md->flags & Document_Deleted)) {
      return 1;
    }
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata *DocTable::GetByKeyR(RedisModuleString *s) const {
  const char *kstr;
  size_t klen;
  kstr = RedisModule_StringPtrLen(s, &klen);
  t_docId id = GetId(kstr, klen);
  return Get(id);
}

//---------------------------------------------------------------------------------------------

inline void DocTable::Set(t_docId docId, RSDocumentMetadata *dmd) {
  uint32_t bucket = GetBucket(docId);
  if (bucket >= cap && cap < maxSize) {
    /* We have to grow the array capacity.
     * We only grow till we reach maxSize, then we starts to add the dmds to
     * the already existing chains.
     */
    size_t oldcap = cap;
    // We grow by half of the current capacity with maximum of 1m
    cap += 1 + (cap ? MIN(cap / 2, 1024 * 1024) : 1);
    cap = MIN(cap, maxSize);  // make sure we do not excised maxSize
    cap = MAX(cap, bucket + 1);  // docs[bucket] needs to be valid, so cap > bucket
    buckets = rm_realloc(buckets, cap * sizeof(DMDChain));
    for (; oldcap < cap; oldcap++) {
      buckets[oldcap].lroot.head = NULL;
      buckets[oldcap].lroot.tail = NULL;
    }
  }

  DMDChain *chain = &buckets[bucket];
  DMD_Incref(dmd);

  // Adding the dmd to the chain
  dllist2_append(&chain->lroot, &dmd->llnode);
}

//---------------------------------------------------------------------------------------------

// Get the docId of a key if it exists in the table, or 0 if it doesnt
t_docId DocTable::GetId(const char *s, size_t n) const {
  return &dim->Get(s, n);
}

//---------------------------------------------------------------------------------------------

// Set payload for a document. Returns 1 if we set the payload, 0 if we couldn't find the document
int DocTable::SetPayload(t_docId docId, const char *data, size_t len) {
  /* Get the metadata */
  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd || !data) {
    return 0;
  }

  /* If we already have metadata - clean up the old data */
  if (dmd->payload) {
    /* Free the old payload */
    if (dmd->payload->data) {
      rm_free((void *)dmd->payload->data);
    }
    memsize -= dmd->payload->len;
  } else {
    dmd->payload = rm_malloc(sizeof(RSPayload));
  }
  /* Copy it... */
  dmd->payload->data = rm_calloc(1, len + 1);
  dmd->payload->len = len;
  memcpy(dmd->payload->data, data, len);

  dmd->flags |= Document_HasPayload;
  memsize += len;
  return 1;
}

//---------------------------------------------------------------------------------------------

// Set the sorting vector for a document. If the vector is NULL we mark the doc as not having a
// vector. Returns 1 on success, 0 if the document does not exist. No further validation is done.
int DocTable::SetSortingVector(t_docId docId, RSSortingVector *v) {
  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd) {
    return 0;
  }

  // LCOV_EXCL_START
  /* Null vector means remove the current vector if it exists */
  /*if (!v) {
    if (dmd->sortVector) {
      delete dmd->sortVector;
    }
    dmd->sortVector = NULL;
    dmd->flags &= ~Document_HasSortVector;
    return 1;
  }*/
  //LCOV_EXCL_STOP
  RS_LOG_ASSERT(v, "Sorting vector does not exist"); // tested in doAssignIds()

  /* Set th new vector and the flags accordingly */
  dmd->sortVector = v;
  dmd->flags |= Document_HasSortVector;
  sortablesSize += v->GetMemorySize();

  return 1;
}

//---------------------------------------------------------------------------------------------

// Set the offset vector for a document. This contains the byte offsets of each token found in
// the document. This is used for highlighting.
int DocTable::SetByteOffsets(t_docId docId, RSByteOffsets *v) {
  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd) {
    return 0;
  }

  dmd->byteOffsets = v;
  dmd->flags |= Document_HasOffsetVector;
  return 1;
}

//---------------------------------------------------------------------------------------------

/* Put a new document into the table, assign it an incremental id and store the metadata in the
 * table.
 * Return 0 if the document is already in the index.
 *
 * NOTE: Currently there is no deduplication on the table so we do not prevent dual insertion of the
 * same key. This may result in document duplication in results  */
t_docId DocTable::Put(const char *s, size_t n, double score, u_char flags,
                      const char *payload, size_t payloadSize) {

  t_docId xid = &dim->Get(s, n);
  // if the document is already in the index, return 0
  if (xid) {
    return 0;
  }
  t_docId docId = ++maxDocId;

  /* Copy the payload since it's probably an input string not retained */
  RSPayload *dpl = NULL;
  if (payload && payloadSize) {

    dpl = rm_malloc(sizeof(RSPayload));
    dpl->data = rm_calloc(1, payloadSize + 1);
    memcpy(dpl->data, payload, payloadSize);
    dpl->len = payloadSize;
    flags |= Document_HasPayload;
    memsize += payloadSize + sizeof(RSPayload);
  }

  sds keyPtr = sdsnewlen(s, n);

  RSDocumentMetadata *dmd = rm_calloc(1, sizeof(RSDocumentMetadata));
  dmd->keyPtr = keyPtr;
  dmd->score = score;
  dmd->flags = flags;
  dmd->payload = dpl;
  dmd->maxFreq = 1;
  dmd->id = docId;
  dmd->sortVector = NULL;

  Set(docId, dmd);
  ++size;
  memsize += sizeof(RSDocumentMetadata) + sdsAllocSize(keyPtr);
  &dim->Put(s, n, docId);
  return docId;
}

//---------------------------------------------------------------------------------------------

// Get the payload for a document, if any was set.
// If no payload has been set or the document id is not found, we return NULL.
RSPayload *DocTable::GetPayload(t_docId docId) {
  RSDocumentMetadata *dmd = Get(docId);
  return dmd ? dmd->payload : NULL;
}

//---------------------------------------------------------------------------------------------

// Get the "real" external key for an incremental id. Returns NULL if docId is not in the table.
const char *DocTable::GetKey(t_docId docId, size_t *lenp) {
  size_t len_s = 0;
  if (!lenp) {
    lenp = &len_s;
  }

  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd) {
    *lenp = 0;
    return NULL;
  }
  *lenp = sdslen(dmd->keyPtr);
  return dmd->keyPtr;
}

//---------------------------------------------------------------------------------------------

// Get the score for a document from the table. Returns 0 if docId is not in the table.
inline float DocTable::GetScore(t_docId docId) {
  RSDocumentMetadata *dmd = Get(docId);
  return dmd ? dmd->score : 0;
}

//---------------------------------------------------------------------------------------------

void DMD_Free(RSDocumentMetadata *md) {
  if (md->payload) {
    rm_free(md->payload->data);
    rm_free(md->payload);
    md->flags &= ~Document_HasPayload;
    md->payload = NULL;
  }
  if (md->sortVector) {
    delete md->sortVector;
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

//---------------------------------------------------------------------------------------------

void DocTable::~DocTable() {
  for (int i = 0; i < cap; ++i) {
    DMDChain *chain = &buckets[i];
    if (DLLIST2_IS_EMPTY(&chain->lroot)) {
      continue;
    }
    DLLIST2_node *nn = chain->lroot.head;
    while (nn) {
      RSDocumentMetadata *md = DLLIST2_ITEM(nn, RSDocumentMetadata, llnode);
      nn = nn->next;
      DMD_Free(md);
    }
  }
}

//---------------------------------------------------------------------------------------------

void DocTable::DmdUnchain(RSDocumentMetadata *md) {
  uint32_t bucketIndex = GetBucket(md->id);
  DMDChain *dmdChain = &buckets[bucketIndex];
  dllist2_delete(&dmdChain->lroot, &md->llnode);
}

//---------------------------------------------------------------------------------------------

int DocTable::Delete(const char *s, size_t n) {
  RSDocumentMetadata *md = Pop(s, n);
  if (md) {
    DMD_Decref(md);
    return 1;
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata *DocTable::Pop(const char *s, size_t n) {
  t_docId docId = &dim->Get(s, n);

  if (docId && docId <= maxDocId) {

    RSDocumentMetadata *md = Get(docId);
    if (!md) {
      return NULL;
    }

    md->flags |= Document_Deleted;

    DmdUnchain(md);
    &dim->Delete(s, n);
    --size;

    return md;
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

// Save the table to RDB. Called from the owning index
void DocTable::RdbSave(RedisModuleIO *rdb) {
  RedisModule_SaveUnsigned(rdb, size);
  RedisModule_SaveUnsigned(rdb, maxDocId);
  RedisModule_SaveUnsigned(rdb, maxSize);

  uint32_t elements_written = 0;
  for (uint32_t i = 0; i < cap; ++i) {
    if (DLLIST2_IS_EMPTY(&buckets[i].lroot)) {
      continue;
    }
    DLLIST2_FOREACH(it, &buckets[i].lroot) {
      const RSDocumentMetadata *dmd = DLLIST2_ITEM(it, RSDocumentMetadata, llnode);
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
        dmd->sortVector->RdbSave(rdb);
      }

      if (dmd->flags & Document_HasOffsetVector) {
        Buffer tmp(16);
        RSByteOffsets_Serialize(dmd->byteOffsets, &tmp);
        RedisModule_SaveStringBuffer(rdb, tmp.data, tmp.offset);
      }
      ++elements_written;
    }
  }
  RS_LOG_ASSERT((elements_written + 1 == size), "Wrong number of written elements");
}

//---------------------------------------------------------------------------------------------

// Load the table from RDB
void DocTable::RdbLoad(RedisModuleIO *rdb, int encver) {
  long long deletedElements = 0;
  size = RedisModule_LoadUnsigned(rdb);
  maxDocId = RedisModule_LoadUnsigned(rdb);
  if (encver >= INDEX_MIN_COMPACTED_DOCTABLE_VERSION) {
    maxSize = RedisModule_LoadUnsigned(rdb);
  } else {
    maxSize = MIN(RSGlobalConfig.maxDocTableSize, maxDocId);
  }

  if (maxDocId > maxSize) {
    /**
     * If the maximum doc id is greater than the maximum cap size
     * then it means there is a possibility that any index under maxId can
     * be accessed. However, it is possible that this bucket does not have
     * any documents inside it (and thus might not be populated below), but
     * could still be accessed for simple queries (e.g. get, exist). Ensure
     * we don't have to rely on Set/Put to ensure the doc table array.
     */
    cap = maxSize;
    rm_free(buckets);
    buckets = rm_calloc(cap, sizeof(*buckets));
  }

  for (size_t i = 1; i < size; i++) {
    size_t len;

    RSDocumentMetadata *dmd;
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
    dmd->payload = NULL;
    // read payload if set
    if ((dmd->flags & Document_HasPayload)) {
      if (!(dmd->flags & Document_Deleted)) {
        dmd->payload = rm_malloc(sizeof(RSPayload));
        dmd->payload->data = RedisModule_LoadStringBuffer(rdb, &dmd->payload->len);
        char *buf = rm_malloc(dmd->payload->len);
        memcpy(buf, dmd->payload->data, dmd->payload->len);
        RedisModule_Free(dmd->payload->data);
        dmd->payload->data = buf;
        dmd->payload->len--;
        memsize += dmd->payload->len + sizeof(RSPayload);
      } else if ((dmd->flags & Document_Deleted) && (encver == INDEX_MIN_EXPIRE_VERSION)) {
        RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL));  // throw this string to garbage
      }
    }
    dmd->sortVector = NULL;
    if (dmd->flags & Document_HasSortVector) {
      dmd->sortVector = new RSSortingVector(rdb, encver);
      sortablesSize += dmd->sortVector->GetMemorySize();
    }

    if (dmd->flags & Document_HasOffsetVector) {
      RMBuffer buf(rdb); // @@ handle exception
      dmd->byteOffsets = new RSByteOffsets(buf);
    }

    if (dmd->flags & Document_Deleted) {
      ++deletedElements;
      DMD_Free(dmd);
    } else {
      &dim->Put(dmd->keyPtr, sdslen(dmd->keyPtr), dmd->id);
      Set(dmd->id, dmd);
      memsize += sizeof(RSDocumentMetadata) + len;
    }
  }
  size -= deletedElements;
}

//---------------------------------------------------------------------------------------------

DocIdMap::DocIdMap() {
  *tm = new TrieMap();
}

//---------------------------------------------------------------------------------------------

// Get docId from a did-map. Returns 0  if the key is not in the map
t_docId DocIdMap::Get(const char *s, size_t n) const {
  void *val = tm->Find((char *)s, n);
  if (val && val != TRIEMAP_NOTFOUND) {
    return *((t_docId *)val);
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

void *_docIdMap_replace(void *oldval, void *newval) {
  if (oldval) {
    rm_free(oldval);
  }
  return newval;
}

//---------------------------------------------------------------------------------------------

// Put a new doc id in the map if it does not already exist
void DocIdMap::Put(const char *s, size_t n, t_docId docId) {
  t_docId *pd = rm_malloc(sizeof(t_docId));
  *pd = docId;
  tm->Add((char *)s, n, pd, _docIdMap_replace);
}

//---------------------------------------------------------------------------------------------

void DocIdMap::~DocIdMap(DocIdMap *m) {
  TrieMap_Free(tm, rm_free);
}

//---------------------------------------------------------------------------------------------

int DocIdMap::Delete(const char *s, size_t n) {
  return tm->Delete((char *)s, n, rm_free);
}

///////////////////////////////////////////////////////////////////////////////////////////////
