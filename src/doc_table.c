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
void DocTable::ctor(size_t cap, t_docId max_size) {
  size = 1;
  maxSize = max_size;
  maxDocId = 0;
  memsize = 0;
  sortablesSize = 0;
  buckets.reserve(cap);
}

DocTable::DocTable(size_t cap) {
  ctor(cap, t_docId{RSGlobalConfig.maxDocTableSize});
}

DocTable::DocTable() {
  ctor(1000, t_docId{RSGlobalConfig.maxDocTableSize});
}

//---------------------------------------------------------------------------------------------

DocTable::BucketIndex DocTable::GetBucketIdx(t_docId docId) const {
  return BucketIndex{docId % maxSize};
}

//---------------------------------------------------------------------------------------------

bool DocTable::ValidateDocId(t_docId docId) const {
  return docId != 0 && docId <= maxDocId;
}

//---------------------------------------------------------------------------------------------

// Get the metadata for a doc Id from the DocTable. If docId is not inside the table, we return NULL

RSDocumentMetadata *DocTable::Get(t_docId docId) const {
  if (!ValidateDocId(docId)) {
    return NULL;
  }
  BucketIndex bucketIndex = GetBucketIdx(docId);
  if (bucketIndex >= buckets.size()) {
    return NULL;
  }

  for (auto &dmd: buckets[bucketIndex]) {
    if (dmd.id == docId) {
      return &dmd;
    }
  }

  return NULL;
}

//---------------------------------------------------------------------------------------------

bool DocTable::Exists(t_docId docId) const {
  if (!ValidateDocId(docId)) {
    return false;
  }

  BucketIndex bucketIndex = GetBucketIdx(docId);
  if (bucketIndex >= buckets.size()) {
    return false;
  }

  for (auto &dmd: buckets[bucketIndex]) {
    if (dmd.id == docId && !dmd.IsDeleted()) {
      return true;
    }
  }

  return false;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata *DocTable::GetByKey(RedisModuleString *s) const {
  const char *kstr;
  size_t klen;
  kstr = RedisModule_StringPtrLen(s, &klen);
  t_docId id = GetId(kstr, klen);
  return Get(id);
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata &DocTable::Set(t_docId docId, RSDocumentMetadata &&dmd) {
  size_t size = buckets.size();
  BucketIndex bucketIdx = GetBucketIdx(docId);
  if (bucketIdx >= size && size < maxSize) {
    // We have to grow the array size.
    // We grow till we reach maxSize, then we add dmds to the existing chains.

    size_t oldsize = size;
    // We grow by half of the current capacity with maximum of 1m
    size += 1 + (size ? MIN(size / 2, 1024 * 1024) : 1);
    size = MIN(size, maxSize);  // make sure we do not excised maxSize
    size = MAX(size, bucketIdx + 1);  // docs[bucket] needs to be valid, so size > bucketIdx

    buckets.resize(size);
  }

  DMDChain &chain = buckets[bucketIdx];
  //dmd->Incref();

  chain.emplace_back(std::move(dmd));
  return chain.back();
}

//---------------------------------------------------------------------------------------------

// Get the docId of a key if it exists in the table, or 0 if it doesnt
t_docId DocTable::GetId(const char *s, size_t n) const {
  return dim.Get(s, n);
}

//---------------------------------------------------------------------------------------------

// Set payload for a document.
// Returns 1 if we set the payload, 0 if we couldn't find the document

bool DocTable::SetPayload(t_docId docId, RSPayload *data) {
  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd || !data) {
    return false;
  }

  if (dmd->payload) {
    memsize -= dmd->payload->memsize();
    delete dmd->payload;
  }

  dmd->payload = data;
  dmd->flags |= Document_HasPayload;
  memsize += dmd->payload->memsize();
  return true;
}

//---------------------------------------------------------------------------------------------

// Set the sorting vector for a document.
// If the vector is NULL we mark the doc as not having a vector.
// Returns true on success, false if the document does not exist. No further validation is done.

bool DocTable::SetSortingVector(t_docId docId, RSSortingVector *v) {
  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd) {
    return false;
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

  // Set th new vector and the flags accordingly
  dmd->sortVector = v;
  dmd->flags |= Document_HasSortVector;
  sortablesSize += v->memsize();

  return true;
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

RSPayload::RSPayload(const char *payload, size_t payloadSize) {
  data = rm_calloc(1, payloadSize + 1);
  memcpy(data, payload, payloadSize);
  len = payloadSize;
}

//---------------------------------------------------------------------------------------------

RSPayload::RSPayload(RedisModuleIO *rdb) {
  void *payload_data = RedisModule_LoadStringBuffer(rdb, &len);
  data = rm_malloc(len);
  memcpy(data, payload_data, len);
  RedisModule_Free(payload_data);
  --len;
}

//---------------------------------------------------------------------------------------------

RSPayload::RSPayload(TriePayload *payload) {
  memcpy(data, payload->data, payload->len);
  len = payload->len;
}


//---------------------------------------------------------------------------------------------

// Put a new document into the table, assign it an incremental id and store the metadata in the
// table.
// Return 0 if the document is already in the index.
//
// NOTE:
// Currently there is no deduplication on the table so we do not prevent dual insertion of the
// same key. This may result in document duplication in results.

t_docId DocTable::Put(const char *s, size_t n, double score, u_char flags, RSPayload *payload) {
  t_docId xid = dim.Get(s, n);
  // if the document is already in the index, return 0
  if (xid) {
    return t_docId{0};
  }

  t_docId docId = ++maxDocId;

  RSDocumentMetadata &dmd = Set(docId, RSDocumentMetadata(s, n, score, flags, payload, docId));
  ++size;
  memsize += dmd.memsize();
  dim.Put(s, n, docId);
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

float DocTable::GetScore(t_docId docId) {
  RSDocumentMetadata *dmd = Get(docId);
  return dmd ? dmd->score : 0;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata::~RSDocumentMetadata() {
  delete payload;
  delete sortVector;
  delete byteOffsets;
  sdsfree(keyPtr);
}

//---------------------------------------------------------------------------------------------

DocTable::~DocTable() {
}

//---------------------------------------------------------------------------------------------

void DocTable::Unchain(RSDocumentMetadata *dmd) {
  uint32_t bucketIndex = GetBucketIdx(dmd->id);
  DMDChain &chain = buckets[bucketIndex];
  chain.erase(dmd->dmd_iter);
}

//---------------------------------------------------------------------------------------------

bool DocTable::Delete(const char *s, size_t n) {
  RSDocumentMetadata *md = Pop(s, n, false);
  return !!md;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata *DocTable::Pop(const char *s, size_t n, bool retain) {
  t_docId docId = dim.Get(s, n);
  if (!ValidateDocId(docId)) return NULL;

  RSDocumentMetadata *dmd = Get(docId);
  if (!dmd) {
    return NULL;
  }

  dmd->flags |= Document_Deleted;

  RSDocumentMetadata *dmd1 = NULL;
  if (retain) dmd1 = new RSDocumentMetadata{std::move(*dmd)};

  Unchain(dmd);
  dim.Delete(s, n);
  --size;

  return dmd1;
}

//---------------------------------------------------------------------------------------------

// Save the table to RDB. Called from the owning index

void DocTable::RdbSave(RedisModuleIO *rdb) {
  RedisModule_SaveUnsigned(rdb, size);
  RedisModule_SaveUnsigned(rdb, maxDocId);
  RedisModule_SaveUnsigned(rdb, maxSize);

  uint32_t elements_written = 0;
  for (auto &bucket: buckets) {
    for (auto &dmd: bucket) {
      dmd.RdbSave(rdb);
      ++elements_written;
    }
  }

  RS_LOG_ASSERT((elements_written + 1 == size), "Wrong number of written elements");
}

//---------------------------------------------------------------------------------------------

void DocTable::RdbLoad(RedisModuleIO *rdb, int encver) {
  size_t deletedElements = 0;
  size = RedisModule_LoadUnsigned(rdb);
  maxDocId = RedisModule_LoadUnsigned(rdb);
  if (encver >= INDEX_MIN_COMPACTED_DOCTABLE_VERSION) {
    maxSize = RedisModule_LoadUnsigned(rdb);
  } else {
    maxSize = MIN(RSGlobalConfig.maxDocTableSize, maxDocId);
  }

  if (maxDocId > maxSize) {
    // If the maximum doc id is greater than the maximum cap size then it means there is a
    // possibility that any index under maxId can be accessed.
    // However, it is possible that this bucket does not have any documents inside it (and
    // thus might not be populated below), but could still be accessed for simple queries
    // (e.g. get, exist). Ensure we don't have to rely on Set/Put to ensure the doc table array.

    buckets.clear();
    buckets.reserve(maxSize);
  }

  for (size_t i = 1; i < size; i++) {
    RSDocumentMetadata dmd{t_docId{i}, rdb, encver};
    if (dmd.flags & Document_Deleted) {
      ++deletedElements;
    } else {
      dim.Put(dmd.keyPtr, sdslen(dmd.keyPtr), dmd.id);
      Set(dmd.id, std::move(dmd));
      memsize += dmd.memsize();
      sortablesSize += dmd.sortVector->memsize();
    }
  }
  size -= deletedElements;
}

//---------------------------------------------------------------------------------------------

t_docId DocTable::GetId(RedisModuleString *r) const {
  size_t n;
  const char *s = RedisModule_StringPtrLen(r, &n);
  return GetId(s, n);
}

//---------------------------------------------------------------------------------------------

bool DocTable::Delete(RedisModuleString *r) {
  size_t n;
  const char *s = RedisModule_StringPtrLen(r, &n);
  return Delete(s, n);
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata *DocTable::Pop(RedisModuleString *r, bool retain) {
  size_t n;
  const char *s = RedisModule_StringPtrLen(r, &n);
  return Pop(s, n, retain);
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata *DocTable::GetByKey(const char *key) {
  t_docId id = GetId(key, strlen(key));
  if (id == 0) {
    return NULL;
  }
  return Get(id);
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSPayload::~RSPayload() {
  if (data) rm_free(data);
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata::RSDocumentMetadata(const char *id, size_t idlen, double score_, Mask(RSDocumentFlags) flags_,
    RSPayload *payload_, t_docId docId) {

  keyPtr = sdsnewlen(id, idlen);
  score = score_;
  flags = flags_ | (payload ? Document_HasPayload : 0);
  payload = payload_;
  maxFreq = 1;
  id = docId;
  sortVector = NULL;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata::RSDocumentMetadata(RSDocumentMetadata &&dmd) : id(dmd.id), keyPtr(dmd.keyPtr),
    score(dmd.score), maxFreq(dmd.maxFreq), len(dmd.len), flags(dmd.flags), payload(dmd.payload),
    sortVector(dmd.sortVector), byteOffsets(dmd.byteOffsets), dmd_iter(dmd.dmd_iter), ref_count(dmd.ref_count) {
  dmd.payload = NULL;
  dmd.sortVector = NULL;
  dmd.byteOffsets = NULL;
}

//---------------------------------------------------------------------------------------------

RSDocumentMetadata::RSDocumentMetadata(t_docId id, RedisModuleIO *rdb, int encver) {
  size_t keylen;
  char *key = RedisModule_LoadStringBuffer(rdb, &keylen);
  if (encver < INDEX_MIN_BINKEYS_VERSION) {
    // Previous versions would encode the NUL byte
    keylen--;
  }
  id = encver < INDEX_MIN_COMPACTED_DOCTABLE_VERSION ? id : t_docId(RedisModule_LoadUnsigned(rdb));
  keyPtr = sdsnewlen(key, keylen);
  RedisModule_Free(key);

  flags = RedisModule_LoadUnsigned(rdb);
  maxFreq = 1;
  len = 1;
  if (encver > 1) {
    maxFreq = RedisModule_LoadUnsigned(rdb);
  }

  if (encver >= INDEX_MIN_DOCLEN_VERSION) {
    len = RedisModule_LoadUnsigned(rdb);
  } else {
    // In older versions, default the len to max freq to avoid division by zero.
    len = maxFreq;
  }

  score = RedisModule_LoadFloat(rdb);

  if (flags & Document_HasPayload) {
    if (!(flags & Document_Deleted)) {
      payload = new RSPayload(rdb);
    } else if (encver == INDEX_MIN_EXPIRE_VERSION) {
      RedisModule_Free(RedisModule_LoadStringBuffer(rdb, NULL)); // throw this string to garbage
    }
  } else {
    payload = NULL;
  }

  if (flags & Document_HasSortVector) {
    sortVector = new RSSortingVector(rdb, encver);
  } else {
    sortVector = NULL;
  }

  if (flags & Document_HasOffsetVector) {
    RMBuffer buf(rdb); // @@ handle exception
    byteOffsets = new RSByteOffsets(buf);
  }
}

//---------------------------------------------------------------------------------------------

void RSDocumentMetadata::RdbSave(RedisModuleIO *rdb) {
  RedisModule_SaveStringBuffer(rdb, keyPtr, sdslen(keyPtr));
  RedisModule_SaveUnsigned(rdb, id);
  RedisModule_SaveUnsigned(rdb, flags);
  RedisModule_SaveUnsigned(rdb, maxFreq);
  RedisModule_SaveUnsigned(rdb, len);
  RedisModule_SaveFloat(rdb, score);
  if (flags & Document_HasPayload) {
    if (payload) {
      // save an extra space for the null terminator to make the payload null terminated on
      RedisModule_SaveStringBuffer(rdb, payload->data, payload->len + 1);
    } else {
      RedisModule_SaveStringBuffer(rdb, "", 1);
    }
  }

  if (flags & Document_HasSortVector) {
    sortVector->RdbSave(rdb);
  }

  if (flags & Document_HasOffsetVector) {
    Buffer tmp(16);
    byteOffsets->Serialize(&tmp);
    RedisModule_SaveStringBuffer(rdb, tmp.data, tmp.offset);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

DocIdMap::DocIdMap() {
  tm = new TrieMap();
}

//---------------------------------------------------------------------------------------------

// Get docId from a did-map. Returns 0  if the key is not in the map
t_docId DocIdMap::Get(const char *s, size_t n) const {
  void *val = tm->Find((char *)s, n);
  if (val && val != TRIEMAP_NOTFOUND) {
    return *((t_docId *)val);
  }
  return t_docId{0};
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

DocIdMap::~DocIdMap() {
  delete tm;
}

//---------------------------------------------------------------------------------------------

int DocIdMap::Delete(const char *s, size_t n) {
  return tm->Delete((char *)s, n);
}

///////////////////////////////////////////////////////////////////////////////////////////////
