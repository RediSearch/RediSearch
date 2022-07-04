
#pragma once

#include "redisearch.h"
#include "sortable.h"
#include "byte_offsets.h"

#include "redismodule.h"
#include "triemap/triemap.h"
#include "rmutil/sds.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Retrieves the pointer and length for the document's key.
inline const char *RSDocumentMetadata::KeyPtrLen(size_t *len) const {
  if (len) {
    *len = sdslen(keyPtr);
  }
  return keyPtr;
}

//---------------------------------------------------------------------------------------------

// Convenience function to create a RedisModuleString from the document's key
inline RedisModuleString *RSDocumentMetadata::CreateKeyString(RedisModuleCtx *ctx) const {
  return RedisModule_CreateString(ctx, keyPtr, sdslen(keyPtr));
}

//---------------------------------------------------------------------------------------------

// Map between external id an incremental id
struct DocIdMap {
  TrieMap *tm;

  DocIdMap();
  ~DocIdMap();

  t_docId Get(const char *s, size_t n) const;

  void Put(const char *s, size_t n, t_docId docId);

  int Delete(const char *s, size_t n);
};

//---------------------------------------------------------------------------------------------

/* The DocTable is a simple mapping between incremental ids and the original document key and
 * metadata. It is also responsible for storing the id incrementor for the index and assigning
 * new incremental ids to inserted keys.
 *
 * NOTE: Currently there is no deduplication on the table so we do not prevent dual insertion of
 * the same key. This may result in document duplication in results
 */

struct DMDChain {
  DLLIST2 lroot;
};

//---------------------------------------------------------------------------------------------

class DocTable : public Object {

protected:
  void ctor(size_t cap, size_t max_size);

  void Set(t_docId docId, RSDocumentMetadata *dmd);

  void DmdUnchain(RSDocumentMetadata *md);

  int ValidateDocId(t_docId docId) const;

  uint32_t GetBucket(t_docId docId) const;

public:
  size_t size;
  // the maximum size this table is allowed to grow to
  t_docId maxSize;
  t_docId maxDocId;
  size_t cap;
  size_t memsize;
  size_t sortablesSize;

  DMDChain *buckets;
  DocIdMap dim;
  
  DocTable(size_t cap, size_t max_size) { ctor(cap, max_size); }
  DocTable(size_t cap);
  DocTable();
  ~DocTable();

  RSDocumentMetadata *Get(t_docId docId) const;

  RSDocumentMetadata *GetByKeyR(RedisModuleString *s) const;

  t_docId Put(const char *s, size_t n, double score, u_char flags,
              const char *payload, size_t payloadSize);

  const char *GetKey(t_docId docId, size_t *n);

  float GetScore(t_docId docId);

  int SetPayload(t_docId docId, const char *data, size_t len);

  bool Exists(t_docId docId) const;

  int SetSortingVector(t_docId docId, RSSortingVector *v);

  int SetByteOffsets(t_docId docId, RSByteOffsets *offsets);

  RSPayload *GetPayload(t_docId dodcId);

  t_docId GetId(const char *s, size_t n) const;
  t_docId GetIdR(RedisModuleString *r) const {
    size_t n;
    const char *s = RedisModule_StringPtrLen(r, &n);
    return GetId(s, n);
  }

  bool Delete(const char *key, size_t n);
  bool DeleteR(RedisModuleString *r) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(r, &n);
    return Delete(s, n);
  }

  RSDocumentMetadata *Pop(const char *s, size_t n);
  RSDocumentMetadata *PopR(RedisModuleString *r) {
    size_t n;
    const char *s = RedisModule_StringPtrLen(r, &n);
    return Pop(s, n);
  }

  RSDocumentMetadata *GetByKey(const char *key) {
    t_docId id = GetId(key, strlen(key));
    if (id == 0) {
      return NULL;
    }
    return Get(id);
  }

  void RdbSave(RedisModuleIO *rdb);
  void RdbLoad(RedisModuleIO *rdb, int encver);
};

//---------------------------------------------------------------------------------------------

//@@ What is the type of `code` (?)
// I want to move it into RSDocumentMetadata struct.
#define DOCTABLE_FOREACH(dt, code)                                           \
  for (size_t i = 1; i < dt->cap; ++i) {                                     \
    DMDChain *chain = &dt->buckets[i];                                       \
    if (DLLIST2_IS_EMPTY(&chain->lroot)) {                                   \
      continue;                                                              \
    }                                                                        \
    DLLIST2_FOREACH(it, &chain->lroot) {                                     \
      RSDocumentMetadata *dmd = DLLIST_ITEM(it, RSDocumentMetadata, llnode); \
      code;                                                                  \
    }                                                                        \
  }

///////////////////////////////////////////////////////////////////////////////////////////////
