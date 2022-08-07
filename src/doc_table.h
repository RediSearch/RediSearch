
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
/*
struct DMDChain {
  DLLIST2 lroot;
};
*/

//---------------------------------------------------------------------------------------------

typedef List<RSDocumentMetadata> DMDChain;

struct DocTable : public Object {
  typedef IdType<uint32_t> BucketIndex;

protected:
  void ctor(size_t cap, t_docId max_size);

  BucketIndex GetBucketIdx(t_docId docId) const;

  void Unchain(RSDocumentMetadata *md);

public:
  size_t size;
  t_docId maxSize; // the maximum size this table is allowed to grow to
  t_docId maxDocId;
  size_t memsize;
  size_t sortablesSize;

  Vector<DMDChain> buckets;
  DocIdMap dim;

  DocTable(size_t cap, t_docId max_size) { ctor(cap, max_size); }
  DocTable(size_t cap);
  DocTable();
  ~DocTable();

  bool ValidateDocId(t_docId docId) const;

  bool Exists(t_docId docId) const;
  const char *GetKey(t_docId docId, size_t *n);
  RSDocumentMetadata *Get(t_docId docId) const;

  float GetScore(t_docId docId);
  RSPayload *GetPayload(t_docId dodcId);

  t_docId GetId(const char *s, size_t n) const;
  t_docId GetId(const std::string_view &id) const { return GetId(id.data(), id.length()); }
  t_docId GetId(RedisModuleString *r) const;

  RSDocumentMetadata *GetByKey(const char *key);
  RSDocumentMetadata *GetByKey(RedisModuleString *s) const;

  RSDocumentMetadata &Set(t_docId docId, RSDocumentMetadata &&dmd);

  bool SetPayload(t_docId docId, const char *data, size_t len);
  bool SetSortingVector(t_docId docId, RSSortingVector *v);
  int SetByteOffsets(t_docId docId, RSByteOffsets *offsets);

  t_docId Put(const char *s, size_t n, double score, u_char flags, RSPayload *payload);

  bool Delete(const char *key, size_t n);
  bool Delete(RedisModuleString *r);

  RSDocumentMetadata *Pop(const char *s, size_t n, bool retail = false);
  RSDocumentMetadata *Pop(RedisModuleString *r, bool retail = false);

  void RdbSave(RedisModuleIO *rdb);
  void RdbLoad(RedisModuleIO *rdb, int encver);

  template <typename F>
  void foreach(F fn) {
    for (auto &bucket: buckets) {
      for (auto &dmd: bucket) {
        fn(dmd);
      }
    }
  }

};

///////////////////////////////////////////////////////////////////////////////////////////////
