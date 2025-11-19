/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __DOC_TABLE_H__
#define __DOC_TABLE_H__
#include <stdlib.h>
#include <string.h>
#include "redismodule.h"
#include "triemap.h"
#include "redisearch.h"
#include "sortable.h"
#include "byte_offsets.h"
#include "hiredis/sds.h"
#include "rmutil/rm_assert.h"
#include "ttl_table.h"

#ifdef __cplusplus
extern "C" {
#endif
// Retrieves the pointer and length for the document's key.
static inline const char *DMD_KeyPtrLen(const RSDocumentMetadata *dmd, size_t *len) {
  if (len) {
    *len = sdslen(dmd->keyPtr);
  }
  return dmd->keyPtr;
}

// Convenience function to create a RedisModuleString from the document's key
static inline RedisModuleString *DMD_CreateKeyString(const RSDocumentMetadata *dmd,
                                                     RedisModuleCtx *ctx) {
  return RedisModule_CreateString(ctx, dmd->keyPtr, sdslen(dmd->keyPtr));
}

/* Map between external id an incremental id */
typedef struct {
  TrieMap *tm;
} DocIdMap;

DocIdMap NewDocIdMap();
/* Get docId from a did-map. Returns 0  if the key is not in the map */
t_docId DocIdMap_Get(const DocIdMap *m, const char *s, size_t n);

/* Put a new doc id in the map if it does not already exist */
void DocIdMap_Put(DocIdMap *m, const char *s, size_t n, t_docId docId);

int DocIdMap_Delete(DocIdMap *m, const char *s, size_t n);
/* Free the doc id map */
void DocIdMap_Free(DocIdMap *m);

/* The DocTable is a simple mapping between incremental ids and the original document key and
 * metadata. It is also responsible for storing the id incrementor for the index and assigning
 * new
 * incremental ids to inserted keys.
 *
 * NOTE: Currently there is no deduplication on the table so we do not prevent dual insertion of
 * the
 * same key. This may result in document duplication in results  */

typedef struct {
  DLLIST2 lroot;
} DMDChain;

typedef struct {
  size_t size;
  t_docId maxSize;          // the maximum size this table is allowed to grow to
  t_docId maxDocId;         // the maximum docId assigned
  size_t cap;               // current capacity of buckets
  size_t memsize;           // total memory size occupied by the table
  size_t sortablesSize;     // total memory size occupied by the sortables

  DMDChain *buckets;
  DocIdMap dim;             // Mapping between document name to internal id
  TimeToLiveTable* ttl;
} DocTable;

#define DOCTABLE_FOREACH(dt, code)                                           \
  for (size_t i = 0; i < dt->cap; ++i) {                                     \
    DMDChain *chain = &dt->buckets[i];                                       \
    if (DLLIST2_IS_EMPTY(&chain->lroot)) {                                   \
      continue;                                                              \
    }                                                                        \
    DLLIST2_FOREACH(it, &chain->lroot) {                                     \
      RSDocumentMetadata *dmd = DLLIST_ITEM(it, RSDocumentMetadata, llnode); \
      code;                                                                  \
    }                                                                        \
  }

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap, size_t max_size);

#define DocTable_New(cap) NewDocTable(cap, RSGlobalConfig.maxDocTableSize)

/* Get a reference to the metadata for a doc Id from the DocTable.
 * If docId is not inside the table, we return NULL */
const RSDocumentMetadata *DocTable_Borrow(const DocTable *t, t_docId docId);

const RSDocumentMetadata *DocTable_BorrowByKeyR(const DocTable *r, RedisModuleString *s);

/* Put a new document into the table, assign it an incremental id and store the metadata in the
 * table.
 *
 * NOTE: Currently there is no deduplication on the table so we do not prevent dual insertion of the
 * same key. This may result in document duplication in results  */
RSDocumentMetadata *DocTable_Put(DocTable *t, const char *s, size_t n, double score,
                                 RSDocumentFlags flags, const char *payload, size_t payloadSize,
                                 DocumentType type);

/* Get the "real" external key for an incremental i
 * If the document ID is not in the table, the returned key's `str` member will
 * be NULL
 */
sds DocTable_GetKey(const DocTable *t, t_docId docId, size_t *n);

/* Set the payload for a document. Returns 1 if we set the payload, 0 if we couldn't find the
 * document */
int DocTable_SetPayload(DocTable *t, RSDocumentMetadata *dmd, const char *data, size_t len);

bool DocTable_Exists(const DocTable *t, t_docId docId);

/* Set the sorting vector for a document. If the vector is NULL we mark the doc as not having a
 * vector. Returns 1 on success, 0 if the document does not exist. No further validation is done */
int DocTable_SetSortingVector(DocTable *t, RSDocumentMetadata *dmd, RSSortingVector *v);

/* Set the offset vector for a document. This contains the byte offsets of each token found in
 * the document. This is used for highlighting
 */
void DocTable_SetByteOffsets(RSDocumentMetadata *dmd, RSByteOffsets *offsets);

void DocTable_UpdateExpiration(DocTable *t, RSDocumentMetadata* dmd, t_expirationTimePoint ttl, arrayof(FieldExpiration) allFieldSorted);

typedef struct {
  FieldMaskOrIndex field;
  // our field expiration predicate
  enum FieldExpirationPredicate predicate;
} FieldFilterContext;

bool DocTable_IsDocExpired(DocTable* t, const RSDocumentMetadata* dmd, struct timespec* expirationPoint);

// Will return true if the document passed the predicate
// default predicate - one of the fields did not yet expire -> entry is still valid
// missing predicate - one of the fields did expire -> entry is valid in the context of missing
static inline bool DocTable_CheckFieldExpirationPredicate(const DocTable *t, t_docId docId, t_fieldIndex field, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  if (!t->ttl) return true;
  return TimeToLiveTable_VerifyDocAndField(t->ttl, docId, field, predicate, expirationPoint);
}
// Same as above, but for a field mask (non-wide schema)
static inline bool DocTable_CheckFieldMaskExpirationPredicate(const DocTable *t, t_docId docId, uint32_t fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex) {
  if (!t->ttl) return true;
  return TimeToLiveTable_VerifyDocAndFieldMask(t->ttl, docId, fieldMask, predicate, expirationPoint, ftIdToFieldIndex);
}
// Same as above, but for a wide field mask
static inline bool DocTable_CheckWideFieldMaskExpirationPredicate(const DocTable *t, t_docId docId, t_fieldMask fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex) {
  if (!t->ttl) return true;
  return TimeToLiveTable_VerifyDocAndWideFieldMask(t->ttl, docId, fieldMask, predicate, expirationPoint, ftIdToFieldIndex);
}


/** Get the docId of a key if it exists in the table, or 0 if it doesn't */
t_docId DocTable_GetId(const DocTable *dt, const char *s, size_t n);

#define STRVARS_FROM_RSTRING(r) \
  size_t n;                     \
  const char *s = RedisModule_StringPtrLen(r, &n);

static inline t_docId DocTable_GetIdR(const DocTable *dt, RedisModuleString *r) {
  STRVARS_FROM_RSTRING(r);
  return DocTable_GetId(dt, s, n);
}

/* Free the table and all the keys of documents */
void DocTable_Free(DocTable *t);

RSDocumentMetadata *DocTable_Pop(DocTable *t, const char *s, size_t n);
static inline RSDocumentMetadata *DocTable_PopR(DocTable *t, RedisModuleString *r) {
  STRVARS_FROM_RSTRING(r);
  return DocTable_Pop(t, s, n);
}

static inline const RSDocumentMetadata *DocTable_BorrowByKey(DocTable *dt, const char *key) {
  t_docId id = DocTable_GetId(dt, key, strlen(key));
  if (id == 0) {
    return NULL;
  }
  return DocTable_Borrow(dt, id);
}

/* Change name of document hash in the same spec without reindexing */
int DocTable_Replace(DocTable *t, const char *from_str, size_t from_len, const char *to_str,
                     size_t to_len);

/* increasing the ref count of the given dmd */
/*
 * This macro is atomic and fits for single writer and multiple readers as it is used only
 * after we locked the index spec (R/W) and we either have a writer alone or multiple readers.
 */
#define DMD_Incref(md)                                                        \
  ({                                                                          \
    uint16_t count = __atomic_fetch_add(&md->ref_count, 1, __ATOMIC_RELAXED); \
    RS_LOG_ASSERT(count < (1 << 16) - 1, "overflow of dmd ref_count");        \
  })

/* don't use this function directly. Use DMD_Return */
void DMD_Free(const RSDocumentMetadata *);

/* Decrement the refcount of the DMD object, freeing it if we're the last reference */
static inline void DMD_Return(const RSDocumentMetadata *cdmd) {
  RSDocumentMetadata *dmd = (RSDocumentMetadata *)cdmd;

  if (dmd) {
      uint16_t count = __atomic_fetch_sub(&dmd->ref_count, 1, __ATOMIC_RELAXED);
      RS_LOG_ASSERT(count >= 1, "underflow of dmd ref_count");
      if (count == 1) {
        DMD_Free(dmd);
      }
  }
}

void DocTable_LegacyRdbLoad(DocTable *t, RedisModuleIO *rdb, int encver);

#ifdef __cplusplus
}
#endif
#endif
