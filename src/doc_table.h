#ifndef __DOC_TABLE_H__
#define __DOC_TABLE_H__
#include <stdlib.h>
#include <string.h>
#include "redismodule.h"
#include "dep/triemap/triemap.h"
#include "redisearch.h"
#include "sortable.h"
#include "byte_offsets.h"
#include "rmutil/sds.h"
#include "util/dict.h"

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
  // the maximum size this table is allowed to grow to
  t_docId maxSize;
  t_docId maxDocId;
  size_t cap;
  size_t memsize;
  size_t sortablesSize;

  DMDChain *buckets;
  DocIdMap dim;
} DocTable;

/* increasing the ref count of the given dmd */
#define DMD_Incref(md) \
  if (md) ++md->ref_count;

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

/* Get the metadata for a doc Id from the DocTable.
 *  If docId is not inside the table, we return NULL */
RSDocumentMetadata *DocTable_Get(const DocTable *t, t_docId docId);

RSDocumentMetadata *DocTable_GetByKeyR(const DocTable *r, RedisModuleString *s);

/* Put a new document into the table, assign it an incremental id and store the metadata in the
 * table.
 *
 * NOTE: Currently there is no deduplication on the table so we do not prevent dual insertion of the
 * same key. This may result in document duplication in results  */
RSDocumentMetadata *DocTable_Put(DocTable *t, const char *s, size_t n, double score, RSDocumentFlags flags,
                                 const char *payload, size_t payloadSize, DocumentType type);

/* Get the "real" external key for an incremental i
 * If the document ID is not in the table, the returned key's `str` member will
 * be NULL
 */
const char *DocTable_GetKey(DocTable *t, t_docId docId, size_t *n);

/* Get the score for a document from the table. Returns 0 if docId is not in the table. */
float DocTable_GetScore(DocTable *t, t_docId docId);

/* Set the payload for a document. Returns 1 if we set the payload, 0 if we couldn't find the
 * document */
int DocTable_SetPayload(DocTable *t, RSDocumentMetadata *dmd, const char *data, size_t len);

int DocTable_Exists(const DocTable *t, t_docId docId);

/* Set the sorting vector for a document. If the vector is NULL we mark the doc as not having a
 * vector. Returns 1 on success, 0 if the document does not exist. No further validation is done */
int DocTable_SetSortingVector(DocTable *t, RSDocumentMetadata *dmd, RSSortingVector *v);

/* Set the offset vector for a document. This contains the byte offsets of each token found in
 * the document. This is used for highlighting
 */
int DocTable_SetByteOffsets(DocTable *t, RSDocumentMetadata *dmd, RSByteOffsets *offsets);

/* Get the payload for a document, if any was set. If no payload has been set or the document id is
 * not found, we return NULL */
RSPayload *DocTable_GetPayload(DocTable *t, t_docId dodcId);

/** Get the docId of a key if it exists in the table, or 0 if it doesnt */
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

int DocTable_Delete(DocTable *t, const char *key, size_t n);
static inline int DocTable_DeleteR(DocTable *t, RedisModuleString *r) {
  STRVARS_FROM_RSTRING(r);
  return DocTable_Delete(t, s, n);
}

RSDocumentMetadata *DocTable_Pop(DocTable *t, const char *s, size_t n);
static inline RSDocumentMetadata *DocTable_PopR(DocTable *t, RedisModuleString *r) {
  STRVARS_FROM_RSTRING(r);
  return DocTable_Pop(t, s, n);
}

static inline RSDocumentMetadata *DocTable_GetByKey(DocTable *dt, const char *key) {
  t_docId id = DocTable_GetId(dt, key, strlen(key));
  if (id == 0) {
    return NULL;
  }
  return DocTable_Get(dt, id);
}

/* Change name of document hash in the same spec without reindexing */
int DocTable_Replace(DocTable *t, const char *from_str, size_t from_len, const char *to_str,
                     size_t to_len);

/* don't use this function directly. Use DMD_Decref */
void DMD_Free(RSDocumentMetadata *);

/* Decrement the refcount of the DMD object, freeing it if we're the last reference */
static inline void DMD_Decref(RSDocumentMetadata *dmd) {
  if (dmd && !--dmd->ref_count) {
    DMD_Free(dmd);
  }
}

/* Save the table to RDB. Called from the owning index */
void DocTable_RdbSave(DocTable *t, RedisModuleIO *rdb);

void DocTable_LegacyRdbLoad(DocTable *t, RedisModuleIO *rdb, int encver);

/* Load the table from RDB */
void DocTable_RdbLoad(DocTable *t, RedisModuleIO *rdb, int encver);

#ifdef __cplusplus
}
#endif
#endif
