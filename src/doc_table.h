#ifndef __DOC_TABLE_H__
#define __DOC_TABLE_H__
#include <stdlib.h>
#include <string.h>
#include "redismodule.h"
#include "dep/triemap/triemap.h"
#include "redisearch.h"
#include "sortable.h"
#include "byte_offsets.h"

/* Map between external id an incremental id */
typedef struct { TrieMap *tm; } DocIdMap;

DocIdMap NewDocIdMap();
/* Get docId from a did-map. Returns 0  if the key is not in the map */
t_docId DocIdMap_Get(DocIdMap *m, const char *key);

/* Put a new doc id in the map if it does not already exist */
void DocIdMap_Put(DocIdMap *m, const char *key, t_docId docId);

int DocIdMap_Delete(DocIdMap *m, const char *key);
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
  size_t size;
  t_docId maxDocId;
  size_t cap;
  size_t memsize;
  RSDocumentMetadata *docs;
  DocIdMap dim;

} DocTable;

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap);

/* Get the metadata for a doc Id from the DocTable.
*  If docId is not inside the table, we return NULL */
RSDocumentMetadata *DocTable_Get(DocTable *t, t_docId docId);

/* Put a new document into the table, assign it an incremental id and store the metadata in the
* table.
*
* NOTE: Currently there is no deduplication on the table so we do not prevent dual insertion of the
* same key. This may result in document duplication in results  */
t_docId DocTable_Put(DocTable *t, const char *key, double score, u_char flags, const char *payload,
                     size_t payloadSize);

/* Get the "real" external key for an incremental id. Returns NULL if docId is not in the table. */
const char *DocTable_GetKey(DocTable *t, t_docId docId);

/* Get the score for a document from the table. Returns 0 if docId is not in the table. */
float DocTable_GetScore(DocTable *t, t_docId docId);

/* Set the payload for a document. Returns 1 if we set the payload, 0 if we couldn't find the
 * document */
int DocTable_SetPayload(DocTable *t, t_docId docId, const char *data, size_t len);

/* Set the sorting vector for a document. If the vector is NULL we mark the doc as not having a
 * vector. Returns 1 on success, 0 if the document does not exist. No further validation is done */
int DocTable_SetSortingVector(DocTable *t, t_docId docId, RSSortingVector *v);

/* Set the offset vector for a document. This contains the byte offsets of each token found in
 * the document. This is used for highlighting
 */
int DocTable_SetByteOffsets(DocTable *t, t_docId docId, RSByteOffsets *offsets);

/* Get the payload for a document, if any was set. If no payload has been set or the document id is
 * not found, we return NULL */
RSPayload *DocTable_GetPayload(DocTable *t, t_docId dodcId);

/** Get the docId of a key if it exists in the table, or 0 if it doesnt */
t_docId DocTable_GetId(DocTable *dt, const char *key);

/* Free the table and all the keys of documents */
void DocTable_Free(DocTable *t);

int DocTable_Delete(DocTable *t, const char *key);

/* Save the table to RDB. Called from the owning index */
void DocTable_RdbSave(DocTable *t, RedisModuleIO *rdb);

/* Load the table from RDB */
void DocTable_RdbLoad(DocTable *t, RedisModuleIO *rdb, int encver);

/* Emit special FT.DTADD commands to recreate the table */
void DocTable_AOFRewrite(DocTable *t, const char *indexName, RedisModuleIO *aof);

#endif