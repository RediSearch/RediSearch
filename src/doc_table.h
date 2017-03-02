#ifndef __DOC_TABLE_H__
#define __DOC_TABLE_H__
#include <stdlib.h>
#include <string.h>
#include "types.h"
#include "redismodule.h"
#include "util/triemap.h"

typedef struct {
  char *data;
  size_t len;
} DocumentPayload;
/* DocumentMetadata describes metadata stored about a document in the index (not the document
* itself).
*
* The key is the actual user defined key of the document, not the incremental id. It is used to
* convert incremental internal ids to external string keys.
*
* Score is the original user score as inserted to the index
*
* Flags is not currently used, but should be used in the future to mark documents as deleted, etc.
*/
typedef struct {
  char *key;
  float score;
  uint32_t maxFreq;
  u_char flags;
  DocumentPayload *payload;
} DocumentMetadata;
//#pragma pack()

/* Document flags. The only supported flag currently is deleted, but more might come later one */
typedef enum {
  Document_DefaultFlags = 0x00,
  Document_Deleted = 0x01,
  Document_HasPayload = 0x02
} DocumentFlags;

/* Map between external id an incremental id */
typedef struct { TrieMapNode *tm; } DocIdMap;

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
  DocumentMetadata *docs;
  DocIdMap dim;

} DocTable;

/* Creates a new DocTable with a given capacity */
DocTable NewDocTable(size_t cap);

/* Get the metadata for a doc Id from the DocTable.
*  If docId is not inside the table, we return NULL */
DocumentMetadata *DocTable_Get(DocTable *t, t_docId docId);

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

/* Get the payload for a document, if any was set. If no payload has been set or the document id is
 * not found, we return NULL */
DocumentPayload *DocTable_GetPayload(DocTable *t, t_docId dodcId);

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
void DocTable_AOFRewrite(DocTable *t, RedisModuleString *k, RedisModuleIO *aof);

#endif