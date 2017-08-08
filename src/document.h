#ifndef __RS_DOCUMENT_H__
#define __RS_DOCUMENT_H__
#include <pthread.h>
#include "redismodule.h"
#include "search_ctx.h"
#include "redisearch.h"
#include "concurrent_ctx.h"

typedef struct {
  const char *name;
  RedisModuleString *text;
} DocumentField;

typedef struct {
  RedisModuleString *docKey;
  DocumentField *fields;
  int numFields;
  float score;
  const char *language;
  t_docId docId;

  const char *payload;
  size_t payloadSize;
  int stringOwner;
} Document;

/**
 * Initialize document structure with the relevant fields. numFields will allocate
 * the fields array, but you must still actually copy the data along.
 *
 * Note that this function assumes that the pointers passed in will remain valid
 * throughout the lifetime of the document. If you need to make independent copies
 * of the data within the document, call Document_Detach on the document (after
 * calling this function).
 */
void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize);

/**
 * Copy any data from the document into its own independent copies. srcCtx is
 * the context owning any RedisModuleString items - which are assigned using
 * RedisModule_RetainString.
 *
 * If the document contains fields, the field data is also retained.
 */
void Document_Detach(Document *doc, RedisModuleCtx *srcCtx);

/**
 * Free any copied data within the document. anyCtx is any non-NULL
 * RedisModuleCtx. The reason for requiring a context is more related to the
 * Redis Module API requiring a context for AutoMemory purposes, though in
 * this case, the pointers are already removed from AutoMemory manangement
 * anyway.
 *
 * This function also calls Document_Free
 */
void Document_FreeDetached(Document *doc, RedisModuleCtx *anyCtx);

/**
 * Free the document's internals (like the field array).
 */
void Document_Free(Document *doc);

#define DOCUMENT_ADD_NOSAVE 0x01
#define DOCUMENT_ADD_REPLACE 0x02

struct ForwardIndex;
struct IndexingContext;

typedef struct RSAddDocumentCtx {
  struct RSAddDocumentCtx *next;
  Document doc;
  RedisModuleBlockedClient *bc;
  RedisModuleCtx *thCtx;
  RedisSearchCtx rsCtx;
  struct ForwardIndex *fwIdx;
  struct IndexingContext *ictx;
  ConcurrentSearchCtx conc;
  uint8_t options;
  uint8_t done;
  pthread_cond_t cond;
} RSAddDocumentCtx;

int Document_AddToIndexes(RSAddDocumentCtx *ctx, const char **errorString);

/* Load a single document */
int Redis_LoadDocument(RedisSearchCtx *ctx, RedisModuleString *key, Document *Doc);

/**
 * Load a single document
 * fields is an array of fields to load from a document.
 * keyp is an [out] pointer to a key which may be closed after the document field
 * is no longer required. Can be NULL
 */
int Redis_LoadDocumentEx(RedisSearchCtx *ctx, RedisModuleString *key, const char **fields,
                         size_t nfields, Document *doc, RedisModuleKey **keyp);

/* Load a bunch of documents from redis */
Document *Redis_LoadDocuments(RedisSearchCtx *ctx, RedisModuleString **keys, int numKeys,
                              const char **fields, int numFields, int *nump);

/**
 * Save a document in the index. Used for returning contents in search results.
 */
int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc);

/**
 * Creates a new context used for adding documents. Once created, call
 * Document_AddToIndexes on it.
 *
 * When done, call AddDocumentCtx_Free
 */
RSAddDocumentCtx *NewAddDocumentCtx(RedisModuleBlockedClient *client, IndexSpec *sp);

/**
 * Free the AddDocumentCtx
 */
void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx);

struct DocumentIndexer;
extern struct DocumentIndexer *Indexer_g;
void StartDocumentIndexer();

#endif