#ifndef __RS_DOCUMENT_H__
#define __RS_DOCUMENT_H__
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

void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize);

void Document_Detatch(Document *doc, RedisModuleCtx *srcCtx);
void Document_FreeDetatched(Document *doc, RedisModuleCtx *anyCtx);

void Document_Free(Document *doc);

#define DOCUMENT_ADD_NOSAVE 0x01
#define DOCUMENT_ADD_REPLACE 0x02

typedef struct {
  Document doc;
  RedisModuleBlockedClient *bc;
  RedisModuleCtx *thCtx;
  RedisSearchCtx rsCtx;
  int options;
  ConcurrentSearchCtx conc;
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

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc);

RSAddDocumentCtx *NewAddDocumentCtx(RedisModuleCtx *origCtx, IndexSpec *sp);
void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx);

#endif