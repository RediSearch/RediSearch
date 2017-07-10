#ifndef __RS_DOCUMENT_H__
#define __RS_DOCUMENT_H__
#include "redismodule.h"
#include "search_ctx.h"
#include "redisearch.h"

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
} Document;

Document NewDocument(RedisModuleString *docKey, double score, int numFields, const char *lang,
                     const char *payload, size_t payloadSize);

void Document_Free(Document doc);

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

#endif