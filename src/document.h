#ifndef __RS_DOCUMENT_H__
#define __RS_DOCUMENT_H__
#include "redismodule.h"
#include "search_ctx.h"
#include "types.h"

typedef struct {
    RedisModuleString *name;
    RedisModuleString *text;
} DocumentField;

typedef struct {
    RedisModuleString *docKey;
    DocumentField *fields;
    int numFields;
    float score; 
    const char *language;
    t_docId docId;
} Document;

void Document_Free(Document doc);

/* Load a single document */
int Redis_LoadDocument(RedisSearchCtx *ctx, RedisModuleString *key, Document *Doc);

/* Load a bunch of documents from redis */
Document *Redis_LoadDocuments(RedisSearchCtx *ctx, RedisModuleString **key, int numKeys, int *nump);

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc);



#endif