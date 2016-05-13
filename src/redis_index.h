#ifndef __REDIS_INDEX__
#define __REDIS_INDEX__

#include "redis_buffer.h"
#include "index.h"
#include "spec.h"
#include "search_ctx.h"

/* Open an index writer on a redis DMA string, for a specific term */
IndexWriter *Redis_OpenWriter(RedisSearchCtx *ctx, const char *term);
/* Close the redis index writer */
void Redis_CloseWriter(IndexWriter *w);

/* Open an inverted index reader on a redis DMA string, for a specific term. 
If singleWordMode is set to 1, we do not load the skip index, only the score index */
IndexReader *Redis_OpenReader(RedisSearchCtx *ctx, const char *term, DocTable *dt, int singleWordMode);
void Redis_CloseReader(IndexReader *r);

/* Load the skip index entry of a redis term */
SkipIndex *Redis_LoadSkipIndex(RedisSearchCtx *ctx, const char *term);



#define TERM_KEY_FORMAT "ft:%s/%s"
#define SKIPINDEX_KEY_FORMAT "si:%s/%s"
#define SCOREINDEX_KEY_FORMAT "ss:%s/%s"

/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisSearchCtx *ctx, const char *term);
RedisModuleString *fmtRedisSkipIndexKey(RedisSearchCtx *ctx, const char *term);
/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisSearchCtx *ctx, const char *term);
void Redis_CloseWriter(IndexWriter *w);



// A key mapping docId => docKey string
#define REDISINDEX_DOCIDS_MAP "__redis_docIds__"
// A key mapping docKey => internal docId
#define REDISINDEX_DOCKEY_MAP "__redis_docKeys__"
// The counter incrementing internal docIds
#define REDISINDEX_DOCIDCOUNTER "__redis_docIdCounter__"

t_docId Redis_GetDocId(RedisSearchCtx *ctx, RedisModuleString *docKey, int *isnew); 
RedisModuleString *Redis_GetDocKey(RedisSearchCtx *ctx, t_docId docId);

typedef struct {
    RedisModuleString *name;
    RedisModuleString *text;
} DocumentField;

typedef struct {
    RedisModuleString *docKey;
    DocumentField *fields;
    int numFields;
    float score; 
} Document;

void Document_Free(Document doc);

/* Load a single document */
int Redis_LoadDocument(RedisSearchCtx *ctx, RedisModuleString *key, Document *Doc);

/* Load a bunch of documents from redis */
Document *Redis_LoadDocuments(RedisSearchCtx *ctx, RedisModuleString **key, int numKeys, int *nump);

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc);



#endif