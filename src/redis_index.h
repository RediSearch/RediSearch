#ifndef __REDIS_INDEX__
#define __REDIS_INDEX__

#include "redis_buffer.h"
#include "index.h"


IndexWriter *Redis_OpenWriter(RedisModuleCtx *ctx, const char *term);
void Redis_CloseWriter(IndexWriter *w);
IndexReader *Redis_OpenReader(RedisModuleCtx *ctx, const char *term, DocTable *dt);
void Redis_CloseReader(IndexReader *r);
SkipIndex *Redis_LoadSkipIndex(RedisModuleCtx *ctx, const char *term);



#define TERM_KEY_FORMAT "ft:%s"
#define SKIPINDEX_KEY_FORMAT "si:%s"
/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisModuleCtx *ctx, const char *term);
RedisModuleString *fmtRedisSkipIndexKey(RedisModuleCtx *ctx, const char *term);
/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisModuleCtx *ctx, const char *term);
void Redis_CloseWriter(IndexWriter *w);



// A key mapping docId => docKey string
#define REDISINDEX_DOCIDS_MAP "__redis_docIds__"
// A key mapping docKey => internal docId
#define REDISINDEX_DOCKEY_MAP "__redis_docKeys__"
// The counter incrementing internal docIds
#define REDISINDEX_DOCIDCOUNTER "__redis_docIdCounter__"

t_docId Redis_GetDocId(RedisModuleCtx *ctx, RedisModuleString *docKey, int *isnew); 
RedisModuleString *Redis_GetDocKey(RedisModuleCtx *ctx, t_docId docId);

typedef struct {
    const char *name;
    const char *text;
} DocumentField;

typedef struct {
    RedisModuleString *docKey;
    DocumentField *fields;
    int numFields;
    float score; 
} Document;




#endif