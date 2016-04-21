#ifndef __REDIS_INDEX__
#define __REDIS_INDEX__

#include "redis_buffer.h"
#include "index.h"


IndexWriter *Redis_OpenWriter(RedisModuleCtx *ctx, const char *term);
void Redis_CloseWriter(IndexWriter *w);
IndexReader *Redis_OpenReader(RedisModuleCtx *ctx, const char *term);
void Redis_CloseReader(IndexReader *r);


#define TERM_KEY_FORMAT "ft:%s"
/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisModuleCtx *ctx, const char *term);
/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisModuleCtx *ctx, const char *term);
void Redis_CloseWriter(IndexWriter *w);
SkipIndex *Redis_LoadSkipIndex(RedisModuleCtx *ctx, const char *term);
IndexReader *Redis_OpenReader(RedisModuleCtx *ctx, const char *term);
void Redis_CloseReader(IndexReader *r);


#define REDISINDEX_DOCIDS_MAP "__redis_docIds__"
#define REDISINDEX_DOCIDCOUNTER "__redis_docIdCounter__"

t_docId Redis_GetDocId(RedisModuleCtx *ctx, RedisModuleString *docKey, int *isnew); 
RedisModuleString *Redis_GetDocKey(RedisModuleCtx *ctx, t_docId docId);
#endif