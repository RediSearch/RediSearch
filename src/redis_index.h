#ifndef __REDIS_INDEX__
#define __REDIS_INDEX__

#include "redis_buffer.h"
#include "index.h"
#include "spec.h"
#include "search_ctx.h"
#include "document.h"

/* Open an index writer on a redis DMA string, for a specific term */
IndexWriter *Redis_OpenWriter(RedisSearchCtx *ctx, const char *term, size_t len);
/* Close the redis index writer */
void Redis_CloseWriter(IndexWriter *w);

/* Open an inverted index reader on a redis DMA string, for a specific term.
If singleWordMode is set to 1, we do not load the skip index, only the score index */
IndexReader *Redis_OpenReader(RedisSearchCtx *ctx, const char *term, size_t len, DocTable *dt,
                              int singleWordMode, u_char fieldMask);
void Redis_CloseReader(IndexReader *r);

/* Load the skip index entry of a redis term */
SkipIndex *Redis_LoadSkipIndex(RedisSearchCtx *ctx, const char *term, size_t len);

#define TERM_KEY_FORMAT "ft:%s/%.*s"
#define SKIPINDEX_KEY_FORMAT "si:%s/%.*s"
#define SCOREINDEX_KEY_FORMAT "ss:%s/%.*s"

typedef int (*ScanFunc)(RedisModuleCtx *ctx, RedisModuleString *keyName, void *opaque);

/* Scan the keyspace with MATCH for a prefix, and call ScanFunc for each key found */
int Redis_ScanKeys(RedisModuleCtx *ctx, const char *prefix, ScanFunc f, void *opaque);

/* Optimize the buffers of a speicif term hit */
int Redis_OptimizeScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);

/* Drop the index and all the associated keys.
*
*  If deleteDocuments is non zero, we will delete the saved documents (if they exist).
*  Only set this if there are no other indexes in the same redis instance.
*/
int Redis_DropIndex(RedisSearchCtx *ctx, int deleteDocuments);

/* Drop all the index's internal keys using this scan handler */
int Redis_DropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);

/* Collect memory stas on the index */
int Redis_StatsScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);
/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisSearchCtx *ctx, const char *term, size_t len);
RedisModuleString *fmtRedisSkipIndexKey(RedisSearchCtx *ctx, const char *term, size_t len);
/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisSearchCtx *ctx, const char *term, size_t len);

void Redis_CloseWriter(IndexWriter *w);

#endif