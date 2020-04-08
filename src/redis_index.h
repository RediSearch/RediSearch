#ifndef __REDIS_INDEX__
#define __REDIS_INDEX__

#include "document.h"
#include "index.h"
#include "inverted_index.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "spec.h"

#define INVERTED_INDEX_ENCVER 1
#define INVERTED_INDEX_NOFREQFLAG_VER 0

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
int Redis_DropIndex(IndexSpec *spec, int deleteDocuments);

/* Collect memory stas on the index */
int Redis_StatsScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque);

extern RedisModuleType *InvertedIndexType;

void InvertedIndex_Free(void *idx);
void *InvertedIndex_RdbLoad(RedisModuleIO *rdb, int encver);
void InvertedIndex_RdbSave(RedisModuleIO *rdb, void *value);
void InvertedIndex_Digest(RedisModuleDigest *digest, void *value);
int InvertedIndex_RegisterType(RedisModuleCtx *ctx);
unsigned long InvertedIndex_MemUsage(const void *value);

#endif