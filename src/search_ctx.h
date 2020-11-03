
#pragma once

//#include "spec.h"
#include "redismodule.h"

#include <sched.h>
#include <time.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

// Context passed to all redis related search handling functions
struct RedisSearchCtx {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key_;
  struct IndexSpec *spec;
  uint32_t refcount;
  int isStatic;
  uint64_t specId;  // Unique id of the spec; used when refreshing
};

#define SEARCH_CTX_STATIC(ctx, sp) \
  { ctx, NULL, sp, 0, 1 }

#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)

// Create a string context on the heap
RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);
RedisSearchCtx *NewSearchCtxDefault(RedisModuleCtx *ctx);

RedisSearchCtx *SearchCtx_Refresh(RedisSearchCtx *sctx, RedisModuleString *keyName);

// Same as above, only from c string (null terminated)
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

#define SearchCtx_Incref(sctx) \
  ({                           \
    (sctx)->refcount++;        \
    sctx;                      \
  })

#define SearchCtx_Decref(sctx) \
  if (!--((sctx)->refcount)) { \
    SearchCtx_Free(sctx);      \
  }

void SearchCtx_Free(RedisSearchCtx *sctx);

/**
 * Format redis key for a term.
 * TODO: Add index name to it
 */
RedisModuleString *fmtRedisTermKey(RedisSearchCtx *ctx, const char *term, size_t len);
RedisModuleString *fmtRedisSkipIndexKey(RedisSearchCtx *ctx, const char *term, size_t len);
RedisModuleString *fmtRedisNumericIndexKey(RedisSearchCtx *ctx, const char *field);

///////////////////////////////////////////////////////////////////////////////////////////////
