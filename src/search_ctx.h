
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
  // uint32_t refcount;
  uint64_t specId;  // Unique id of the spec; used when refreshing

  RedisSearchCtx(RedisModuleCtx *ctx);
  RedisSearchCtx(RedisModuleCtx *ctx, struct IndexSpec *spec);
  RedisSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);
  RedisSearchCtx(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);
  RedisSearchCtx(const RedisSearchCtx &sctx);
  ~RedisSearchCtx();

  void ctor(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

  void Refresh(RedisModuleString *keyName);

  RedisModuleString *TermKeyName(const char *term, size_t len);
  RedisModuleString *SkipIndexKeyName(const char *term, size_t len);
  RedisModuleString *NumericIndexKeyName(const char *field);
};

#define SEARCH_CTX_STATIC(ctx, sp) \
  { ctx, NULL, sp, 0, 1 }

#if 0
#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)

#define SearchCtx_Incref(sctx) \
  ({                           \
    (sctx)->refcount++;        \
    sctx;                      \
  })

#define SearchCtx_Decref(sctx) \
  if (!--((sctx)->refcount)) { \
    SearchCtx_Free(sctx);      \
  }
#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
