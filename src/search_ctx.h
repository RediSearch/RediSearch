
#pragma once

#include "redismodule.h"
#include "spec.h"
#include "document.h"

#include <sched.h>
#include <time.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

struct AddDocumentOptions;
typedef uint64_t IndexSpecId;

// Context passed to all redis related search handling functions

struct RedisSearchCtx {
  RedisModuleCtx *redisCtx;
  RedisModuleKey *key_;
  IndexSpec *spec;
  IndexSpecId specId;  // Unique id of the spec; used when refreshing

  RedisSearchCtx(RedisModuleCtx *ctx, IndexSpecId specId);
  RedisSearchCtx(RedisModuleCtx *ctx, const IndexSpec *spec_);
  RedisSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);
  RedisSearchCtx(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);
  RedisSearchCtx(const RedisSearchCtx &sctx);
  ~RedisSearchCtx();

  void ctor(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

  void Refresh(RedisModuleString *keyName);

  RedisModuleString *TermKeyName(const char *term, size_t len);
  RedisModuleString *SkipIndexKeyName(const char *term, size_t len);
  RedisModuleString *ScoreIndexKeyName(const char *term, size_t len);
  RedisModuleString *NumericIndexKey(const char *field);

  int AddDocument(RedisModuleString *name, const AddDocumentOptions *opts, QueryError *status);
};

// #define SEARCH_CTX_STATIC(ctx, sp) \
//   { ctx, NULL, sp, 0, 1 }

//@@ Is it truly static?
RedisSearchCtx SEARCH_CTX_STATIC(RedisModuleCtx *ctx, IndexSpec *sp) {
  return *new RedisSearchCtx(ctx, sp);
}

#if 0
#define SEARCH_CTX_SORTABLES(ctx) ((ctx && ctx->spec) ? ctx->spec->sortables : NULL)

#define SearchCtx_Incref(sctx) \
  ({                           \
    (sctx)->refcount++;        \
    sctx;                      \
  })

#define SearchCtx_Decref(sctx) \
  if (!--((sctx)->refcount)) { \
    delete sctx;      \
  }
#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
