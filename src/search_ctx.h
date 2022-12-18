
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
  RedisModuleKey *key;
  IndexSpec *spec;
  IndexSpecId specId;  // Unique id of the spec; used when refreshing

  RedisSearchCtx(RedisModuleCtx *ctx, IndexSpecId specId);
  RedisSearchCtx(RedisModuleCtx *ctx, IndexSpec *spec);
  RedisSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL);
  RedisSearchCtx(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);
  RedisSearchCtx(const RedisSearchCtx &sctx);
  ~RedisSearchCtx();

  void ctor(RedisModuleCtx *ctx, const char *indexName, bool resetTTL);

  void Refresh(RedisModuleString *keyName);

  RedisModuleString *TermKeyName(const String& term);
  RedisModuleString *SkipIndexKeyName(const char *term, size_t len);
  RedisModuleString *ScoreIndexKeyName(const char *term, size_t len);
  RedisModuleString *NumericIndexKey(String field);

  int AddDocument(RedisModuleString *name, const AddDocumentOptions &opts, QueryError *status);
};

#if 0
#define SEARCH_CTX_STATIC(ctx, sp) ctx, sp
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
