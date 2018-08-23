#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>
#include <util/block_alloc.h>
#include "query_error.h"

/* Maximum possible value to random sample group size */
#define MAX_SAMPLE_SIZE 1000

typedef struct {
  void *privdata;
  const char *property;
  RedisSearchCtx *ctx;
  BlkAlloc alloc;
} ReducerCtx;

static inline void *ReducerCtx_Alloc(ReducerCtx *ctx, size_t sz, size_t blkSz) {
  return BlkAlloc_Alloc(&ctx->alloc, sz, blkSz);
}

typedef struct reducer {
  ReducerCtx ctx;

  char *alias;

  void *(*NewInstance)(ReducerCtx *ctx);

  int (*Add)(void *ctx, SearchResult *res);

  int (*Finalize)(void *ctx, const char *key, SearchResult *res);

  // Free just frees up the processor. If left as NULL we simply use free()
  void (*Free)(struct reducer *r);

  void (*FreeInstance)(void *ctx);
} Reducer;

static inline void Reducer_GenericFree(Reducer *r) {
  BlkAlloc_FreeAll(&r->ctx.alloc, NULL, 0, 0);
  free(r->ctx.privdata);
  free(r->alias);
  free(r);
}

/**
 * Exactly like GenericFree, but doesn't free private data.
 */
static inline void Reducer_GenericFreeWithStaticPrivdata(Reducer *r) {
  BlkAlloc_FreeAll(&r->ctx.alloc, NULL, 0, 0);
  free(r->alias);
  free(r);
}

static Reducer *NewReducer(RedisSearchCtx *ctx, void *privdata) {
  Reducer *r = malloc(sizeof(*r));
  r->ctx.ctx = ctx;
  r->ctx.privdata = privdata;
  BlkAlloc_Init(&r->ctx.alloc);
  return r;
}

// Format a function name in the form of s(arg). Returns a pointer for use with 'free'
static inline char *FormatAggAlias(const char *alias, const char *fname, const char *propname) {
  if (alias) {
    return strdup(alias);
  }

  if (!propname || *propname == 0) {
    return strdup(fname);
  }

  char *s = NULL;
  asprintf(&s, "%s(%s)", fname, propname);
  return s;
}

Reducer *NewCount(RedisSearchCtx *ctx, const char *alias);

Reducer *NewSum(RedisSearchCtx *ctx, const char *property, const char *alias);

Reducer *NewToList(RedisSearchCtx *ctx, const char *property, const char *alias);

Reducer *NewMin(RedisSearchCtx *, const char *, const char *);
Reducer *NewMax(RedisSearchCtx *, const char *, const char *);
Reducer *NewAvg(RedisSearchCtx *, const char *, const char *);
Reducer *NewCountDistinct(RedisSearchCtx *, const char *, const char *);
Reducer *NewCountDistinctish(RedisSearchCtx *, const char *, const char *);
Reducer *NewQuantile(RedisSearchCtx *, const char *, const char *, double, size_t);
Reducer *NewStddev(RedisSearchCtx *, const char *, const char *);
Reducer *GetReducer(RedisSearchCtx *ctx, const char *name, const char *alias, RSValue **args,
                    size_t argc, QueryError *err);
RSValueType GetReducerType(const char *name);
Reducer *NewFirstValue(RedisSearchCtx *ctx, const char *key, const char *sortKey, int asc,
                       const char *alias);
Reducer *NewRandomSample(RedisSearchCtx *sctx, int size, const char *property, const char *alias);
Reducer *NewHLL(RedisSearchCtx *ctx, const char *alias, const char *key);
Reducer *NewHLLSum(RedisSearchCtx *ctx, const char *alias, const char *key);

#endif