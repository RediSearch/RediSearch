#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>
#include <util/block_alloc.h>

typedef struct {
  void *privdata;
  RedisSearchCtx *ctx;
  BlkAlloc alloc;

} ReducerCtx;

static inline void *ReducerCtx_Alloc(ReducerCtx *ctx, size_t sz, size_t blkSz) {
  return BlkAlloc_Alloc(&ctx->alloc, sz, blkSz);
}

typedef struct reducer {
  ReducerCtx ctx;

  const char *alias;

  void *(*NewInstance)(ReducerCtx *ctx);

  int (*Add)(void *ctx, SearchResult *res);

  int (*Finalize)(void *ctx, const char *key, SearchResult *res);

  // Free just frees up the processor. If left as NULL we simply use free()
  void (*Free)(struct reducer *r);

  void (*FreeInstance)(void *ctx);
} Reducer;

static Reducer *NewReducer(RedisSearchCtx *ctx, const char *alias, void *privdata) {
  Reducer *r = malloc(sizeof(*r));
  r->alias = alias;
  r->ctx.ctx = ctx;
  r->ctx.privdata = privdata;
  BlkAlloc_Init(&r->ctx.alloc);
  return r;
}

Reducer *NewCount(RedisSearchCtx *ctx, const char *alias);

Reducer *NewSum(RedisSearchCtx *ctx, const char *property, const char *alias);

Reducer *NewToList(RedisSearchCtx *ctx, const char *property, const char *alias);

Reducer *NewMin(RedisSearchCtx *, const char *, const char *);
Reducer *NewMax(RedisSearchCtx *, const char *, const char *);
Reducer *NewAvg(RedisSearchCtx *, const char *, const char *);
Reducer *NewCountDistinct(RedisSearchCtx *, const char *, const char *);
Reducer *NewCountDistinctish(RedisSearchCtx *, const char *, const char *);
Reducer *NewQuantile(RedisSearchCtx *, const char *, const char *, double);
Reducer *NewStddev(RedisSearchCtx *, const char *, const char *);
Reducer *GetReducer(RedisSearchCtx *ctx, const char *name, const char *alias, CmdArray *args,
                    char **err);

#endif