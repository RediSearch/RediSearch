#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>

typedef struct {
  void *privdata;
  RedisSearchCtx *ctx;
} ReducerCtx;

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