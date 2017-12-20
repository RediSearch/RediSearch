#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <rmutil/cmdparse.h>

typedef struct reducer {
  void *privdata;

  const char *alias;

  void *(*NewInstance)(void *privdata);

  int (*Add)(void *ctx, SearchResult *res);

  int (*Finalize)(void *ctx, const char *key, SearchResult *res);

  // Free just frees up the processor. If left as NULL we simply use free()
  void (*Free)(struct reducer *r);

  void (*FreeInstance)(void *ctx);
} Reducer;

Reducer *NewCount(const char *alias);
Reducer *NewCountArgs(CmdArray *args, char **err);

Reducer *NewSum(const char *property, const char *alias);
Reducer *NewSumArgs(CmdArray *args, char **err);

Reducer *GetReducer(const char *name, CmdArray *args, char **err);

#endif