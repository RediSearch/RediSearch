#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>

typedef struct reducer {
  void *privdata;

  void *(*NewInstance)(void *privdata, const char *name);

  int (*Add)(void *ctx, SearchResult *res);

  int (*Finalize)(void *ctx, SearchResult *res);

  // Free just frees up the processor. If left as NULL we simply use free()
  void (*Free)(struct reducer *r);

  void (*FreeInstance)(void *ctx);
} Reducer;

Reducer *NewCounter();

#endif