#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>

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

Reducer *NewCounter(const char *alias);

Reducer *NewSummer(const char *property, const char *alias);

#endif