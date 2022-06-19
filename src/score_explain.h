#pragma once

#include "redismodule.h"
#include "redisearch.h"

struct RSScoreExplain {
  char *str;
  int numChildren;
  struct RSScoreExplain *children;

  void SEReply(RedisModuleCtx *ctx);
  void SEDestroy();
};
