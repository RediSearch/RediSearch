#pragma once

#include "redismodule.h"
#include "redisearch.h"
#include "rmutil/vector.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct ScoreExplain {
  ScoreExplain(ScoreExplain *exp = NULL);
  ~ScoreExplain();

  String str;
  Vector<struct ScoreExplain*> children;

  void explain(char *fmt, ...);

  void RMReply(RedisModuleCtx *ctx, int depth = 1);
};

///////////////////////////////////////////////////////////////////////////////////////////////
