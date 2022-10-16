
#include "score_explain.h"
#include "rmalloc.h"
#include "config.h"

///////////////////////////////////////////////////////////////////////////////////////////////

ScoreExplain::ScoreExplain(ScoreExplain *exp) {
  if (exp) {
    children.push_back(exp);
  }
}

//---------------------------------------------------------------------------------------------

void ScoreExplain::RMReply(RedisModuleCtx *ctx, int depth) {
  if (children.empty() || depth == REDIS_ARRAY_LIMIT && !isFeatureSupported(NO_REPLY_DEPTH_LIMIT)) {
    RedisModule_ReplyWithSimpleString(ctx, str.data());
  } else {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, str.data());
    RedisModule_ReplyWithArray(ctx, children.size());
    for (auto chi: children) {
      chi->RMReply(ctx, depth + 2);
    }
  }
}

//---------------------------------------------------------------------------------------------

ScoreExplain::~ScoreExplain() {
  for (auto chi: children) {
    delete chi;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
