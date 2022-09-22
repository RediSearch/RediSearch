
#include "score_explain.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

ScoreExplain::ScoreExplain(ScoreExplain *exp) {
  if (exp) {
    children.push_back(exp);
  }
}

//---------------------------------------------------------------------------------------------

void ScoreExplain::RMReply(RedisModuleCtx *ctx) {
  if (children.empty()) {
    RedisModule_ReplyWithSimpleString(ctx, str.data());
  } else {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, str.data());
    RedisModule_ReplyWithArray(ctx, children.size());
    for (auto chi: children) {
      chi->RMReply(ctx);
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
