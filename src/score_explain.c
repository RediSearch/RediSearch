#include "score_explain.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

void RSScoreExplain::RMReply(RedisModuleCtx *ctx) {
  int numChildren = numChildren;

  if (children.empty()) {
    RedisModule_ReplyWithSimpleString(ctx, str);
  } else {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, str);
    RedisModule_ReplyWithArray(ctx, children.size());
    for (auto chi: children) {
      chi->RMReply(ctx);
    }
  }
}

//---------------------------------------------------------------------------------------------

void RSScoreExplain::SEDestroy() {
  for (auto chi: children) {
    delete *chi;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
