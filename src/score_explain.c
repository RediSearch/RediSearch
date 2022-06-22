#include "score_explain.h"
#include "rmalloc.h"

// RedisModule_reply.
void RSScoreExplain::SEReply(RedisModuleCtx *ctx) {
  int numChildren = numChildren;

  if (numChildren == 0) {
    RedisModule_ReplyWithSimpleString(ctx, str);
  } else {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, str);
    RedisModule_ReplyWithArray(ctx, numChildren);
    for (int i = 0; i < numChildren; i++) {
      children[i].SEReply(ctx);
    }
  }
}

// Release allocated resources. //@@ should be decostroctor?
void RSScoreExplain::SEDestroy() {
  for (int i = 0; i < numChildren; i++) {
    children[i].SEDestroy();
  }
  rm_free(children);
  rm_free(str);
}
