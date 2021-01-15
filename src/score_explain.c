#include "score_explain.h"
#include "rmalloc.h"
#include "config.h"

static void recExplainReply(RedisModuleCtx *ctx, RSScoreExplain *scrExp, int depth) {
  int numChildren = scrExp->numChildren;

  if (numChildren == 0 ||
     (depth >= REDIS_ARRAY_LIMIT - 1 && !isFeatureSupported(NO_REPLY_DEPTH_LIMIT))) {
    RedisModule_ReplyWithSimpleString(ctx, scrExp->str);
  } else {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, scrExp->str);
    RedisModule_ReplyWithArray(ctx, numChildren);
    for (int i = 0; i < numChildren; i++) {
      recExplainReply(ctx, &scrExp->children[i], depth + 2);
    }
  }
}

static void recExplainDestroy(RSScoreExplain *scrExp) {
  for (int i = 0; i < scrExp->numChildren; i++) {
    recExplainDestroy(&scrExp->children[i]);
  }
  rm_free(scrExp->children);
  rm_free(scrExp->str);
}

void SEReply(RedisModuleCtx *ctx, RSScoreExplain *scrExp) {
  if (scrExp != NULL) {
    recExplainReply(ctx, scrExp, 1);
  }
}

void SEDestroy(RSScoreExplain *scrExp) {
  if (scrExp != NULL) {
    recExplainDestroy(scrExp);
    rm_free(scrExp);
  }
}