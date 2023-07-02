/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "score_explain.h"
#include "rmalloc.h"
#include "config.h"

static void recExplainReply(RedisModule_Reply *reply, RSScoreExplain *scrExp, int depth) {
  int numChildren = scrExp->numChildren;

  if (numChildren == 0 ||
     (depth >= REDIS_ARRAY_LIMIT - 1 && !isFeatureSupported(NO_REPLY_DEPTH_LIMIT))) {
    RedisModule_Reply_SimpleString(reply, scrExp->str);
  } else {
    RedisModule_Reply_Array(reply);
      RedisModule_ReplyKV_Array(reply, scrExp->str);
      for (int i = 0; i < numChildren; i++) {
        recExplainReply(reply, &scrExp->children[i], depth + 2);
      }
      RedisModule_Reply_ArrayEnd(reply);
    RedisModule_Reply_ArrayEnd(reply);
  }
}

static void recExplainDestroy(RSScoreExplain *scrExp) {
  for (int i = 0; i < scrExp->numChildren; i++) {
    recExplainDestroy(&scrExp->children[i]);
  }
  rm_free(scrExp->children);
  rm_free(scrExp->str);
}

void SEReply(RedisModule_Reply *reply, RSScoreExplain *scrExp) {
  if (scrExp != NULL) {
    recExplainReply(reply, scrExp, 1);
  }
}

void SEDestroy(RSScoreExplain *scrExp) {
  if (scrExp != NULL) {
    recExplainDestroy(scrExp);
    rm_free(scrExp);
  }
}