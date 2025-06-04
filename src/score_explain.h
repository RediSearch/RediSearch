/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_SCORE_EXPLAIN_H_
#define RS_SCORE_EXPLAIN_H_

#include "redismodule.h"
#include "redisearch.h"
#include "reply.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RSScoreExplain {
  char *str;
  int numChildren;
  struct RSScoreExplain *children;
} RSScoreExplain;

/*
 * RedisModule_reply.
 */
void SEReply(RedisModule_Reply *reply, RSScoreExplain *scrExp);

/*
 * Release allocated resources.
 */
void SEDestroy(RSScoreExplain *scrExp);


#define EXPLAIN(exp, fmt, args...) \
  {                                \
    if (exp) {                     \
      explain(exp, fmt, ##args);   \
    }                              \
  }

void explain(RSScoreExplain *scrExp, char *fmt, ...);

#ifdef __cplusplus
}
#endif
#endif  // RS_SCORE_EXPLAIN_H_