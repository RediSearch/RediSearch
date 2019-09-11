#ifndef RS_SCORE_EXPLAIN_H_
#define RS_SCORE_EXPLAIN_H_
#include "redismodule.h"
#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RSScoreExplain RSScoreExplain;

/*
 * Extract explanation strings from temporary RSIndexResult.
 */
RSScoreExplain *SEExtractStrings(RSIndexResult *inxRes);

/*
 * RedisModule_reply.
 */
void SEReply(RedisModuleCtx *ctx, RSScoreExplain *scrExp);

/*
 * Release allocated resources.
 */
void SEDestroy(RSScoreExplain *scrExp);


#ifdef __cplusplus
}
#endif
#endif // RS_SCORE_EXPLAIN_H_