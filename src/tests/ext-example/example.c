#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include <redisearch.h>
#include "example.h"
#include "rmalloc.h"

struct privdata {
  int freed;
};

/* Calculate sum(TF-IDF)*document score for each result */
double myScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                double minScore) {
  return 3.141;
}

double filterOutScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                       double minScore) {
  return RS_SCORE_FILTEROUT;
}

void myExpander(RSQueryExpanderCtx *ctx, RSToken *token) {
  ctx->ExpandToken(ctx, rm_strdup("foo"), 3, 0x00ff);
}

int numFreed = 0;
void myFreeFunc(void *p) {
  // printf("Freeing %p\n", p);
  numFreed++;
  rm_free(p);
}

/* Register the default extension */
int RS_ExtensionInit(RSExtensionCtx *ctx) {

  struct privdata *spd = rm_malloc(sizeof(struct privdata));
  spd->freed = 0;
  if (ctx->RegisterScoringFunction("example_scorer", myScorer, myFreeFunc, spd) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  if (ctx->RegisterScoringFunction("filterout_scorer", filterOutScorer, myFreeFunc, spd) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  spd = rm_malloc(sizeof(struct privdata));
  spd->freed = 0;
  /* Snowball Stemmer is the default expander */
  if (ctx->RegisterQueryExpander("example_expander", myExpander, myFreeFunc, spd) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}
