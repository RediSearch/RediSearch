#include "scoreExplain.h"

struct RSScoreExplain{
  char *str;
  int numChildren;
  RSScoreExplain **children;
};

static RSScoreExplain *recExplainExtractStrings(RSIndexResult *inxRes) {
  RSScoreExplain *scrExp = (RSScoreExplain *)calloc(1, sizeof(RSScoreExplain));
  scrExp->str = inxRes->scoreExplainStr;
  inxRes->scoreExplainStr = NULL;
  
  if (inxRes->type & (RSResultType_Intersection | RSResultType_Union) &&
      inxRes->agg.children[0]->scoreExplainStr != NULL) { // Some children don't get love
    int numChildren = inxRes->agg.numChildren;
    scrExp->numChildren = numChildren;
    scrExp->children = (RSScoreExplain **)calloc(numChildren, sizeof(RSScoreExplain *));
    for (int i = 0; i < numChildren; i++) {
      scrExp->children[i] = recExplainExtractStrings(inxRes->agg.children[i]);
    }
  } else { 
    scrExp->numChildren = 0; 
    scrExp->children = NULL; 
  }
  
  return scrExp;
}

static void recExplainReply(RedisModuleCtx *ctx, RSScoreExplain *scrExp) {
  int numChildren = scrExp->numChildren;

  if (numChildren == 0) {
    RedisModule_ReplyWithSimpleString(ctx, scrExp->str);
  } else {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithSimpleString(ctx, scrExp->str);
    RedisModule_ReplyWithArray(ctx, numChildren);
    for(int i = 0; i < numChildren; i++) {
      recExplainReply(ctx, scrExp->children[i]);
    }
  }
}

static void recExplainDestroy(RSScoreExplain *scrExp) {
  for(int i = 0; i < scrExp->numChildren; i++) {
    recExplainDestroy(scrExp->children[i]);
  }
  free(scrExp->children);
  free(scrExp->str);
  free(scrExp);
}

RSScoreExplain *SEExtractStrings(RSIndexResult *inxRes) {
  if (inxRes->scoreExplainStr == NULL) { return NULL; }
  return recExplainExtractStrings(inxRes);
}

void SEReply(RedisModuleCtx *ctx, RSScoreExplain *scrExp){
  if (scrExp != NULL) {
    recExplainReply(ctx, scrExp);
  }
}

void SEDestroy(RSScoreExplain *scrExp) {
  if (scrExp != NULL) {
    recExplainDestroy(scrExp);
  }
}