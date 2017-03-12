#include "extension.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "redismodule.h"
#include "index_result.h"
#include "dep/triemap/triemap.h"
#include "query.h"

/* The registry for query expanders. Initialized by Extensions_Init() */
TrieMap *__queryExpanders = NULL;

/* The registry for scorers. Initialized by Extensions_Init() */
TrieMap *__scorers = NULL;

/* Init the extension system - currently just create the regsistries */
void Extensions_Init() {
  __queryExpanders = NewTrieMap();
  __scorers = NewTrieMap();
}

/* Register a scoring function by its alias. privdata is an optional pointer to a user defined
 * struct. ff is a free function releasing any resources allocated at the end of query execution */
int Ext_RegisterScoringFunction(const char *alias, RSScoringFunction func, RSFreeFunction ff,
                                void *privdata) {
  if (func == NULL) {
    return REDISEARCH_ERR;
  }
  ExtScoringFunctionCtx *ctx = rm_new(ExtScoringFunctionCtx);
  ctx->privdata = privdata;
  ctx->ff = ff;
  ctx->sf = func;

  if (TrieMap_Find(__scorers, (char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    rm_free(ctx);
    return REDISEARCH_ERR;
  }
  TrieMap_Add(__scorers, (char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

int Ext_RegisterQueryExpander(const char *alias, RSQueryTokenExpander exp, RSFreeFunction ff,
                              void *privdata) {
  if (exp == NULL) {
    return REDISEARCH_ERR;
  }
  ExtQueryExpanderCtx *ctx = rm_new(ExtQueryExpanderCtx);
  ctx->privdata = privdata;
  ctx->ff = ff;
  ctx->exp = exp;
  if (TrieMap_Find(__scorers, (char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    rm_free(ctx);
    return REDISEARCH_ERR;
  }
  TrieMap_Add(__queryExpanders, (char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

int Extension_Load(const char *name, RSExtensionInitFunc func) {
  RSExtensionCtx ctx = {
      .RegisterScoringFunction = Ext_RegisterScoringFunction,
      .RegisterQueryExpander = Ext_RegisterQueryExpander,
  };

  return func(&ctx);
}

int Ext_ScorerGetSlop(RSIndexResult *r) {
  return IndexResult_MinOffsetDelta(r);
}

ExtScoringFunctionCtx *Extensions_GetScoringFunction(RSScoringFunctionCtx *ctx, const char *name) {

  if (!__scorers) return NULL;

  ExtScoringFunctionCtx *p = TrieMap_Find(__scorers, (char *)name, strlen(name));
  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    ctx->privdata = p->privdata;
    ctx->GetSlop = Ext_ScorerGetSlop;
    return p;
  }
  return NULL;
}

void Ext_ExpandToken(struct RSQueryExpanderCtx *ctx, const char *str, size_t len,
                     RSTokenFlags flags) {

  Query *q = ctx->query;
  QueryNode *qn = *ctx->currentNode;

  if (qn->type != QN_UNION) {
    QueryNode *un = NewUnionNode();
    QueryUnionNode_AddChild(&un->un, qn);
    *ctx->currentNode = un;
  }
  QueryUnionNode_AddChild(&(*ctx->currentNode)->un, NewTokenNode(q, str, len));
  q->numTokens++;
}

void Ext_SetPayload(struct RSQueryExpanderCtx *ctx, RSPayload payload) {
  ctx->query->payload = payload;
}

ExtQueryExpanderCtx *Extensions_GetQueryExpander(RSQueryExpanderCtx *ctx, const char *name) {

  if (!__queryExpanders) return NULL;

  ExtQueryExpanderCtx *p = TrieMap_Find(__queryExpanders, (char *)name, strlen(name));
  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    ctx->ExpandToken = Ext_ExpandToken;
    ctx->SetPayload = Ext_SetPayload;
    ctx->privdata = p->privdata;
    return p;
  }
  return NULL;
}
