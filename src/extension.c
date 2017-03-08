#include "extension.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "redismodule.h"
#include "index_result.h"
#include "dep/triemap/triemap.h"
#include "query.h"

TrieMap *__queryExpanders = NULL;
TrieMap *__scorers = NULL;

void Extensions_Init() {
  __queryExpanders = NewTrieMap();
  __scorers = NewTrieMap();
}

int Ext_RegisterScoringFunction(const char *alias, RSScoringFunction func, void *privdata) {
  if (func == NULL) {
    return REDISEARCH_ERR;
  }
  ExtensionsScoringFunctionCtx *ctx = rm_new(ExtensionsScoringFunctionCtx);
  ctx->privdata = privdata;
  ctx->sf = func;
  if (TrieMap_Find(__scorers, (char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    return REDISEARCH_ERR;
  }
  TrieMap_Add(__scorers, (char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

int Ext_RegisterQueryExpander(const char *alias, RSQueryTokenExpander exp, void *privdata) {
  if (exp == NULL) {
    return REDISEARCH_ERR;
  }
  ExtensionsQueryExpanderCtx *ctx = rm_new(ExtensionsQueryExpanderCtx);
  ctx->privdata = privdata;
  ctx->exp = exp;
  if (TrieMap_Find(__scorers, (char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    return REDISEARCH_ERR;
  }
  TrieMap_Add(__scorers, (char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

int Extension_Load(const char *name, RSExtensionInitFunc func) {
  printf("Loading extension %s\n", name);
  RSExtensionCtx ctx = {
      .RegisterScoringFunction = Ext_RegisterScoringFunction,
      .RegisterQueryExpander = Ext_RegisterQueryExpander,
  };

  return func(&ctx);
}

int Ext_ScorerGetSlop(RSIndexResult *r) {
  return IndexResult_MinOffsetDelta(r);
}

RSScoringFunction Extensions_GetScoringFunction(RSScoringFunctionCtx *ctx, const char *name) {

  ExtensionsScoringFunctionCtx *p = TrieMap_Find(__scorers, (char *)name, strlen(name));
  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    ctx->privdata = p->privdata;
    ctx->GetSlop = Ext_ScorerGetSlop;
    return p->sf;
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
    qn = un;
  }
  QueryUnionNode_AddChild(&qn->un, NewTokenNode(q, str, len));
  q->numTokens++;
}

void Ext_SetPayload(struct RSQueryExpanderCtx *ctx, RSPayload payload) {
  ctx->query->payload = payload;
}

RSQueryTokenExpander Extensions_GetQueryExpander(RSQueryExpanderCtx *ctx, const char *name) {
  ExtensionsQueryExpanderCtx *p = TrieMap_Find(__queryExpanders, (char *)name, strlen(name));
  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    ctx->privdata = p->privdata;
    return p->exp;
  }
  return NULL;
}
