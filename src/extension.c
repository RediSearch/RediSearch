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
  if (func == NULL || __scorers == NULL) {
    return REDISEARCH_ERR;
  }
  ExtScoringFunctionCtx *ctx = rm_new(ExtScoringFunctionCtx);
  ctx->privdata = privdata;
  ctx->ff = ff;
  ctx->sf = func;

  /* Make sure that two scorers are never registered under the same name */
  if (TrieMap_Find(__scorers, (char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    rm_free(ctx);
    return REDISEARCH_ERR;
  }

  TrieMap_Add(__scorers, (char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

/* Register a aquery expander */
int Ext_RegisterQueryExpander(const char *alias, RSQueryTokenExpander exp, RSFreeFunction ff,
                              void *privdata) {
  if (exp == NULL || __queryExpanders == NULL) {
    return REDISEARCH_ERR;
  }
  ExtQueryExpanderCtx *ctx = rm_new(ExtQueryExpanderCtx);
  ctx->privdata = privdata;
  ctx->ff = ff;
  ctx->exp = exp;

  /* Make sure there are no two query expanders under the same name */
  if (TrieMap_Find(__queryExpanders, (char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    rm_free(ctx);
    return REDISEARCH_ERR;
  }
  TrieMap_Add(__queryExpanders, (char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

/* Load an extension by calling its init function. return REDISEARCH_ERR or REDISEARCH_OK */
int Extension_Load(const char *name, RSExtensionInitFunc func) {
  // bind the callbacks in the context
  RSExtensionCtx ctx = {
      .RegisterScoringFunction = Ext_RegisterScoringFunction,
      .RegisterQueryExpander = Ext_RegisterQueryExpander,
  };

  return func(&ctx);
}

/* Get a scoring function by name */
ExtScoringFunctionCtx *Extensions_GetScoringFunction(RSScoringFunctionCtx *ctx, const char *name) {

  if (!__scorers) return NULL;

  /* lookup the scorer by name (case sensitive) */
  ExtScoringFunctionCtx *p = TrieMap_Find(__scorers, (char *)name, strlen(name));
  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    ctx->privdata = p->privdata;
    ctx->GetSlop = IndexResult_MinOffsetDelta;
    return p;
  }
  return NULL;
}

/* The implementation of the actual query expansion. This function either turns the current node
 * into a union node with the original token node and new token node as children. Or if it is
 * already a union node (in consecutive calls), it just adds a new token node as a child to it */
void Ext_ExpandToken(struct RSQueryExpanderCtx *ctx, const char *str, size_t len,
                     RSTokenFlags flags) {

  Query *q = ctx->query;
  QueryNode *qn = *ctx->currentNode;

  /* Replace current node with a new union node if needed */
  if (qn->type != QN_UNION) {
    QueryNode *un = NewUnionNode();

    /* Append current node to the new union node as a child */
    QueryUnionNode_AddChild(un, qn);
    *ctx->currentNode = un;
  }
  /* Now the current node must be a union node - so we just add a new token node to it */
  QueryUnionNode_AddChild(*ctx->currentNode, NewTokenNodeExpanded(q, str, len, flags));
  // q->numTokens++;
}

/* Set the query payload */
void Ext_SetPayload(struct RSQueryExpanderCtx *ctx, RSPayload payload) {
  ctx->query->payload = payload;
}

/* Get an expander by name */
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
