#include <dlfcn.h>
#include <stdio.h>
#include "extension.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "redismodule.h"
#include "index_result.h"
#include "query.h"
#include <err.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Init the extension system - currently just create the regsistries

void Extensions::Init() {
  if (!queryExpanders_g) {
    queryExpanders_g = new TrieMap();
    scorers_g = new TrieMap();
  }
}

//---------------------------------------------------------------------------------------------

Extensions::~Extensions() {
  if (queryExpanders_g) {
    delete queryExpanders_g;
    queryExpanders_g = NULL;
  }
  if (scorers_g) {
    delete scorers_g;
    scorers_g = NULL;
  }
}

//---------------------------------------------------------------------------------------------

// Register a scoring function by its alias. privdata is an optional pointer to a user defined
// struct. ff is a free function releasing any resources allocated at the end of query execution.

int Extensions::RegisterScoringFunction(const char *alias, RSScoringFunction func, RSFreeFunction ff,
                                        void *privdata) {
  if (func == NULL || scorers_g == NULL) {
    return REDISEARCH_ERR;
  }
  ExtScoringFunction *ctx = rm_new(ExtScoringFunction);
  ctx->privdata = privdata;
  ctx->ff = ff;
  ctx->sf = func;

  /* Make sure that two scorers are never registered under the same name */
  if (scorers_g->Find((char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    rm_free(ctx);
    return REDISEARCH_ERR;
  }

  scorers_g->Add((char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

//---------------------------------------------------------------------------------------------

// Register a aquery expander

int Extensions::RegisterQueryExpander(const char *alias, RSQueryTokenExpander exp, RSFreeFunction ff,
                                      void *privdata) {
  if (exp == NULL || queryExpanders_g == NULL) {
    return REDISEARCH_ERR;
  }
  ExtQueryExpander *ctx;
  ctx->privdata = privdata;
  ctx->ff = ff;
  ctx->exp = exp;

  /* Make sure there are no two query expanders under the same name */
  if (queryExpanders_g->Find((char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    rm_free(ctx);
    return REDISEARCH_ERR;
  }
  queryExpanders_g->Add((char *)alias, strlen(alias), ctx, NULL);
  return REDISEARCH_OK;
}

//---------------------------------------------------------------------------------------------

/* Load an extension by calling its init function. return REDISEARCH_ERR or REDISEARCH_OK */
int Extensions::Load(const char *name, RSExtensionInitFunc func) {
  // bind the callbacks in the context
  RSExtensionCtx ctx = {
      .RegisterScoringFunction = RegisterScoringFunction,
      .RegisterQueryExpander = RegisterQueryExpander,
  };

  return func(&ctx);
}

//---------------------------------------------------------------------------------------------

// Dynamically load a RediSearch extension by .so file path. Returns REDISMODULE_OK or ERR

int Extensions::LoadDynamic(const char *path, char **errMsg) {
  int (*init)(struct RSExtensionCtx *);
  void *handle;
  *errMsg = NULL;
  handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (handle == NULL) {
    FMT_ERR(errMsg, "Extension %s failed to load: %s", path, dlerror());
    return REDISMODULE_ERR;
  }
  init = (int (*)(struct RSExtensionCtx *))(unsigned long)dlsym(handle, "RS_ExtensionInit");
  if (init == NULL) {
    FMT_ERR(errMsg,
            "Extension %s does not export RS_ExtensionInit() "
            "symbol. Module not loaded.",
            path);
    return REDISMODULE_ERR;
  }

  if (Extensions::Load(path, init) == REDISEARCH_ERR) {
    FMT_ERR(errMsg, "Could not register extension %s", path);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

// Get a scoring function by name
static ExtScoringFunction *Extensions::GetScoringFunction(ScoringFunctionArgs *fnargs, const char *name) {
  if (!scorers_g) return NULL;

  /* lookup the scorer by name (case sensitive) */
  ExtScoringFunction *p = scorers_g->Find((char *)name, strlen(name));
  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    /* if no ctx was given, we just return the scorer */
    if (fnargs) {
      fnargs->extdata = p->privdata;
      fnargs->GetSlop = IndexResult::MinOffsetDelta;
    }
    return p;
  }
  return NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// ExpandToken allows the user to add an expansion of the token in the query, that will be
// union-merged with the given token in query time.
// str is the expanded string, len is its length, and flags is a 32 bit flag mask that
// can be used by the extension to set private information on the token.

// The implementation of the actual query expansion. This function either turns the current node
// into a union node with the original token node and new token node as children. Or if it is
// already a union node (in consecutive calls), it just adds a new token node as a child to it.

void RSQueryExpander::ExpandToken(const char *str, size_t len, RSTokenFlags flags) {
  QueryAST *q = qast;
  QueryNode *qn = *currentNode;

  // Replace current node with a new union node if needed
  if (qn->type != QN_UNION) {
    QueryUnionNode *un;

    un->opts.fieldMask = qn->opts.fieldMask;

    // Append current node to the new union node as a child
    un->AddChild(qn);
    *currentNode = un;
  }

  QueryTokenNode *exp = q->NewTokenNodeExpanded(str, len, flags);
  exp->opts.fieldMask = qn->opts.fieldMask;
  // Now the current node must be a union node - so we just add a new token node to it
  *currentNode->AddChild(exp);
}

//---------------------------------------------------------------------------------------------

// Expand the token with a multi-word phrase, where all terms are intersected.
// toks is an array with num its len, each member of it is a null terminated string.
// If replace is set to 1, we replace the original token with the new phrase.
// If exact is 1 the expanded phrase is an exact match phrase.

// The implementation of the actual query expansion.
// Either turn the current node into a union node with the original token node and new
// token node as children. Or if it is already a union node (in consecutive calls),
// it just adds a new token node as a child to it.

void RSQueryExpander::ExpandTokenWithPhrase(const char **toks, size_t num, RSTokenFlags flags,
                                            bool replace, bool exact) {
  QueryAST *q = qast;
  QueryNode *qn = *currentNode;

  QueryPhraseNode *ph = new QueryPhraseNode(exact);
  for (size_t i = 0; i < num; i++) {
    ph->AddChild(q->NewTokenNodeExpanded(toks[i], strlen(toks[i]), flags));
  }

  // if we're replacing - just set the expanded phrase instead of the token
  if (replace) {
    delete qn;

    currentNode = ph;
  } else {

    // Replace current node with a new union node if needed
    if (qn->type != QN_UNION) {
      QueryUnionNode *un;

      // Append current node to the new union node as a child
      un->AddChild(qn);
      currentNode = un;
    }
    // Now the current node must be a union node - so we just add a new token node to it
    currentNode->AddChild(ph);
  }
}

//---------------------------------------------------------------------------------------------

// Set the query payload
void RSQueryExpander::SetPayload(RSPayload payload) {
  qast->udata = payload.data;
  qast->udatalen = payload.len;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Get an expander by name

static ExtQueryExpander *Extensions::GetQueryExpander(RSQueryExpander *ctx, const char *name) {
  if (!queryExpanders_g) return NULL;

  ExtQueryExpander *p = queryExpanders_g->Find((char *)name, strlen(name));

  if (p && (void *)p != TRIEMAP_NOTFOUND) {
    // ctx->ExpandToken = RSQueryExpander::ExpandToken;
    // ctx->SetPayload = RSQueryExpander::SetPayload;
    // ctx->ExpandTokenWithPhrase = RSQueryExpander::ExpandTokenWithPhrase;
    ctx->privdata = p->privdata;
    return p;
  }
  return NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////
