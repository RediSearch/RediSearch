#include "extension.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "redismodule.h"
#include "index_result.h"
#include "query.h"

#include <dlfcn.h>
#include <stdio.h>
#include <err.h>

///////////////////////////////////////////////////////////////////////////////////////////////

extern Extensions g_ext = new Extensions();

//---------------------------------------------------------------------------------------------

int Extensions::Register(const char *alias, Scorer *scorer) {
  if (scorer == NULL) {
    return REDISEARCH_ERR;
  }
  // Make sure that two scorers are never registered under the same name
  if (scorers.find(alias) != scorers::end()->Find((char *)alias, strlen(alias)) != TRIEMAP_NOTFOUND) {
    return REDISEARCH_ERR;
  }

  scorers[alias] = scorer;
  return REDISEARCH_OK;
}

//---------------------------------------------------------------------------------------------

int Extensions::RegisterQueryExpander(const char *alias, RSQueryTokenExpander *exp) {
  if (exp == NULL) {
    return REDISEARCH_ERR;
  }
  // Make sure there are no two query expanders under the same name
  if (queryExpanders.find(alias) != queryExpanders.end()) {
    return REDISEARCH_ERR;
  }
  queryExpanders[alias] = exp;
  return REDISEARCH_OK;
}

//---------------------------------------------------------------------------------------------

// Load an extension by calling its init function. return REDISEARCH_ERR or REDISEARCH_OK

int Extensions::Load(const char *name, Extension *ext) {
  return REDISEARCH_OK;
}

//---------------------------------------------------------------------------------------------

// Dynamically load a RediSearch extension by .so file path. Returns REDISMODULE_OK or ERR

typedef int (*RS_ExtensionInit)(RSExtensions *);

int Extensions::LoadDynamic(const char *path, char **errMsg) {
  *errMsg = NULL;
  void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (handle == NULL) {
    FMT_ERR(errMsg, "Extension %s failed to load: %s", path, dlerror());
    return REDISMODULE_ERR;
  }
  RS_ExtensionInit init = reinterpret_cast<RS_ExtensionInit>(dlsym(handle, "RS_ExtensionInit"));
  if (init == NULL) {
    FMT_ERR(errMsg, "Extension %s does not export RS_ExtensionInit() symbol. Module not loaded.", path);
    return REDISMODULE_ERR;
  }

  if (Load(path, init) == REDISEARCH_ERR) {
    FMT_ERR(errMsg, "Could not register extension %s", path);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

Scorer *Extensions::GetScorer(ScorerArgs *args, const char *name) {
  // lookup the scorer by name (case sensitive)
  auto it = scorers.find(name);
  if (it == scorers.end()) {
    return NULL;
  }

  Scorer *scorer = it->second;
  if (args) {
    args->extdata = scorer;
    args->GetSlop = IndexResult::MinOffsetDelta();
  }
  return scorer;
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
  QueryNode *qn = currentNode;

  // Replace current node with a new union node if needed
  if (qn->type != QN_UNION) {
    QueryUnionNode *un;

    un->opts.fieldMask = qn->opts.fieldMask;

    // Append current node to the new union node as a child
    un->AddChild(qn);
    currentNode = un;
  }

  QueryTokenNode *exp = q->NewTokenNodeExpanded(str, len, flags);
  exp->opts.fieldMask = qn->opts.fieldMask;
  // Now the current node must be a union node - so we just add a new token node to it
  currentNode->AddChild(exp);
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
  QueryNode *qn = currentNode;

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

QueryExpander *Extensions::GetQueryExpander(RSQueryExpander *ctx, const char *name) {
  auto it = queryExpanders.find(name);
  if (it == queryExpanders.end()) {
    return NULL;
  }

  queryExpanders *exp = it->second;
  ctx->privdata = exp;
  return exp;
}

///////////////////////////////////////////////////////////////////////////////////////////////
