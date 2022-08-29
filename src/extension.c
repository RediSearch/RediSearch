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

Extensions g_ext;

//---------------------------------------------------------------------------------------------

int Extensions::Register(const char *name, Scorer scorer) {
  if (scorer == NULL) {
    throw Error("Cannot register %s: null scorer", name);
  }

  // Make sure that two scorers are never registered under the same name
  if (scorers.find(name) != scorers.end()) {
    throw Error("Cannot register %s: already registered", name);
  }

  scorers[name] = scorer;
}

//---------------------------------------------------------------------------------------------

int Extensions::Register(const char *name, QueryExpander::Factory factory) {
  if (factory == NULL) {
    return REDISEARCH_ERR;
  }

  if (queryExpanders.find(name) != queryExpanders.end()) {
    throw Error("Cannot register %s: already registered", name);
  }

  queryExpanders[name] = factory;
}

//---------------------------------------------------------------------------------------------

// Load an extension by calling its init function. return REDISEARCH_ERR or REDISEARCH_OK
#if 1
int Extensions::Load(const char *name, RS_ExtensionInit init) {
  return init();
}
#endif

//---------------------------------------------------------------------------------------------

// Dynamically load a RediSearch extension by .so file path. Returns REDISMODULE_OK or ERR

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

// lookup the scorer by name (case sensitive)

Scorer Extensions::GetScorer(const char *name) {
  auto it = scorers.find(name);
  if (it == scorers.end()) {
    throw Error("Cannot find scorer %s", name);
  }
  return it->second;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// ExpandToken allows the user to add an expansion of the token in the query, that will be
// union-merged with the given token in query time.
// We either turn the current node into a union node with the original token node
// and new token node as children. Or if it is already a union node (in consecutive calls),
// we just adds a new token node as a child to it.

// str is the expanded string. flags is a 32 bit flag mask that can be used by the extension 
// to set private information on the token.

void QueryExpander::ExpandToken(std::string_view str, RSTokenFlags flags) {
  QueryNode *node = currentNode;

  // Replace current node with a new union node if needed
  if (node->type != QN_UNION) {
    auto union_node = new QueryUnionNode;

    union_node->opts.fieldMask = node->opts.fieldMask;

    // Append current node to the new union node as a child
    union_node->AddChild(node);
    currentNode = union_node;
  }

  QueryTokenNode *exp = qast->NewTokenNodeExpanded(str, flags);
  exp->opts.fieldMask = node->opts.fieldMask;
  // Now the current node must be a union node - so we just add a new token node to it
  currentNode->AddChild(exp);
}

//---------------------------------------------------------------------------------------------

// Expand the token with a multi-word phrase, where all terms are intersected.
// If replace is true, we replace the original token with the new phrase.
// If exact is 1 the expanded phrase is an exact match phrase.

// Either turn the current node into a union node with the original token node and new
// token node as children. Or if it is already a union node (in consecutive calls),
// it just adds a new token node as a child to it.

void QueryExpander::ExpandTokenWithPhrase(const Vector<String> &tokens, RSTokenFlags flags,
                                          bool replace, bool exact) {
  QueryNode *node = currentNode;

  QueryPhraseNode *phrase_node = new QueryPhraseNode(exact);
  for (auto &token: tokens) {
    phrase_node->AddChild(qast->NewTokenNodeExpanded(token, flags));
  }

  // if we're replacing - just set the expanded phrase instead of the token
  if (replace) {
    delete node;

    currentNode = phrase_node;
  } else {
    // Replace current node with a new union node if needed
    if (node->type != QN_UNION) {
      auto union_node = new QueryUnionNode;

      // Append current node to the new union node as a child
      union_node->AddChild(node);
      currentNode = union_node;
    }
    // Now the current node must be a union node - so we just add a new token node to it
    currentNode->AddChild(phrase_node);
  }
}

//---------------------------------------------------------------------------------------------

void QueryExpander::SetPayload(RSPayload payload) {
  qast->payload = payload;
}

///////////////////////////////////////////////////////////////////////////////////////////////

QueryExpander::Factory Extensions::GetQueryExpander(const char *name) {
  auto it = queryExpanders.find(name);
  if (it != queryExpanders.end()) {
    return NULL;
  }

  QueryExpander::Factory fact = it->second;
  return fact;
}

///////////////////////////////////////////////////////////////////////////////////////////////
