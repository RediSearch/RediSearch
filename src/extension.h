
#pragma once

#include "redisearch.h"
#include "util/map.h"

///////////////////////////////////////////////////////////////////////////////////////////////

/*
struct Scorer {
  virtual ~Scorer() {};

  virtual double Score(const ScorerArgs *args, const IndexResult *res, const RSDocumentMetadata *dmd, double minScore);
};
*/

typedef double (*Scorer)(const ScorerArgs *args, const IndexResult *res, const RSDocumentMetadata *dmd, double minScore);

//---------------------------------------------------------------------------------------------
/*
struct QueryExpander {
  virtual ~QueryExpander() {}

  virtual int Expand(RSToken *token) = 0;

  typedef QueryExpander (*Factory)(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status);
};
*/

//---------------------------------------------------------------------------------------------

struct Extensions {
  UnorderedMap<String, Scorer> scorers;
  UnorderedMap<String, QueryExpander::Factory> queryExpanders;

  Extensions();
  ~Extensions();

  Scorer GetScorer(ScorerArgs *args, const char *name);
  QueryExpander::Factory GetQueryExpander(const char *name);

  int Register(const char *alias, Scorer scorer);

  template <class QueryExpander>
  int Register(const char *name, QueryExpander::Factory factory) {
    if (factory == NULL) {
      return REDISEARCH_ERR;
    }

    if (queryExpanders.find(name) != queryExpanders.end()) {
      throw Error("Cannot register %s: already registered", name);
    }

    queryExpanders[name] = factory;
  }

  typedef struct Extension *(*RS_ExtensionInit)();

  int Load(const char *name, RS_ExtensionInit init);
  int LoadDynamic(const char *path, char **errMsg);
};

//---------------------------------------------------------------------------------------------

struct ExtensionAPI_v1 {
  int (*RegisterScorer)(const char *alias, Scorer scorer);
  int (*QueryExpander)(const char *alias, QueryExpander::Factory expanderFactory);
};

//---------------------------------------------------------------------------------------------

extern Extensions g_ext;

//---------------------------------------------------------------------------------------------

struct Extension : Object {
  int Register(const char *alias, Scorer scorer) {
    g_ext.Register(alias, scorer);
  }

  template <class QueryExpander>
  int Register(const char *alias, QueryExpander::Factory factory) {
    g_ext.Register(alias, factory);
  }
};

//---------------------------------------------------------------------------------------------

struct DefaultExtension : Extension {
  DefaultExtension();
};

///////////////////////////////////////////////////////////////////////////////////////////////
