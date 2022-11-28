
#pragma once

#include "redisearch.h"
#include "util/map.h"

///////////////////////////////////////////////////////////////////////////////////////////////

/*
struct Scorer {
  virtual ~Scorer() {};

  virtual double Score(const ScorerArgs *args, const IndexResult *res, const DocumentMetadata *dmd, double minScore);
};
*/

typedef double (*Scorer)(const ScorerArgs &args, const IndexResult *res, const DocumentMetadata *dmd, double minScore);

//---------------------------------------------------------------------------------------------

struct Extensions {
  UnorderedMap<String, Scorer> scorers;
  UnorderedMap<String, QueryExpander::Factory> queryExpanders;

  Scorer GetScorer(const char *name);
  QueryExpander::Factory GetQueryExpander(const char *name);

  int Register(const char *alias, Scorer scorer);
  int Register(const char *name, QueryExpander::Factory factory);

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

  int Register(const char *alias, QueryExpander::Factory factory) {
    g_ext.Register(alias, factory);
  }
};

//---------------------------------------------------------------------------------------------

struct DefaultExtension : Extension {
  DefaultExtension();
};

///////////////////////////////////////////////////////////////////////////////////////////////
