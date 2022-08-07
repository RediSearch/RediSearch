
#pragma once

#include "redisearch.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct Scorer {
  virtual ~Scorer() {};

  virtual double Score(const ScorerArgs *args, const IndexResult *res, const RSDocumentMetadata *dmd, double minScore);
};

//---------------------------------------------------------------------------------------------

struct QueryExpander {
  virtual ~QueryExpander() {}

  RSQueryTokenExpander exp;
};

//---------------------------------------------------------------------------------------------

struct Extension : Object {
};

//---------------------------------------------------------------------------------------------

struct Extensions {
  UnorderedMap<String, Scorer*> queryExpanders;
  UnorderedMap<String, QueryExpander*> scorers;

	Extensions();
	~Extensions();

  Scorer *GetScorer(ScorerArgs *args, const char *name);
  QueryExpander *GetQueryExpander(RSQueryExpander *expander, const char *name);

  int Register(const char *alias, Scorer *scorer);
  int Register(const char *alias, QueryExpander *expander);

  int Load(const char *name, Extension *ext);
  int LoadDynamic(const char *path, char **errMsg);
};

//---------------------------------------------------------------------------------------------

extern Extensions g_ext;

///////////////////////////////////////////////////////////////////////////////////////////////
