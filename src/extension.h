
#pragma once

#include "redisearch.h"

///////////////////////////////////////////////////////////////////////////////////////////////

class Extensions {
  static TrieMap *queryExpanders_g;
  static TrieMap *scorers_g;

public:
	Extensions();
	~Extensions();

  // Get a scoring function by name. Returns NULL if no such scoring function exists
  static struct ExtScoringFunction *GetScoringFunction(ScoringFunctionArgs *fnargs, const char *name);

  // Get a query expander function by name. Returns NULL if no such function exists
  static struct ExtQueryExpander *GetQueryExpander(RSQueryExpander *expander, const char *name);
};

//---------------------------------------------------------------------------------------------

// Context for saving a scoring function and its private data and free
struct ExtScoringFunction {
  RSScoringFunction sf;
  RSFreeFunction ff;
  void *privdata;
};

//---------------------------------------------------------------------------------------------

// Context for saving the a token expander and its free / privdata
struct ExtQueryExpander {
  RSQueryTokenExpander exp;
  RSFreeFunction ff;
  void *privdata;
};

//---------------------------------------------------------------------------------------------

// Load an extension explicitly with its name and an init function
int Extension_Load(const char *name, RSExtensionInitFunc func);

// Dynamically load a RediSearch extension by .so file path. Returns REDISMODULE_OK or ERR.
// errMsg is set to NULL on success or an error message on failure.
int Extension_LoadDynamic(const char *path, char **errMsg);

///////////////////////////////////////////////////////////////////////////////////////////////
