#pragma once

#include "util/map.h"
#include "redismodule.h"
#include "spec.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Do not access or otherwise touch the backreference in the index spec.
// This is used for add and delete operations
#define INDEXALIAS_NO_BACKREF 0x01

struct AliasTable : public Object {
  UnorderedMap<String, IndexSpec *> d;

  int Add(const char *alias, IndexSpec *spec, int options, QueryError *status);
  int Del(const char *alias, IndexSpec *spec, int options, QueryError *status);
  IndexSpec *Get(const char *alias);
};

//---------------------------------------------------------------------------------------------

struct IndexAlias {
  static AliasTable *AliasTable_g;

  static int Add(const char *alias, IndexSpec *spec, int options, QueryError *status);
  static int Del(const char *alias, IndexSpec *spec, int options, QueryError *status);
  static IndexSpec *Get(const char *alias);

  static void InitGlobal();
  static void DestroyGlobal();
};

///////////////////////////////////////////////////////////////////////////////////////////////
