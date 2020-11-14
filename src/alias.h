#pragma once

#include "redismodule.h"
#include "spec.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Do not access or otherwise touch the backreference in the index spec. 
// This is used for add and delete operations
#define INDEXALIAS_NO_BACKREF 0x01

struct AliasTable : public Object {
  struct dict *d;
  void (*on_add)(const char *alias, const IndexSpec *spec);
  void (*on_del)(const char *alias, const IndexSpec *spec);

  AliasTable();
  ~AliasTable();

  int Add(const char *alias, IndexSpec *spec, int options, QueryError *status);
  int Del(const char *alias, IndexSpec *spec, int options, QueryError *status);
  IndexSpec *Get(const char *alias);
};

//---------------------------------------------------------------------------------------------

struct IndexAlias {
  static AliasTable *AliasTable_g;

  int Add(const char *alias, IndexSpec *spec, int options, QueryError *status);
  int Del(const char *alias, IndexSpec *spec, int options, QueryError *status);
  IndexSpec *Get(const char *alias);

  static void InitGlobal();
  static void DestroyGlobal();
};

///////////////////////////////////////////////////////////////////////////////////////////////
