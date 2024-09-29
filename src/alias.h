/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef ALIAS_H
#define ALIAS_H

#include "redismodule.h"
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  struct dict *d;
  void (*on_add)(const HiddenName *alias, const IndexSpec *spec);
  void (*on_del)(const HiddenName *alias, const IndexSpec *spec);
} AliasTable;

extern AliasTable *AliasTable_g;

// Do not access or otherwise touch the backreference in the
// index spec. This is used for add and delete operations
#define INDEXALIAS_NO_BACKREF 0x01

AliasTable *AliasTable_New(void);

void IndexAlias_InitGlobal(void);
void IndexAlias_DestroyGlobal(AliasTable **t);

int IndexAlias_Add(HiddenName *alias, StrongRef spec, int options, QueryError *status);
int IndexAlias_Del(HiddenName *alias, StrongRef spec, int options, QueryError *status);
StrongRef IndexAlias_Get(const char *alias);

#ifdef __cplusplus
}
#endif
#endif
