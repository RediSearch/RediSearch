/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
  void (*on_add)(const HiddenString *alias, const IndexSpec *spec);
  void (*on_del)(const HiddenString *alias, const IndexSpec *spec);
} AliasTable;

extern AliasTable *AliasTable_g;

// Do not access or otherwise touch the backreference in the
// index spec. This is used for add and delete operations
#define INDEXALIAS_NO_BACKREF 0x01

AliasTable *AliasTable_New(void);

void IndexAlias_InitGlobal(void);
void IndexAlias_DestroyGlobal(AliasTable **t);

int IndexAlias_Add(const HiddenString *alias, StrongRef spec, int options, QueryError *status);
int IndexAlias_Del(const HiddenString *alias, StrongRef spec, int options, QueryError *status);
StrongRef IndexAlias_Get(const HiddenString *alias);

#ifdef __cplusplus
}
#endif
#endif
