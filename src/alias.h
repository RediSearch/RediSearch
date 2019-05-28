#ifndef ALIAS_H
#define ALIAS_H

#include "redismodule.h"
#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dict AliasTable;
extern AliasTable *AliasTable_g;

// Do not access or otherwise touch the backreference in the
// index spec. This is used for add and delete operations
#define INDEXALIAS_NO_BACKREF 0x01

AliasTable *AliasTable_New(void);

int AliasTable_Add(AliasTable *table, const char *alias, IndexSpec *spec, int options,
                   QueryError *status);

int AliasTable_Del(AliasTable *table, const char *alias, IndexSpec *spec, int options,
                   QueryError *status);

IndexSpec *AliasTable_Get(AliasTable *table, const char *alias);

void IndexAlias_InitGlobal(void);
void IndexAlias_DestroyGlobal(void);

int IndexAlias_Add(const char *alias, IndexSpec *spec, int options, QueryError *status);
int IndexAlias_Del(const char *alias, IndexSpec *spec, int options, QueryError *status);
IndexSpec *IndexAlias_Get(const char *alias);

#ifdef __cplusplus
}
#endif
#endif