/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __REDISEARCH_SEPARATORS_H___
#define __REDISEARCH_SEPARATORS_H___

#include "reply.h"
#include "redismodule.h"

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SeparatorList {
  char *separatorString;
  char separatorMap[256];
  size_t refcount;
} SeparatorList;

/* Check if a separator list contains a char */
// int SeparatorList_Contains(const SeparatorList sl, const char *separator);

struct SeparatorList *DefaultSeparatorList();
void SeparatorList_FreeGlobals(void);

/* Create a new separator list from a NULL-terminated C string */
struct SeparatorList *NewSeparatorListCStr(const char *str);

/* Free a separator list's memory */
void SeparatorList_Unref(struct SeparatorList *sl);

#define SeparatorList_Free SeparatorList_Unref

/* Load a separator list from RDB */
struct SeparatorList *SeparatorList_RdbLoad(RedisModuleIO* rdb);

/* Save a separator list to RDB */
void SeparatorList_RdbSave(RedisModuleIO *rdb, struct SeparatorList *sl);

void SeparatorList_Ref(struct SeparatorList *sl);

void ReplyWithSeparatorList(RedisModule_Reply *reply, struct SeparatorList *sl);

#ifdef FTINFO_FOR_INFO_MODULES
void AddSeparatorListToInfo(RedisModuleInfoCtx *ctx, struct SeparatorList *sl);
#endif

// TODO:
/* Returns a list of separators */
// char *GetSeparatorList(SeparatorList *sl);


#ifdef __cplusplus
}
#endif
#endif
