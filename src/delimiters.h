/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __REDISEARCH_DELIMITERS_H___
#define __REDISEARCH_DELIMITERS_H___

#include "reply.h"
#include "redismodule.h"

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DelimiterList {
  char *delimiterString;
  char delimiterMap[256];
  size_t refcount;
} DelimiterList;

/* Check if a delimiter list contains a char */
// int DelimiterList_Contains(const DelimiterList dl, const char *delimiter);

struct DelimiterList *DefaultDelimiterList();
void DelimiterList_FreeGlobals(void);

/* Create a new delimiter list from a NULL-terminated C string */
struct DelimiterList *NewDelimiterListCStr(const char *str);

/* Free a delimiter list's memory */
void DelimiterList_Unref(struct DelimiterList *dl);

#define DelimiterList_Free DelimiterList_Unref

/* Load a delimiter list from RDB */
struct DelimiterList *DelimiterList_RdbLoad(RedisModuleIO* rdb);

/* Save a delimiter list to RDB */
void DelimiterList_RdbSave(RedisModuleIO *rdb, struct DelimiterList *dl);

void DelimiterList_Ref(struct DelimiterList *dl);

void ReplyWithDelimiterList(RedisModule_Reply *reply, struct DelimiterList *dl);

#ifdef FTINFO_FOR_INFO_MODULES
void AddDelimiterListToInfo(RedisModuleInfoCtx *ctx, struct DelimiterList *dl);
#endif

// TODO:
/* Returns a list of delimiters */
// char *GetDelimiterList(DelimiterList *dl);


#ifdef __cplusplus
}
#endif
#endif
