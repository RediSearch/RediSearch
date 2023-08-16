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


struct DelimiterList *DefaultDelimiterList();

/* Return the string of default delimiters*/
const char *DefaultDelimiterString();

void DelimiterList_FreeGlobals(void);

/* Create a new delimiter list from a NULL-terminated C string */
struct DelimiterList *NewDelimiterListCStr(const char *str);

/* Add delimiters in str to existing Delimiter list dl*/
DelimiterList *AddDelimiterListCStr(const char* str, DelimiterList* dl);

/* Remove delimiters in str from existing Delimiter list dl*/
DelimiterList *RemoveDelimiterListCStr(const char* str, DelimiterList* dl);

/* Free a delimiter list's memory */
void DelimiterList_Unref(struct DelimiterList *dl);

#define DelimiterList_Free DelimiterList_Unref

/* Load a delimiter list from RDB */
struct DelimiterList *DelimiterList_RdbLoad(RedisModuleIO* rdb);

/* Save a delimiter list to RDB */
void DelimiterList_RdbSave(RedisModuleIO *rdb, struct DelimiterList *dl);

void DelimiterList_Ref(struct DelimiterList *dl);

void ReplyWithDelimiterList(RedisModule_Reply *reply, struct DelimiterList *dl);

// #ifdef FTINFO_FOR_INFO_MODULES
// void AddDelimiterListToInfo(RedisModuleInfoCtx *ctx, struct DelimiterList *dl);
// #endif

char *toksep(char **s, size_t *tokLen, const DelimiterList *dl);

/* Check if c if part of the delimiter list dl*/
int istoksep(int c, const DelimiterList *dl);

#ifdef __cplusplus
}
#endif
#endif
