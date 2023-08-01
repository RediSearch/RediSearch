/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __REDISEARCH_DELIMITERS_H___
#define __REDISEARCH_DELIMITERS_H___

#include "reply.h"
#include "redismodule.h"

#include <stdint.h>
#include <stdlib.h>

static const char DEFAULT_DELIMITERS_STR[6] = "!#$~@";
// static const char DEFAULT_DELIMITERS_STR[32] = "!\"#$\%&'()*+,-./:;<=>?@[\\]^`{|}~";
//! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ ` { | } ~
// static const char DEFAULT_DELIMITERS[256] = {
//     [' '] = 1, ['\t'] = 1, [','] = 1,  ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1, ['{'] = 1,
//     ['}'] = 1, ['['] = 1,  [']'] = 1,  [':'] = 1, [';'] = 1, ['~'] = 1, ['!'] = 1, ['@'] = 1,
//     ['#'] = 1, ['$'] = 1,  ['%'] = 1,  ['^'] = 1, ['&'] = 1, ['*'] = 1, ['-'] = 1, ['='] = 1,
//     ['+'] = 1, ['|'] = 1,  ['\''] = 1, ['`'] = 1, ['"'] = 1, ['<'] = 1, ['>'] = 1, ['?'] = 1,
// };

#ifdef __cplusplus
extern "C" {
#endif

// #ifndef __REDISEARCH_DELIMITERS_C__
typedef char* DelimiterList;
// #else
// char* DelimiterList;
// #endif


/* Check if a delimiter list contains a char */
// int DelimiterList_Contains(const DelimiterList dl, const char *delimiter);

DelimiterList DefaultDelimiterList();
void DelimiterList_FreeGlobals(void);

/* Create a new delimiter list from a NULL-terminated C string */
DelimiterList NewDelimiterListCStr(const char *str);

/* Free a stopword list's memory */
void DelimiterList_Unref(DelimiterList dl);

#define DelimiterList_Free DelimiterList_Unref

/* Load a delimiter list from RDB */
char* DelimiterList_RdbLoad(RedisModuleIO* rdb, int encver);

/* Save a delimiter list to RDB */
void DelimiterList_RdbSave(RedisModuleIO *rdb, DelimiterList dl);

// TODO: Do we need this function?
// void DelimiterList_Ref(DelimiterList dl);

void ReplyWithDelimiterList(RedisModule_Reply *reply, DelimiterList dl);

#ifdef FTINFO_FOR_INFO_MODULES
void AddDelimiterListToInfo(RedisModuleInfoCtx *ctx, DelimiterList dl);
#endif

// TODO:
/* Returns a NULL terminated list of stopwords */
// char *GetDelimiterList(DelimiterList *dl);


#ifdef __cplusplus
}
#endif
#endif
