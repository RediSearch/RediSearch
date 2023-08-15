/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdint.h>
#define __REDISEARCH_DELIMITERS_C__
#include "delimiters.h"
#include "rdb.h"
#include "rmalloc.h"


#define MAX_DELIMITERSTRING_SIZE 64

static DelimiterList *__default_delimiters = NULL;

DelimiterList *DefaultDelimiterList() {
  return __default_delimiters;
}

DelimiterList *NewDelimiterListCStr(const char* str) {
  if(str == NULL) {
    return NULL;
  }

  DelimiterList *dl = rm_malloc(sizeof(DelimiterList));

  // initialize the delimiter string
  size_t len = strlen(str);
  if (len > MAX_DELIMITERSTRING_SIZE) {
    len = MAX_DELIMITERSTRING_SIZE;
  }
  dl->delimiterString = rm_malloc(sizeof(char) * (len + 1));
  strncpy(dl->delimiterString, str, len + 1);
  dl->delimiterString[len]='\0';

  // initialize the delimiter map
  memset(dl->delimiterMap, 0, sizeof(dl->delimiterMap));
  for(uint16_t i = 0; i < len; i++) {
    uint8_t pos = (uint8_t)str[i];
    dl->delimiterMap[pos] = 1;
  }
  dl->refcount = 1;
  return dl;
}

static void DelimiterList_FreeInternal(DelimiterList *dl) {
  if(dl) {
    if(dl->delimiterString) {
        rm_free(dl->delimiterString);
      }
      rm_free(dl);
  }
}

void DelimiterList_Unref(DelimiterList *dl) {
  if (dl == __default_delimiters) {
    return;
  }

  if (__sync_sub_and_fetch(&dl->refcount, 1)) {
    return;
  }
  DelimiterList_FreeInternal(dl);
}

void DelimiterList_FreeGlobals(void) {
}

DelimiterList *DelimiterList_RdbLoad(RedisModuleIO* rdb) {
  DelimiterList *dl = NULL;
  size_t len;
  char *s = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  dl = NewDelimiterListCStr(s);
  if(s) {
    rm_free(s);
  }
  return dl;

cleanup:
  if(dl) {
    DelimiterList_FreeInternal(dl);
  }
  return DefaultDelimiterList();
}

void DelimiterList_RdbSave(RedisModuleIO* rdb, DelimiterList *dl) {
  if (dl != NULL && dl->delimiterString != NULL) {
    RedisModule_SaveStringBuffer(rdb, dl->delimiterString, strlen(dl->delimiterString) + 1);
  }
}

void DelimiterList_Ref(DelimiterList *dl) {
    __sync_fetch_and_add(&dl->refcount, 1);
}

void ReplyWithDelimiterList(RedisModule_Reply* reply, DelimiterList *dl) {
  RedisModule_Reply_SimpleString(reply, "delimiters");

  RedisModule_Reply_Array(reply);
  if (dl == NULL || dl->delimiterString == NULL) {
    RedisModule_Reply_Null(reply);
  } else {
    RedisModule_Reply_StringBuffer(reply, dl->delimiterString, strlen(dl->delimiterString));
  }
  RedisModule_Reply_ArrayEnd(reply);
}

// TODO:
// void AddDelimiterListToInfo(RedisModuleInfoCtx* ctx, DelimiterList *dl) {
// }

// TODO:
// char* GetDelimiterList(DelimiterList* dl) {
//   return NULL;
// }
