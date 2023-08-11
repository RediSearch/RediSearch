/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdint.h>
#define __REDISEARCH_SEPARATORS_C__
#include "separators.h"
#include "rdb.h"
#include "rmalloc.h"


#define MAX_SEPARATORSTRING_SIZE 64

static SeparatorList *__default_separators = NULL;

SeparatorList *DefaultSeparatorList() {
  return __default_separators;
}

SeparatorList *NewSeparatorListCStr(const char* str) {
  if(str == NULL) {
    return NULL;
  }

  SeparatorList *sl = rm_malloc(sizeof(SeparatorList));

  // initialize the separator string
  size_t len = strlen(str);
  if (len > MAX_SEPARATORSTRING_SIZE) {
    len = MAX_SEPARATORSTRING_SIZE;
  }
  sl->separatorString = rm_malloc(sizeof(char) * (len + 1));
  strncpy(sl->separatorString, str, len + 1);
  sl->separatorString[len]='\0';

  // initialize the separator map
  memset(sl->separatorMap, 0, sizeof(sl->separatorMap));
  for(uint16_t i = 0; i < len; i++) {
    uint8_t pos = (uint8_t)str[i];
    sl->separatorMap[pos] = 1;
  }
  sl->refcount = 1;
  return sl;
}

static void SeparatorList_FreeInternal(SeparatorList *sl) {
  if(sl) {
    if(sl->separatorString) {
        rm_free(sl->separatorString);
      }
      rm_free(sl);
  }
}

void SeparatorList_Unref(SeparatorList *sl) {
  if (sl == __default_separators) {
    return;
  }

  if (__sync_sub_and_fetch(&sl->refcount, 1)) {
    return;
  }
  SeparatorList_FreeInternal(sl);
}

void SeparatorList_FreeGlobals(void) {
}

SeparatorList *SeparatorList_RdbLoad(RedisModuleIO* rdb) {
  SeparatorList *sl = NULL;
  size_t len;
  char *s = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  sl = NewSeparatorListCStr(s);
  sl->refcount = 1;
  if(s) {
    rm_free(s);
  }
  return sl;

cleanup:
  if(sl) {
    SeparatorList_FreeInternal(sl);
  }
  return DefaultSeparatorList();
}

void SeparatorList_RdbSave(RedisModuleIO* rdb, SeparatorList *sl) {
  if (sl != NULL && sl->separatorString != NULL) {
    RedisModule_SaveStringBuffer(rdb, sl->separatorString, strlen(sl->separatorString) + 1);
  }
}

void SeparatorList_Ref(SeparatorList *sl) {
    __sync_fetch_and_add(&sl->refcount, 1);
}

void ReplyWithSeparatorList(RedisModule_Reply* reply, SeparatorList *sl) {
  RedisModule_Reply_SimpleString(reply, "separators");

  RedisModule_Reply_Array(reply);
  if (sl == NULL || sl->separatorString == NULL) {
    RedisModule_Reply_Null(reply);
  } else {
    RedisModule_Reply_StringBuffer(reply, sl->separatorString, strlen(sl->separatorString));
  }
  RedisModule_Reply_ArrayEnd(reply);
}

// TODO:
// void AddSeparatorListToInfo(RedisModuleInfoCtx* ctx, SeparatorList *sl) {
// }

// TODO:
// char* GetSeparatorList(SeparatorList* sl) {
//   return NULL;
// }
