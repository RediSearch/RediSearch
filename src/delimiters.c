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

static DelimiterList *__default_delimiters = NULL;

DelimiterList *DefaultDelimiterList() {
  // if(__default_delimiters == NULL) {
  //   __default_delimiters = NewDelimiterListCStr(DEFAULT_DELIMITERS_STR);
  // }
  return __default_delimiters;
}

struct DelimiterList *NewDelimiterListCStr(const char* str) {
  if(str == NULL || strlen(str) == 0) {
    return NULL;
  }
  //if (len > MAX_DELIMITERLIST_SIZE) {
  // Truncate?
  //}

  // initialize the delimiter string
  DelimiterList *dl = rm_malloc(sizeof(DelimiterList));
  uint16_t len = strlen(str);
  dl->delimiters = rm_malloc(sizeof(char) * (len + 1));
  strcpy(dl->delimiters, str);
  // initialize the delimiter map
  memset(dl->delimiterMap, 0, 256);
  for(uint16_t i = 0; i < len; i++) {
    uint8_t pos = (uint8_t) str[i];
    dl->delimiterMap[pos] = 1;
  }
  return dl;
}

static void DelimiterList_FreeInternal(DelimiterList *dl) {
  if(dl) {
    if(dl->delimiters) {
        rm_free(dl->delimiters);
      }
      rm_free(dl);
  }
}

void DelimiterList_Unref(DelimiterList *dl) {
  if (dl == __default_delimiters) {
    return;
  }

  DelimiterList_FreeInternal(dl);
}

void DelimiterList_FreeGlobals(void) {
    if (__default_delimiters) {
    DelimiterList_FreeInternal(__default_delimiters);
    __default_delimiters = NULL;
  }
}

DelimiterList *DelimiterList_RdbLoad(RedisModuleIO* rdb, int encver) {
  if(encver >= INDEX_DELIMITERS_VERSION) {
    char *s;
    LoadStringBufferAlloc_IOErrors(rdb, s, NULL, goto fail);
    return NewDelimiterListCStr(s);
    rm_free(s);
  }

fail:
  return DefaultDelimiterList();
}

void DelimiterList_RdbSave(RedisModuleIO* rdb, DelimiterList *dl) {
  if (dl != NULL && dl->delimiters != NULL) {
    RedisModule_SaveStringBuffer(rdb, dl->delimiters, strlen(dl->delimiters) + 1);
  }
}

// TODO: do we need this function?
// void DelimiterList_Ref(DelimiterList dl) {
// }

void ReplyWithDelimiterList(RedisModule_Reply* reply, DelimiterList *dl) {
  RedisModule_Reply_SimpleString(reply, "delimiters");

  RedisModule_Reply_Array(reply);
  if (dl == NULL || dl->delimiters == NULL) {
    RedisModule_Reply_Null(reply);
  } else {
    RedisModule_Reply_StringBuffer(reply, dl->delimiters, strlen(dl->delimiters));
  }
  RedisModule_Reply_ArrayEnd(reply);
}

// TODO:
void AddDelimiterListToInfo(RedisModuleInfoCtx* ctx, DelimiterList *dl) {
}

// TODO:
// char* GetDelimiterList(DelimiterList* dl) {
//   return NULL;
// }
