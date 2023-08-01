/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#define __REDISEARCH_DELIMITERS_C__
#include "delimiters.h"
#include "rdb.h"
#include "spec.h"
#include "rmalloc.h"


// int DelimiterList_Contains(const DelimiterList dl, const char* term) {
//   return 0;
// }

DelimiterList DefaultDelimiterList() {
  return NewDelimiterListCStr(DEFAULT_DELIMITERS_STR);
}

void DelimiterList_FreeGlobals(void) {
}

DelimiterList NewDelimiterListCStr(const char* str) {
  if(str == NULL) {
    return NULL;
  }
  //if (len > MAX_DELIMITERLIST_SIZE) {
  // Truncate?
  //}

  DelimiterList dl = rm_malloc(sizeof(char) * (strlen(str)+1));
  strcpy(dl, str);
  return dl;
}

void DelimiterList_Unref(DelimiterList dl) {
  rm_free(dl);
}

char *DelimiterList_RdbLoad(RedisModuleIO* rdb, int encver) {
  if(encver >= INDEX_DELIMITERS_VERSION) {
    size_t l;
    char *s;
    LoadStringBufferAlloc_IOErrors(rdb, s, NULL, goto fail);
    return s;
  }

fail:
  return DefaultDelimiterList();
}

void DelimiterList_RdbSave(RedisModuleIO* rdb, DelimiterList dl) {
  if (dl != NULL) {
    RedisModule_SaveStringBuffer(rdb, dl, strlen(dl) + 1);
  }
}

// TODO: do we need this function?
// void DelimiterList_Ref(DelimiterList dl) {
// }

void ReplyWithDelimiterList(RedisModule_Reply* reply, DelimiterList dl) {
  RedisModule_Reply_SimpleString(reply, "delimiters");

  RedisModule_Reply_Array(reply);
  if (dl == NULL) {
    RedisModule_Reply_Null(reply);
  } else {
    RedisModule_Reply_StringBuffer(reply, dl, strlen(dl));
  }
  RedisModule_Reply_ArrayEnd(reply);
}

// TODO:
void AddDelimiterListToInfo(RedisModuleInfoCtx* ctx, DelimiterList dl) {
}

// TODO:
// char* GetDelimiterList(DelimiterList* dl) {
//   return NULL;
// }
