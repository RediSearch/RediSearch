/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#define __REDISEARCH_STOPORWORDS_C__
#include "stopwords.h"
#include "triemap.h"
#include "rmalloc.h"
#include "util/strconv.h"
#include "rmutil/rm_assert.h"
#include <ctype.h>
#include "rdb.h"

#define MAX_STOPWORDLIST_SIZE 1024

typedef struct StopWordList {
  TrieMap *m;
  size_t refcount;
} StopWordList;

static StopWordList *__default_stopwords = NULL;
static StopWordList *__empty_stopwords = NULL;

StopWordList *DefaultStopWordList() {
  if (__default_stopwords == NULL) {
    __default_stopwords = NewStopWordListCStr(DEFAULT_STOPWORDS,
                                              sizeof(DEFAULT_STOPWORDS) / sizeof(const char *) - 1);
  }
  return __default_stopwords;
}

/* Check if a stopword list contains a term. */
int StopWordList_Contains(const StopWordList *sl, const char *term, size_t len) {
  char *lowStr;
  char stackStr[32];
  if (sl == __empty_stopwords || !sl || !term) {
    return 0;
  }

  // do not use heap allocation for short strings
  if (len < 32) {
    memcpy(stackStr, term, len);
    stackStr[len] = '\0';
    lowStr = stackStr;
  } else {
    lowStr = rm_strndup(term, len);
  }

  // convert multi-byte characters to lowercase
  char *longerDst = unicode_tolower(lowStr, &len);
  if (longerDst) {
    if (lowStr != stackStr) {
      rm_free(lowStr);
    }
    lowStr = longerDst;
  } else {
    // No memory allocation, just ensure null termination
    lowStr[len] = '\0';
  }

  int ret = TrieMap_Find(sl->m, (char *)lowStr, len) != TRIEMAP_NOTFOUND;

  // free memory if allocated
  if (lowStr != stackStr) rm_free(lowStr);

  return ret;
}

StopWordList *NewStopWordListCStr(const char **strs, size_t len) {
  if (len == 0 && __empty_stopwords) {
    return __empty_stopwords;
  }
  if (len > MAX_STOPWORDLIST_SIZE) {
    len = MAX_STOPWORDLIST_SIZE;
  }
  StopWordList *sl = rm_malloc(sizeof(*sl));
  sl->refcount = 1;
  sl->m = NewTrieMap();

  for (size_t i = 0; i < len; i++) {

    char *t = rm_strdup(strs[i]);
    if (t == NULL) {
      break;
    }
    size_t tlen = strlen(t);

    // convert multi-byte characters to lowercase
    char *dst = unicode_tolower(t, &tlen);
    if (dst) {
        rm_free(t);
        t = dst;
    } else {
      // No memory allocation, just ensure null termination
      t[tlen] = '\0';
    }

    TrieMap_Add(sl->m, t, tlen, NULL, NULL);
    rm_free(t);
  }
  if (len == 0) {
    __empty_stopwords = sl;
  }
  return sl;
}

void StopWordList_Ref(StopWordList *sl) {
  __sync_fetch_and_add(&sl->refcount, 1);
}

static void StopWordList_FreeInternal(StopWordList *sl) {
  if (sl) {
    TrieMap_Free(sl->m, NULL);
    rm_free(sl);
  }
}

/* Free a stopword list's memory */
void StopWordList_Unref(StopWordList *sl) {
  if (sl == __default_stopwords || sl == __empty_stopwords) {
    return;
  }

  if (__sync_sub_and_fetch(&sl->refcount, 1)) {
    return;
  }
  StopWordList_FreeInternal(sl);
}

void StopWordList_FreeGlobals(void) {
  if (__default_stopwords) {
    StopWordList_FreeInternal(__default_stopwords);
    __default_stopwords = NULL;
  }

  if (__empty_stopwords) {
    StopWordList_FreeInternal(__empty_stopwords);
    __empty_stopwords = NULL;
  }
}

/* Load a stopword list from RDB */
StopWordList *StopWordList_RdbLoad(RedisModuleIO *rdb, int encver) {
  StopWordList *sl = NULL;
  uint64_t elements = LoadUnsigned_IOError(rdb, goto cleanup);
  sl = rm_malloc(sizeof(*sl));
  sl->m = NewTrieMap();
  sl->refcount = 1;

  while (elements--) {
    size_t len;
    char *str = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
    TrieMap_Add(sl->m, str, len, NULL, NULL);
    RedisModule_Free(str);
  }

  return sl;

cleanup:
  if (sl) {
    StopWordList_FreeInternal(sl);
  }
  return NULL;
}

/* Save a stopword list to RDB */
void StopWordList_RdbSave(RedisModuleIO *rdb, StopWordList *sl) {

  RedisModule_SaveUnsigned(rdb, TrieMap_NUniqueKeys(sl->m));
  TrieMapIterator *it = TrieMap_Iterate(sl->m);
  char *str;
  tm_len_t len;
  void *ptr;

  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    RedisModule_SaveStringBuffer(rdb, str, len);
  }
  TrieMapIterator_Free(it);
}

void ReplyWithStopWordsList(RedisModule_Reply *reply, struct StopWordList *sl) {
  RedisModule_Reply_SimpleString(reply, "stopwords_list");

  if (sl == NULL) {
    RedisModule_Reply_Array(reply);
      RedisModule_Reply_Null(reply);
    RedisModule_Reply_ArrayEnd(reply);
    return;
  }

  TrieMapIterator *it = TrieMap_Iterate(sl->m);
  char *str;
  tm_len_t len;
  void *ptr;

  RedisModule_Reply_Array(reply);
    for (size_t i = 0; TrieMapIterator_Next(it, &str, &len, &ptr); ++i) {
      RedisModule_Reply_StringBuffer(reply, str, len);
    }
  RedisModule_Reply_ArrayEnd(reply);

  TrieMapIterator_Free(it);

}

#ifdef FTINFO_FOR_INFO_MODULES
void AddStopWordsListToInfo(RedisModuleInfoCtx *ctx, struct StopWordList *sl) {
  if (sl == NULL) {
    return;
  }

  TrieMapIterator *it = TrieMap_Iterate(sl->m, "", 0);
  char *str;
  tm_len_t len;
  void *ptr;
  bool first = true;
  arrayof(char) stopwords = array_new(char, 512);
  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    stopwords = array_ensure_append_1(stopwords, "\"");
    stopwords = array_ensure_append_n(stopwords, str, len);
    stopwords = array_ensure_append_n(stopwords, "\",", 2);
  }
  stopwords[array_len(stopwords)-1] = '\0';
  RedisModule_InfoAddFieldCString(ctx, "stop_words", stopwords);
  array_free(stopwords);
  TrieMapIterator_Free(it);
}
#endif

char **GetStopWordsList(struct StopWordList *sl, size_t *size) {
  *size = TrieMap_NUniqueKeys(sl->m);
  if (*size == 0) {
    return NULL;
  }

  char **list = rm_malloc((*size) * sizeof(*list));

  TrieMapIterator *it = TrieMap_Iterate(sl->m);
  char *str;
  tm_len_t len;
  void *ptr;
  size_t i = 0;

  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    list[i++] = rm_strndup(str, len);
  }

  TrieMapIterator_Free(it);
  RS_LOG_ASSERT(i == *size, "actual size must equal expected size");

  return list;
}
