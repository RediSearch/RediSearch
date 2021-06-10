#define __REDISEARCH_STOPORWORDS_C__
#include "stopwords.h"
#include "dep/triemap/triemap.h"
#include "rmalloc.h"
#include "util/strconv.h"
#include <ctype.h>

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

  strtolower(lowStr);
  int ret = TrieMap_Find(sl->m, (char *)lowStr, len) != TRIEMAP_NOTFOUND;

  // free memory if allocated 
  if (len >= 32) rm_free(lowStr);

  return ret;
}

/* Create a new stopword list from a list of redis strings */
StopWordList *NewStopWordList(RedisModuleString **strs, size_t len) {

  if (len > MAX_STOPWORDLIST_SIZE) {
    len = MAX_STOPWORDLIST_SIZE;
  }
  const char *cstrs[len];
  for (size_t i = 0; i < len && i < MAX_STOPWORDLIST_SIZE; i++) {
    cstrs[i] = (char *)RedisModule_StringPtrLen(strs[i], NULL);
  }

  return NewStopWordListCStr(cstrs, len);
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

    // lowercase the letters
    for (size_t pos = 0; pos < tlen; pos++) {
      if (isalpha(t[pos])) {
        t[pos] = tolower(t[pos]);
      }
    }
    // printf("Adding stopword %s\n", t);
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
  }
  rm_free(sl);
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
  uint64_t elements = RedisModule_LoadUnsigned(rdb);
  StopWordList *sl = rm_malloc(sizeof(*sl));
  sl->m = NewTrieMap();
  sl->refcount = 1;

  while (elements--) {
    size_t len;
    char *str = RedisModule_LoadStringBuffer(rdb, &len);
    TrieMap_Add(sl->m, str, len, NULL, NULL);
    RedisModule_Free(str);
  }

  return sl;
}

/* Save a stopword list to RDB */
void StopWordList_RdbSave(RedisModuleIO *rdb, StopWordList *sl) {

  RedisModule_SaveUnsigned(rdb, sl->m->cardinality);
  TrieMapIterator *it = TrieMap_Iterate(sl->m, "", 0);
  char *str;
  tm_len_t len;
  void *ptr;

  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    RedisModule_SaveStringBuffer(rdb, str, len);
  }
  TrieMapIterator_Free(it);
}

void ReplyWithStopWordsList(RedisModuleCtx *ctx, struct StopWordList *sl) {
  RedisModule_ReplyWithSimpleString(ctx, "stopwords_list");

  if (sl == NULL) {
    RedisModule_ReplyWithArray(ctx, 1);
    RedisModule_ReplyWithNull(ctx);
    return;
  }

  TrieMapIterator *it = TrieMap_Iterate(sl->m, "", 0);
  char *str;
  tm_len_t len;
  void *ptr;
  size_t i = 0;

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    RedisModule_ReplyWithStringBuffer(ctx, str, len);
    ++i;
  }
  RedisModule_ReplySetArrayLength(ctx, i);
  TrieMapIterator_Free(it);
}
