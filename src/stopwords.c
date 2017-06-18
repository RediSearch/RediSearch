#define __REDISEARCH_STOPORWORDS_C__
#include "stopwords.h"
#include "dep/triemap/triemap.h"
#include "rmalloc.h"
#include <ctype.h>

#define MAX_STOPWORDLIST_SIZE 1024

typedef struct StopWordList { TrieMap *m; } StopWordList;

StopWordList *__default_stopwords = NULL;

StopWordList *DefaultStopWordList() {
  if (__default_stopwords == NULL) {
    __default_stopwords = NewStopWordListCStr(DEFAULT_STOPWORDS,
                                              sizeof(DEFAULT_STOPWORDS) / sizeof(const char *) - 1);
  }
  return __default_stopwords;
}

/* Check if a stopword list contains a term. The term must be already lowercased */
int StopWordList_Contains(StopWordList *sl, const char *term, size_t len) {
  if (!sl || !term) {
    return 0;
  }

  return TrieMap_Find(sl->m, (char *)term, len) != TRIEMAP_NOTFOUND;
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
  if (len > MAX_STOPWORDLIST_SIZE) {
    len = MAX_STOPWORDLIST_SIZE;
  }
  StopWordList *sl = rm_malloc(sizeof(*sl));
  sl->m = NewTrieMap();

  for (size_t i = 0; i < len; i++) {

    char *t = strdup(strs[i]);
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
    free(t);
  }

  return sl;
}

/* Free a stopword list's memory */
void StopWordList_Free(StopWordList *sl) {
  if (sl) {
    TrieMap_Free(sl->m, NULL);
  }
  rm_free(sl);
}

/* Load a stopword list from RDB */
StopWordList *StopWordList_RdbLoad(RedisModuleIO *rdb, int encver) {
  uint64_t elements = RedisModule_LoadUnsigned(rdb);
  StopWordList *sl = rm_malloc(sizeof(*sl));
  sl->m = NewTrieMap();

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
