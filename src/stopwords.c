#include "stopwords.h"
#include "dep/triemap/triemap.h"
#include "rmalloc.h"
#include <ctype.h>

typedef struct StopWordList { TrieMap *m; } StopWordList;

/* Check if a stopword list contains a term. The term must be already lowercased */
int StopWordList_Contains(StopWordList *sl, const char *term, size_t len) {
  if (!sl || !sl->m) {
    return 0;
  }

  return TrieMap_Find(sl->m, (char *)term, len) != TRIEMAP_NOTFOUND;
}

/* Create a new stopword list from a list of redis strings */
StopWordList *NewStopWordList(RedisModuleString **strs, size_t len) {
  StopWordList *sl = rm_malloc(sizeof(*sl));
  sl->m = NewTrieMap();

  for (size_t i = 0; i < len; i++) {
    size_t tlen;
    char *t = (char *)RedisModule_StringPtrLen(strs[i], &tlen);
    if (!t || !tlen) {
      continue;
    }

    // lowercase the letters
    for (size_t pos = 0; pos < tlen; pos++) {
      if (isalpha(t[pos])) {
        t[pos] = tolower(t[pos]);
      }
    }

    TrieMap_Add(sl->m, t, tlen, NULL, NULL);
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
}
