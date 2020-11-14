
#define __REDISEARCH_STOPORWORDS_C__
#include "stopwords.h"

#include "dep/triemap/triemap.h"
#include "rmalloc.h"

#include <ctype.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define MAX_STOPWORDLIST_SIZE 1024

//---------------------------------------------------------------------------------------------

static const char *DEFAULT_STOPWORDS[] = {
    "a",    "is",    "the",   "an",   "and",  "are", "as",  "at",   "be",   "but",  "by",   "for",
    "if",   "in",    "into",  "it",   "no",   "not", "of",  "on",   "or",   "such", "that", "their",
    "then", "there", "these", "they", "this", "to",  "was", "will", "with", NULL};

static StopWordList default_stopwords(DEFAULT_STOPWORDS, sizeof(DEFAULT_STOPWORDS)/sizeof(const char *) - 1);

StopWordList *DefaultStopWordList() {
  return &default_stopwords;
}

//---------------------------------------------------------------------------------------------

static StopWordList empty_stopwords;

StopWordList *EmptyStopWordList() {
  return &empty_stopwords;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Check if a stopword list contains a term. The term must be already lowercased

bool StopWordList::Contains(const char *term, size_t len) const {
  if (!term) {
    return 0;
  }

  return TrieMap_Find(m, (char *)term, len) != TRIEMAP_NOTFOUND;
}

//---------------------------------------------------------------------------------------------

// Create a new stopword list from a list of redis strings

StopWordList::StopWordList(RedisModuleString **strs, size_t len) {
  if (len > MAX_STOPWORDLIST_SIZE) {
    len = MAX_STOPWORDLIST_SIZE;
  }
  const char *cstrs[len];
  for (size_t i = 0; i < len && i < MAX_STOPWORDLIST_SIZE; i++) {
    cstrs[i] = (char *)RedisModule_StringPtrLen(strs[i], NULL);
  }

  ctor(cstrs, len);
}

//---------------------------------------------------------------------------------------------

void StopWordList::ctor(const char **strs, size_t len) {
  if (len > MAX_STOPWORDLIST_SIZE) {
    len = MAX_STOPWORDLIST_SIZE;
  }
  refcount = 1;
  m = NewTrieMap();

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
    TrieMap_Add(m, t, tlen, NULL, NULL);
    rm_free(t);
  }
}

//---------------------------------------------------------------------------------------------

StopWordList::~StopWordList() {
  TrieMap_Free(m, NULL);
}

//---------------------------------------------------------------------------------------------

#if 0

void StopWordList::Ref() {
  __sync_fetch_and_add(&refcount, 1);
}

//---------------------------------------------------------------------------------------------

// Free a stopword list's memory
void StopWordList::Unref() {
  if (__sync_sub_and_fetch(&refcount, 1)) {
    return;
  }
  delete this;
}

#endif // 0

//---------------------------------------------------------------------------------------------

// Load a stopword list from RDB
StopWordList::StopWordList(RedisModuleIO *rdb, int encver) {
  uint64_t elements = RedisModule_LoadUnsigned(rdb);
  m = NewTrieMap();
  refcount = 1;

  while (elements--) {
    size_t len;
    char *str = RedisModule_LoadStringBuffer(rdb, &len);
    TrieMap_Add(m, str, len, NULL, NULL);
    RedisModule_Free(str);
  }
}

//---------------------------------------------------------------------------------------------

// Save a stopword list to RDB
void StopWordList::RdbSave(RedisModuleIO *rdb) {
  RedisModule_SaveUnsigned(rdb, m->cardinality);
  TrieMapIterator *it = TrieMap_Iterate(m, "", 0);
  char *str;
  tm_len_t len;
  void *ptr;

  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    RedisModule_SaveStringBuffer(rdb, str, len);
  }
  TrieMapIterator_Free(it);
}

//---------------------------------------------------------------------------------------------

void StopWordList::ReplyWithStopWordsList(RedisModuleCtx *ctx) {
  RedisModule_ReplyWithSimpleString(ctx, "stopwords_list");

#if 0
  if (sl == NULL) {
    RedisModule_ReplyWithArray(ctx, 1);
    RedisModule_ReplyWithNull(ctx);
    return;
  }
#endif
  TrieMapIterator *it = TrieMap_Iterate(m, "", 0);
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

///////////////////////////////////////////////////////////////////////////////////////////////
