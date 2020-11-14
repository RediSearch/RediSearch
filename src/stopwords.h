
#pragma once

#include "redismodule.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct StopWordList {
  TrieMap *m;
  size_t refcount;

  StopWordList() { ctor(); }

  // Create a new stopword list from a list of NULL-terminated C strings
  StopWordList(const char **strs, size_t len) { ctor(strs, len); }

  // Create a new stopword list from a list of redis strings
  StopWordList(RedisModuleString **strs, size_t len);

  // Load a stopword list from RDB
  StopWordList(RedisModuleIO *rdb, int encver);

  void ctor(const char **strs = NULL, size_t len = 0);

  ~StopWordList();

  // Check if a stopword list contains a term. The term must be already lowercased
  int Contains(const char *term, size_t len) const;

  // Save a stopword list to RDB
  void RdbSave(RedisModuleIO *rdb) const;

  void ReplyWithStopWordsList(RedisModuleCtx *ctx) const;
};

StopWordList *DefaultStopWordList();
StopWordList *EmptyStopWordList();

#if 0

// Free a stopword list's memory
void StopWordList_Unref(StopWordList *sl);

#define StopWordList_Free StopWordList_Unref

void StopWordList_Ref(StopWordList *sl);

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
