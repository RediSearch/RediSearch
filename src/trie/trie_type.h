#ifndef __TRIE_TYPE_H__
#define __TRIE_TYPE_H__

#include "../redismodule.h"

#include "trie.h"
#include "levenshtein.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisModuleType *TrieType;

#define TRIE_ENCVER_CURRENT 1
#define TRIE_ENCVER_NOPAYLOADS 0

struct Trie {
  TrieNode *root;
  size_t size;

  Trie();

  int Delete(const char *s, size_t len);
  int Insert(RedisModuleString *s, double score, int incr, RSPayload *payload);
  int InsertStringBuffer(const char *s, size_t len, double score, int incr, RSPayload *payload);
  Vector *Search(const char *s, size_t len, size_t num, int maxDist, int prefixMode, int trim, int optimize);

  TrieIterator *Iterate(const char *prefix, size_t len, int maxDist, int prefixMode);

  bool RandomKey(char **str, t_len *len, double *score);
};

struct TrieSearchResult : Object {
  char *str;
  size_t len;
  float score;
  char *payload;
  size_t plen;

  ~TrieSearchResult()
};

#define SCORE_TRIM_FACTOR 10.0

/* Commands related to the redis TrieType registration */
int TrieType_Register(RedisModuleCtx *ctx);
void *TrieType_GenericLoad(RedisModuleIO *rdb, int loadPayloads);
void TrieType_GenericSave(RedisModuleIO *rdb, Trie *t, int savePayloads);
void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver);
void TrieType_RdbSave(RedisModuleIO *rdb, void *value);
void TrieType_Digest(RedisModuleDigest *digest, void *value);
void TrieType_Free(void *value);

#ifdef __cplusplus
}
#endif
#endif
