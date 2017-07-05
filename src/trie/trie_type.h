#ifndef __TRIE_TYPE_H__
#define __TRIE_TYPE_H__

#include "../redismodule.h"

#include "trie.h"
#include "levenshtein.h"

extern RedisModuleType *TrieType;

typedef struct {
  TrieNode *root;
  size_t size;
} Trie;

typedef struct {
  char *str;
  size_t len;
  float score;
  char *payload;
  size_t plen;
} TrieSearchResult;

#define SCORE_TRIM_FACTOR 10.0

#define TRIE_ADD_CMD "FT.SUGADD"
#define TRIE_LEN_CMD "FT.SUGLEN"
#define TRIE_SEARCH_CMD "FT.SUGGET"
#define TRIE_DEL_CMD "FT.SUGDEL"

#define STR_INFO_SPLIT_SEP  "$"
#define INFO_SPLIT_SEP "#"

void split(char *src, const char *separator, char **dest, int *num);

Trie *NewTrie();
int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr, RSPayload *payload);
int Trie_InsertStringBuffer(Trie *t, char *s, size_t len, double score, int incr, RSPayload *payload);
/* Delete the string from the trie. Return 1 if the node was found and deleted, 0 otherwise */
int Trie_Delete(Trie *t, char *s, size_t len);

void TrieSearchResult_Free(TrieSearchResult *e);
Vector *Trie_Search(Trie *tree, char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize);

/* Iterate a prefix in the trie, using maxDist edit distance, returning a trie iterator that the
 * caller needs to free */
TrieIterator *Trie_IteratePrefix(Trie *t, char *prefix, size_t len, int maxDist);

/* Commands related to the redis TrieType registration */
int TrieType_Register(RedisModuleCtx *ctx);
void *TrieType_GenericLoad(RedisModuleIO *rdb);
void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver);
void TrieType_RdbSave(RedisModuleIO *rdb, void *value);
void TrieType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);
void TrieType_Digest(RedisModuleDigest *digest, void *value);
void TrieType_Free(void *value);

#endif
