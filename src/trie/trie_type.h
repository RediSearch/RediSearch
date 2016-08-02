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
    float rawScore;
} TrieSearchResult;

#define SCORE_TRIM_FACTOR 10.0

#define TRIE_ADD_CMD "FT.SUGADD"
#define TRIE_LEN_CMD "FT.SUGLEN"
#define TRIE_SEARCH_CMD "FT.SUGGET"

Trie *NewTrie();
void Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr);
void Trie_InsertStringBuffer(Trie *t, char *s, size_t len, double score, int incr);
void TrieSearchResult_Free(TrieSearchResult *e);
Vector *Trie_Search(Trie *tree, char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim);

/* Commands related to the redis TrieType registration */
int TrieType_Register(RedisModuleCtx *ctx);
void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver);
void TrieType_RdbSave(RedisModuleIO *rdb, void *value);
void TrieType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);
void TrieType_Digest(RedisModuleDigest *digest, void *value);
void TrieType_Free(void *value);

#endif
