/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __TRIE_TYPE_H__
#define __TRIE_TYPE_H__

#include "redismodule.h"

#include "trie.h"
#include "levenshtein.h"

#ifdef __cplusplus
extern "C" {
#endif

extern RedisModuleType *TrieType;

#define TRIE_ENCVER_CURRENT 1
#define TRIE_ENCVER_NOPAYLOADS 0

typedef struct {
  TrieNode *root;
  size_t size;
  TrieFreeCallback freecb;
  TrieSortMode sortMode;
} Trie;

typedef struct {
  char *str;
  size_t len;
  float score;
  char *payload;
  size_t plen;
} TrieSearchResult;

#define SCORE_TRIM_FACTOR 10.0

/* Creates a new Trie.
 * Trie can be sorted by lexicographic order using `Trie_Sort_Lex` or by
 * score using `Trie_Sort_Score.                            */
Trie *NewTrie(TrieFreeCallback freecb, TrieSortMode sortMode);

int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr, RSPayload *payload);
int Trie_InsertStringBuffer(Trie *t, const char *s, size_t len, double score, int incr,
                            RSPayload *payload);
int Trie_InsertRune(Trie *t, const rune *s, size_t len, double score, int incr,
                            RSPayload *payload);

/* Get the payload from the node. if `exact` is 0, the payload is return even if local offset!=len 
   Use for debug only! */
void *Trie_GetValueStringBuffer(Trie *t, const char *s, size_t len, bool exact);
void *Trie_GetValueRune(Trie *t, const rune *runes, size_t len, bool exact);

/* Delete the string from the trie. Return 1 if the node was found and deleted, 0 otherwise */
int Trie_Delete(Trie *t, const char *s, size_t len);
int Trie_DeleteRunes(Trie *t, const rune *runes, size_t len);

void TrieSearchResult_Free(TrieSearchResult *e);
Vector *Trie_Search(Trie *tree, const char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize);

/* Iterate  the trie, using maxDist edit distance, returning a trie iterator that the
 * caller needs to free. If prefixmode is 1 we treat the string as only a prefix to iterate.
 * Otherwise we return an iterator to all strings within maxDist Levenshtein distance */
TrieIterator *Trie_Iterate(Trie *t, const char *prefix, size_t len, int maxDist, int prefixMode);

/* Get a random key from the trie, and put the node's score in the score pointer. Returns 0 if the
 * trie is empty and we cannot do that */
int Trie_RandomKey(Trie *t, char **str, t_len *len, double *score);

/* Commands related to the redis TrieType registration */
int TrieType_Register(RedisModuleCtx *ctx);
void *TrieType_GenericLoad(RedisModuleIO *rdb, int loadPayloads);
void TrieType_GenericSave(RedisModuleIO *rdb, Trie *t, int savePayloads);
void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver);
void TrieType_RdbSave(RedisModuleIO *rdb, void *value);
void TrieType_Digest(RedisModuleDigest *digest, void *value);
size_t TrieType_MemUsage(const void *value);
void TrieType_Free(void *value);

#ifdef __cplusplus
}
#endif
#endif
