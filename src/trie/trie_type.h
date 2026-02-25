/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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

#define TRIE_ENCVER_CURRENT 2
#define TRIE_ENCVER_NUMDOCS 2
#define TRIE_ENCVER_PAYLOADS 1

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

int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr, RSPayload *payload,
                size_t numDocs);
int Trie_InsertStringBuffer(Trie *t, const char *s, size_t len, double score, int incr,
                            RSPayload *payload, size_t numDocs);
int Trie_InsertRune(Trie *t, const rune *s, size_t len, double score, int incr,
                    RSPayload *payload, size_t numDocs);

/* Get the payload from the node. if `exact` is 0, the payload is return even if local offset!=len
   Use for debug only! */
void *Trie_GetValueStringBuffer(Trie *t, const char *s, size_t len, bool exact);
void *Trie_GetValueRune(Trie *t, const rune *runes, size_t len, bool exact);

/* Delete the string from the trie. Return 1 if the node was found and deleted, 0 otherwise */
int Trie_Delete(Trie *t, const char *s, size_t len);
int Trie_DeleteRunes(Trie *t, const rune *runes, size_t len);

/* Result codes for Trie_DecrementNumDocs */
typedef enum {
  TRIE_DECR_NOT_FOUND = 0,   /* Term not found in trie */
  TRIE_DECR_UPDATED = 1,     /* numDocs decremented, still > 0 */
  TRIE_DECR_DELETED = 2,     /* numDocs reached 0, node deleted */
} TrieDecrResult;

/* Decrement the numDocs count for a term in the trie.
 * If numDocs reaches 0, the node is marked as deleted.
 * Parameters:
 *   t     - the trie
 *   s     - UTF-8 encoded term string
 *   len   - length of the string in bytes
 *   delta - amount to decrement numDocs by
 * Returns:
 *   TRIE_DECR_NOT_FOUND - term not found
 *   TRIE_DECR_UPDATED   - numDocs decremented but still > 0
 *   TRIE_DECR_DELETED   - numDocs reached 0, node deleted
 */
TrieDecrResult Trie_DecrementNumDocs(Trie *t, const char *s, size_t len, size_t delta);

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
void *TrieType_GenericLoad(RedisModuleIO *rdb, bool loadPayloads, bool loadNumDocs);
void TrieType_GenericSave(RedisModuleIO *rdb, Trie *t, bool savePayloads, bool saveNumDocs);
void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver);
void TrieType_RdbSave(RedisModuleIO *rdb, void *value);
size_t TrieType_MemUsage(const void *value);
void TrieType_Free(void *value);

#ifdef __cplusplus
}
#endif
#endif
