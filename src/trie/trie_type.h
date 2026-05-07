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

#include "rmutil/rm_assert.h"
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

/* Insert a rune-keyed entry without updating the trie's size counter. Behaves
 * exactly like calling TrieNode_Add(&t->root, ...) directly: no length guard,
 * no size bookkeeping.
 *
 * Intended only for the suffix-trie full-word insert in addSuffixTrie(), which
 * historically called TrieNode_Add directly to bypass both the size update
 * (suffix tries never read ->size) and Trie_InsertRune's length guard (the
 * suffix trie's full-word entries are not subject to that guard).
 *
 * Do not use for new call sites - prefer Trie_InsertRune so size stays in sync
 * with TrieNode_Add's return value. */
int Trie_InsertRuneNoSize(Trie *t, const rune *s, size_t len, double score, int incr,
                          RSPayload *payload, size_t numDocs);

/* Get the payload from the node. if `exact` is 0, the payload is return even if local offset!=len
   Use for debug only! */
void *Trie_GetValueStringBuffer(Trie *t, const char *s, size_t len, bool exact);
void *Trie_GetValueRune(Trie *t, const rune *runes, size_t len, bool exact);

/* Delete the string from the trie. Return 1 if the node was found and deleted, 0 otherwise */
int Trie_Delete(Trie *t, const char *s, size_t len);
int Trie_DeleteRunes(Trie *t, const rune *runes, size_t len);

/* Look up a node by rune key. Wraps TrieNode_Get on the trie's root so callers do not
 * need to reach into Trie internals. See TrieNode_Get for parameter semantics. */
TrieNode *Trie_GetNode(Trie *t, const rune *str, t_len len, bool exact, int *offsetOut);

/* Iterate all nodes within a lexicographic range. Wraps TrieNode_IterateRange on the
 * trie's root. See TrieNode_IterateRange for parameter semantics. */
void Trie_IterateRange(Trie *t, const rune *min, int minlen, bool includeMin,
                       const rune *max, int maxlen, bool includeMax,
                       TrieRangeCallback callback, void *ctx);

/* Iterate all nodes that contain (or begin/end with) the given pattern. Wraps
 * TrieNode_IterateContains on the trie's root. See TrieNode_IterateContains for
 * parameter semantics. */
void Trie_IterateContains(Trie *t, const rune *str, int nstr, bool prefix, bool suffix,
                          TrieRangeCallback callback, void *ctx, struct timespec *timeout,
                          bool skipTimeoutChecks);

/* Iterate all nodes matching a wildcard pattern. Wraps TrieNode_IterateWildcard on the
 * trie's root. See TrieNode_IterateWildcard for parameter semantics. */
void Trie_IterateWildcard(Trie *t, const rune *str, int nstr,
                          TrieRangeCallback callback, void *ctx, struct timespec *timeout,
                          bool skipTimeoutChecks);

/* Number of terminal entries in the trie. Wraps the internal size counter. */
static inline size_t Trie_Size(const Trie *t) {
  RS_ASSERT(t);
  return t->size;
}

/* Iterate every node in the trie with no filter or distance constraint. Wraps
 * TrieNode_Iterate on the trie's root with no filter. Used by debug paths that
 * want raw traversal; production code should prefer Trie_Iterate with a
 * prefix/maxDist. */
TrieIterator *Trie_IterateAll(Trie *t);

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
