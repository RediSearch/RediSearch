/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "trie/trie_type.h"
#include "index.h"

#define MIN_SUFFIX 2

typedef enum {
    SUFFIX_TYPE_SUFFIX = 0,
    SUFFIX_TYPE_CONTAINS = 1,
    SUFFIX_TYPE_WILDCARD = 2,    
} SuffixType;

/***********************************************************/
/*****************        Trie          ********************/
/***********************************************************/
typedef struct SuffixCtx {
    TrieNode *root;
    rune *rune;
    size_t runelen;
    const char *cstr;
    size_t cstrlen;
    SuffixType type;
    TrieSuffixCallback *callback;
    void *cbCtx;
    struct timespec *timeout;
} SuffixCtx;


void addSuffixTrie(Trie *trie, const char *str, uint32_t len);
void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len);

void suffixTrie_freeCallback(void *data);

/* Iterate on suffix trie and add use callback function on results */
void Suffix_IterateContains(SuffixCtx *sufCtx);

/* Iterate on suffix trie and add use callback function on results 
 * If wildcard pattern does not support suffix trie, return 0, else return 1. */
int Suffix_IterateWildcard(SuffixCtx *sufCtx);


/***********************************************************/
/*****************        TrieMap       ********************/
/***********************************************************/
size_t addSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);
void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);

void suffixTrieMap_freeCallback(void *payload);

/* Return a list of list of terms which match the suffix or contains term */
arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, const char *str, uint32_t len,
                                        bool prefix, struct timespec timeout);

/* Return a list of terms which match the wildcard pattern
 * If pattern does not match using suffix trie, return 0xBAAAAAAD */
arrayof(char*) GetList_SuffixTrieMap_Wildcard(TrieMap *trie, const char *pattern, uint32_t len,
                                               struct timespec timeout, long long maxPrefixExpansions);

/* Breaks wildcard at '*'s and finds the best token to get iterate the suffix trie.
 * tokenIdx and tokenLen arrays should sufficient space for all tokens. Max (len / 2) + 1.
 * The function does not assume str is NULL terminated. */
int Suffix_ChooseToken(const char *str, size_t len, size_t *tokenIdx, size_t *tokenLen);
int Suffix_ChooseToken_rune(const rune *str, size_t len, size_t *tokenIdx, size_t *tokenLen);

#ifdef __cplusplus
}
#endif
