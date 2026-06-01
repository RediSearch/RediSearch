/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "trie/trie.h"
#include "util/arr.h"

typedef struct TrieMap TrieMap;

/* Minimum length for entries in the suffix trie / suffix triemap. The four
 * mutators (`add`/`delete` × `Trie`/`TrieMap`) silently no-op for values
 * shorter than this. Queries shorter than this fall back to brute-force on
 * the regular trie.
 *
 * Memory trade-off: storing length-1 sub-suffix entries would mean one node
 * per character with an array of every term ending in it — bounded storage
 * but very low query selectivity (a 1-char wildcard saturates
 * `MAX_PREFIX_EXPANSIONS` instantly). Unit is bytes for TAG (`TrieMap` is
 * byte-keyed) and runes for TEXT (`Trie` is rune-keyed).
 */
#define SUFFIX_DS_MIN_LEN 2

typedef enum {
    SUFFIX_TYPE_SUFFIX = 0,
    SUFFIX_TYPE_CONTAINS = 1,
    SUFFIX_TYPE_WILDCARD = 2,
} SuffixType;

/***********************************************************/
/*****************        Trie          ********************/
/***********************************************************/
typedef struct SuffixCtx {
    Trie *trie;
    rune *rune;
    size_t runelen;
    const char *cstr;
    size_t cstrlen;
    SuffixType type;
    TrieSuffixCallback *callback;
    void *cbCtx;
    struct timespec *timeout;
    bool skipTimeoutChecks;  // flag to skip timeout checks in trie iteration
} SuffixCtx;

typedef struct suffixData {
  // int wordExists; // exact match to string exists already
  // rune *rune;
  char *term;             // string is used in the array of all suffix tokens
  arrayof(char *) array;  // list of words containing the string. weak pointers
} suffixData;


/* Add string to suffix trie. If string already exists, do nothing.
 * In case of allocation overflow in TrieNode_Add, log error and return without
 * adding the string.
 */
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
void addSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);
void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);

void suffixTrieMap_freeCallback(void *payload);

/* Return a list of list of terms which match the suffix or contains term */
arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, const char *str, uint32_t len,
                                        bool prefix, struct timespec timeout, bool skipTimeoutChecks);

/* Return a list of terms which match the wildcard pattern
 * If pattern does not match using suffix trie, return 0xBAAAAAAD */
arrayof(char*) GetList_SuffixTrieMap_Wildcard(TrieMap *trie, const char *pattern, uint32_t len,
                                               struct timespec timeout, long long maxPrefixExpansions, bool skipTimeoutChecks);

/* Breaks wildcard at '*'s and finds the best token to get iterate the suffix trie.
 * tokenIdx and tokenLen arrays should sufficient space for all tokens. Max (len / 2) + 1.
 * The function does not assume str is NULL terminated. */
int Suffix_ChooseToken(const char *str, size_t len, size_t *tokenIdx, size_t *tokenLen);
int Suffix_ChooseToken_rune(const rune *str, size_t len, size_t *tokenIdx, size_t *tokenLen);

#ifdef __cplusplus
}
#endif
