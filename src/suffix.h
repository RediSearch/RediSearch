#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "trie/trie_type.h"
#include "index.h"

#define MIN_SUFFIX 2

// arrayof(char *) findSuffix(TrieMap *suffix, const char *str, uint32_t len);
// arrayof(char **) findSuffixContains(TrieMap *suffix, const char *str, uint32_t len);

void addSuffixTrie(Trie *trie, const char *str, uint32_t len);
void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len);
void SuffixTrieFree(Trie *suffix);

void Suffix_IterateContains(TrieNode *n, const rune *str, size_t nstr, bool prefix,
                              TrieSuffixCallback callback, void *ctx);

#ifdef __cplusplus
}
#endif
