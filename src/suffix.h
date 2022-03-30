#pragma once

#ifdef __cplusplus
extern "C" {
#endif

//#include "triemap/triemap.h"
#include "trie/trie_type.h"
#include "index.h"

#define MIN_SUFFIX 2

// arrayof(char *) findSuffix(TrieMap *suffix, const char *str, uint32_t len);
// arrayof(char **) findSuffixContains(TrieMap *suffix, const char *str, uint32_t len);

void writeSuffixTrie(Trie *trie, const char *str, uint32_t len);
void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len);
void SuffixTrieFree(Trie *suffix);

// void writeSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);
// void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);
// void SuffixTrieFreeMap(TrieMap *suffix);

#ifdef __cplusplus
}
#endif
