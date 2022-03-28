#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "triemap/triemap.h"

#define MIN_SUFFIX 2

arrayof(char *) findSuffix(TrieMap *suffix, const char *str, uint32_t len);
arrayof(char **) findSuffixContains(TrieMap *suffix, const char *str, uint32_t len);

void writeSuffixTrie(TrieMap *trie, const char *str, uint32_t len);

void deleteSuffixTrie(TrieMap *trie, const char *str, uint32_t len);

void SuffixTrieFree(TrieMap *suffix);

#ifdef __cplusplus
}
#endif
