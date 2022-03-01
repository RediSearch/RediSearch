#pragma once

#include "triemap/triemap.h"

#define MIN_SUFFIX 2

void writeSuffixTrie(TrieMap *trie, const char *str, uint32_t len);

// void deleteSuffix(IndexSpec *spec, char *str, uint32_t len);
void deleteSuffixTrie(TrieMap *trie, const char *str, uint32_t len);

void SuffixTrieFree(TrieMap *suffix);
