#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "trie/trie_type.h"
#include "index.h"

#define MIN_SUFFIX 2

/***********************************************************/
/*****************        Trie          ********************/
/***********************************************************/
void addSuffixTrie(Trie *trie, const char *str, uint32_t len);
void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len);

void suffixTrie_freeCallback(void *data);

void Suffix_IterateContains(TrieNode *n, const rune *str, size_t nstr, bool prefix,
                              TrieSuffixCallback callback, void *ctx);


/***********************************************************/
/*****************        TrieMap       ********************/
/***********************************************************/
void addSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);
void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len);

void suffixTrieMap_freeCallback(void *payload);

arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, const char *str, uint32_t len,
                                        bool prefix, struct timespec timeout);

arrayof(char*) GetList_SuffixTrieMap_Wildcard(TrieMap *trie, const char *str, uint32_t len,
                                               struct timespec timeout);

#ifdef __cplusplus
}
#endif
