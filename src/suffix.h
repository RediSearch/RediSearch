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
    const rune *rune;
    size_t runelen;
    const char *cstr;
    size_t cstrlen;
    SuffixType type;
    TrieSuffixCallback *callback;
    void *cbCtx;
} SuffixCtx;

void addSuffixTrie(Trie *trie, const char *str, uint32_t len);
void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len);

void suffixTrie_freeCallback(void *data);

void Suffix_IterateContains(SuffixCtx *sufCtx);

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
