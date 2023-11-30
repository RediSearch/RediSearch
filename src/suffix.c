/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include "wildcard/wildcard.h"

#include <string.h>
#include <strings.h>

typedef struct suffixData {
  // int wordExists; // exact match to string exists already
  // rune *rune;
  char *term;             // string is used in the array of all suffix tokens
  arrayof(char *) array;   // list of words containing the string. weak pointers
} suffixData;

#define Suffix_GetData(node) node ? node->payload ? \
                             (suffixData *)node->payload->data : NULL : NULL


/***********************************************************/
/*****************        Trie          ********************/
/***********************************************************/


static suffixData createSuffixNode(char *term, int keepPtr) {
  suffixData node = { 0 };
  if (keepPtr) {
    node.term = term;
  }
  node.array = array_ensure_append_1(node.array, term);
  return node;
}

static void freeSuffixNode(suffixData *node) {
  array_free(node->array);
  rm_free(node->term);
  rm_free(node);
}

void addSuffixTrie(Trie *trie, const char *str, uint32_t len) {
  //if () {}  check here gor other types
  size_t rlen = 0;
  runeBuf buf;
  rune *runes = runeBufFill(str, len, &buf, &rlen);

  TrieNode *trienode = TrieNode_Get(trie->root, runes, rlen, 1, NULL);
  suffixData *data = NULL;
  if (trienode && trienode->payload) {
    // suffixData *node = TrieNode_GetValue(trie->root, runes, rlen, 1);
    data = Suffix_GetData(trienode);
    // if string was added in the past, skip
    if (data && data->term) {
      //rm_free(runes);
      runeBufFree(&buf);
      return;
    }
  }

  char *copyStr = rm_strndup(str, len);
  if (!data) {
    suffixData newdata = createSuffixNode(copyStr, 1);
    RSPayload payload = { .data = (char*)&newdata, .len = sizeof(newdata) };
    TrieNode_Add(&trie->root, runes, rlen, &payload, 1, ADD_REPLACE, trie->freecb);
  } else {
    RS_LOG_ASSERT(!data->term, "can't reach here");
    data->term = copyStr;
    data->array = array_ensure_append_1(data->array, copyStr);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    TrieNode *trienode = TrieNode_Get(trie->root, runes + j, rlen - j, 1, NULL);
    
    data = Suffix_GetData(trienode);
    if (!trienode || !trienode->payload) {
      suffixData newdata = createSuffixNode(copyStr, 0);
      RSPayload payload = { .data = (char*)&newdata, .len = sizeof(newdata) };
      Trie_InsertRune(trie, runes + j, rlen - j, 1, ADD_REPLACE, &payload);
    } else {
      data->array = array_ensure_append_1(data->array, copyStr);
    }
  }
  runeBufFree(&buf);
}

static void removeSuffix(const char *str, size_t rlen, arrayof(char*) array) {
  for (int i = 0; i < array_len(array); ++i) {
    if (!strncmp(array[i], str, rlen)) {
      array = array_del_fast(array, i);
      return;
    }
  }
}

void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len) {
  size_t rlen = 0;
  //rune *runes = strToRunesN(str, len, &rlen);

  runeBuf buf;
  rune *runes = runeBufFill(str, len, &buf, &rlen);

  //rune runes[len];
  //size_t rlen = strToRunesN(str, len, &runes);
  char *oldTerm = NULL;

  // iterate all matching terms and remove word
  for (int j = 0; j < len - MIN_SUFFIX + 1; ++j) {
    TrieNode *node = TrieNode_Get(trie->root, runes + j, rlen - j, 1, NULL);
    suffixData *data = Suffix_GetData(node);
    // suffix trie is shared between all text fields in index, even if they don't use it.
    // if the trie is owned by other fields and not any one containing this suffix,
    // then failure to find the suffix is not an error. just move along.
    if (!data) continue;
    // RS_LOG_ASSERT(data, "all suffixes must exist");
    // suffixData *data = TrieMap_Find(trie, str + j, len - j);
    if (j == 0) {
      // keep pointer to word string to free after it was found in al sub tokens.
      oldTerm = data->term;
      data->term = NULL;
    }
    // remove from array
    removeSuffix(str, len, data->array);
    // if array is empty, remove the node
    if (array_len(data->array) == 0) {
      RS_LOG_ASSERT(!data->term, "array should contain a pointer to the string");
      Trie_DeleteRunes(trie, runes + j, rlen - j);
    }
  }
  rm_free(oldTerm);
  runeBufFree(&buf);
}

static int processSuffixData(suffixData *data, SuffixCtx *sufCtx) {
  //TrieSuffixCallback callback, void *ctx) {
  if (!data) {
    return REDISMODULE_OK;
  }
  arrayof(char *) array = data->array;
  for (int i = 0; i < array_len(array); ++i) {
    if (sufCtx->callback(array[i], strlen(array[i]), sufCtx->cbCtx, NULL) != REDISMODULE_OK) {
      return REDISEARCH_ERR;
    }
  }
  return REDISMODULE_OK;
}

static int recursiveAdd(TrieNode *node, SuffixCtx *sufCtx) {
  if (node->payload) {
    size_t rlen;
    suffixData *data = Suffix_GetData(node);
    if (processSuffixData(data, sufCtx) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }
  if (node->numChildren) {
    TrieNode **children = __trieNode_children(node);
    for (int i = 0; i < node->numChildren; ++i) {
      if (recursiveAdd(children[i], sufCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    }
  }
  return REDISMODULE_OK;
}

void Suffix_IterateContains(SuffixCtx *sufCtx) {
  if (sufCtx->type == SUFFIX_TYPE_CONTAINS) {
    // get string from node and children
    TrieNode *node = TrieNode_Get(sufCtx->root, sufCtx->rune, sufCtx->runelen, 0, NULL);
    if (!node) {
      return;
    }
    recursiveAdd(node, sufCtx);
  } else if (sufCtx->type == SUFFIX_TYPE_SUFFIX) {
    // exact match. Get strings from a single node
    TrieNode *node = TrieNode_Get(sufCtx->root, sufCtx->rune, sufCtx->runelen, 1, NULL);
    suffixData *data = Suffix_GetData(node);
    if (data) {
      processSuffixData(data, sufCtx);
    }
  }
}


/***********************************************************************************
*                                    Wildcard                                      *
************************************************************************************/
int Suffix_ChooseToken(const char *str, size_t len, size_t *tokenIdx, size_t *tokenLen) {
  int runner = 0;
  int i = 0;
  int init = 0;
  while (i < len) {
    // save location of token
    if (str[i] != '*') {
      tokenIdx[runner] = i;
      init = 1;
    }
    // skip all characters other than `*`
    while (i < len && str[i] != '*') {
      ++i;
    }
    // save length of token
    if (init) {
      tokenLen[runner] = i - tokenIdx[runner];
      ++runner;
    }
    // skip `*` characters
    while (str[i] == '*') {
      ++i;
    }
  }

  // choose best option
  int score = INT32_MIN;
  int retidx = REDISEARCH_UNINITIALIZED;
  for (int i = 0; i < runner; ++i) {
    if (tokenLen[i] < MIN_SUFFIX) {
      continue;
    }

    // 1. long string are likely to have less results
    // 2. tokens at end of pattern are likely to be more relevant
    int curScore = tokenLen[i] + i;

    // iterating all children is demanding
    if (str[tokenIdx[i] + tokenLen[i]] == '*') {
      curScore -= 5;
    }

    // this branching is heavy
    for (int j = tokenIdx[i]; j < tokenIdx[i] + tokenLen[i]; ++j) {
      if (str[j] == '?') {
        --curScore;
      }
    }

    if (curScore >= score) {
      score = curScore;
      retidx = i;
    }
  }

  return retidx;
}

int Suffix_ChooseToken_rune(const rune *str, size_t len, size_t *tokenIdx, size_t *tokenLen) {
  int runner = 0;
  int i = 0;
  int init = 0;
  while (i < len) {
    // save location of token
    if (str[i] != (rune)'*') {
      tokenIdx[runner] = i;
      init = 1;
    }
    // skip all characters other than `*`
    while (i < len && str[i] != (rune)'*') {
      ++i;
    }
    // save length of token
    if (init) {
      tokenLen[runner] = i - tokenIdx[runner];
      ++runner;
    }
    // skip `*` characters
    while (str[i] == (rune)'*') {
      ++i;
    }
  }

  // choose best option
  int score = INT32_MIN;
  int retidx = REDISEARCH_UNINITIALIZED;
  for (int i = 0; i < runner; ++i) {
    if (tokenLen[i] < MIN_SUFFIX) {
      continue;
    }

    // 1. long string are likely to have less results
    // 2. tokens at end of pattern are likely to be more relevant
    int curScore = tokenLen[i] + 1;

    // iterating all children is demanding
    if (str[tokenIdx[i] + tokenLen[i]] == (rune)'*') {
      curScore -= 5;
    }

    // this branching is heavy
    for (int j = tokenIdx[i]; j < tokenIdx[i] + tokenLen[i]; ++j) {
      if (str[j] == (rune)'?') {
        --score;
      }
    }

    if (curScore >= score) {
      score = curScore;
      retidx = i;
    }
  }

  return retidx;
}

int Suffix_CB_Wildcard(const rune *rune, size_t len, void *p, void *payload) {
  SuffixCtx *sufCtx = p;
  TriePayload *pl = payload;
  if (!pl) {
    return REDISMODULE_OK;
  }

  suffixData *data = (suffixData *)pl->data;
  arrayof(char *) array = data->array;
  for (int i = 0; i < array_len(array); ++i) {
    if (Wildcard_MatchChar(sufCtx->cstr, sufCtx->cstrlen, array[i], strlen(array[i]))
            == FULL_MATCH) {
      if (sufCtx->callback(array[i], strlen(array[i]), sufCtx->cbCtx, NULL) != REDISMODULE_OK) {
        return REDISEARCH_ERR;
      }
    }
  }
  return REDISMODULE_OK;
}

int Suffix_IterateWildcard(SuffixCtx *sufCtx) {
  size_t idx[sufCtx->cstrlen];
  size_t lens[sufCtx->cstrlen];
  int useIdx = Suffix_ChooseToken_rune(sufCtx->rune, sufCtx->runelen, idx, lens);

  if (useIdx == REDISEARCH_UNINITIALIZED) {
    return 0;
  }

  rune *token = sufCtx->rune + idx[useIdx];
  size_t toklen = lens[useIdx];
  if (token[toklen] == (rune)'*') {
    toklen++;
  }
  token[toklen] = (rune)'\0';

  TrieNode_IterateWildcard(sufCtx->root, token, toklen, Suffix_CB_Wildcard, sufCtx, sufCtx->timeout);
  return 1;
}

void suffixTrie_freeCallback(void *payload) {
  suffixData *data = payload;
  array_free(data->array);
  data->array = NULL;
  rm_free(data->term);
  data->term = NULL;
}


/***********************************************************/
/*****************        TrieMap       ********************/
/***********************************************************/


size_t addSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
  size_t sz = 0;
  suffixData *data = TrieMap_Find(trie, (char *)str, len);

  // if we found a node and term exists, we already have the term in the suffix
  if (data != TRIEMAP_NOTFOUND && data->term) {
    return 0;
  }

  char *copyStr = rm_strndup(str, len);
  if (data == TRIEMAP_NOTFOUND) {    // node doesn't exist even as suffix of another term
    data = rm_calloc(1, sizeof(*data));
    data->term = copyStr;
    data->array = array_ensure_append_1(data->array, copyStr);
    sz = sizeof(*data);
    size_t initialTrieMapMemSize = trie->memsize;
    TrieMap_Add(trie, copyStr, len, data, NULL);
    size_t newTrieMapMemSize = trie->memsize;
    sz += newTrieMapMemSize - initialTrieMapMemSize;
  } else {    // node exists as suffix for other term
    RS_LOG_ASSERT(!data->term, "can't reach here");
    data->term = copyStr;
    data->array = array_ensure_append_1(data->array, copyStr);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    data = TrieMap_Find(trie, copyStr + j, len - j);

    if (data == TRIEMAP_NOTFOUND) {
      data = rm_calloc(1, sizeof(*data));
      data->array = array_ensure_append_1(data->array, copyStr);
      sz = sizeof(*data);
      size_t initialTrieMapMemSize = trie->memsize;
      TrieMap_Add(trie, copyStr + j, len - j, data, NULL);
      size_t newTrieMapMemSize = trie->memsize;
      sz += newTrieMapMemSize - initialTrieMapMemSize;
    } else {
      data->array = array_ensure_append_1(data->array, copyStr);
    }
  }
  return sz;
}

void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
  char *oldTerm = NULL;

  // iterate all matching terms and remove word
  for (int j = 0; j < len - MIN_SUFFIX + 1; ++j) {
    suffixData *data = TrieMap_Find(trie, str + j, len - j);
    // suffix trie is shared between all tag fields in index, even if they don't use it.
    // if the trie is owned by other fields and not any one containing this suffix,
    // then failure to find the suffix is not an error. just move along.
    if (data == TRIEMAP_NOTFOUND) continue;
    // RS_LOG_ASSERT(data != TRIEMAP_NOTFOUND, "all suffixes must exist");
    if (j == 0) {
      // keep pointer to word string to free after it was found in al sub tokens.
      oldTerm = data->term;
      data->term = NULL;
    }
    // remove from array
    removeSuffix(str, len, data->array);
    // if array is empty, remove the node
    if (array_len(data->array) == 0) {
      RS_LOG_ASSERT(!data->term, "array should contain a pointer to the string");
      TrieMap_Delete(trie, str + j, len - j, (freeCB)freeSuffixNode);
    }
  }
  rm_free(oldTerm);
}

arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, const char *str, uint32_t len,
                                          bool prefix, struct timespec timeout) {
  arrayof(char**) arr = NULL;
  suffixData *data = NULL;
  if (!prefix) {
    data = TrieMap_Find(trie, str, len);
    if (data == TRIEMAP_NOTFOUND) {
      return NULL;
    } else {
      arr = array_ensure_append_1(arr, data->array);
      return arr;
    }
  } else {
    TrieMapIterator *it = TrieMap_Iterate(trie, str, len);
    TrieMapIterator_SetTimeout(it, timeout);
    if (!it) {
      return NULL;
    }
    // an upper limit on the number of expansions is enforced to avoid stuff like "*"
    char *s;
    tm_len_t sl;
    //void *ptr;

    // Find all completions of the prefix
    while (TrieMapIterator_Next(it, &s, &sl, (void**)&data)) {
      arr = array_ensure_append_1(arr, data->array);
    }
    TrieMapIterator_Free(it);
    return arr;
  }
}

// TODO:
/* This function iterates the suffix trie, find matches to a `token` and returns an
 * array with terms matching the pattern.
 * The 'token' address is 'pattern + tokenidx' with length of tokenlen. */
static arrayof(char*) _getWildcardArray(TrieMapIterator *it, const char *pattern, uint32_t plen, long long maxPrefixExpansions) {
  char *s;
  tm_len_t sl;
  suffixData *nodeData;;
  arrayof(char*) resArray = NULL;

  while (TrieMapIterator_NextWildcard(it, &s, &sl, (void **)&nodeData)) {
    for (int i = 0; i < array_len(nodeData->array); ++i) {
      if (array_len(resArray) > maxPrefixExpansions) {
        goto end;
      }
      if (Wildcard_MatchChar(pattern, plen, nodeData->array[i], strlen(nodeData->array[i])) == FULL_MATCH) {
        resArray = array_ensure_append_1(resArray, nodeData->array[i]);
      }
    }
  }

end:
  TrieMapIterator_Free(it);

  return resArray;
}

arrayof(char*) GetList_SuffixTrieMap_Wildcard(TrieMap *trie, const char *pattern, uint32_t len,
                                              struct timespec timeout, long long maxPrefixExpansions) {
  size_t idx[len];
  size_t lens[len];
  // find best token
  int useIdx = Suffix_ChooseToken(pattern, len, idx, lens);
  if (useIdx == REDISEARCH_UNINITIALIZED) {
    return BAD_POINTER;
  }

  size_t tokenidx = idx[useIdx];
  size_t tokenlen = lens[useIdx];
  // if token end with '*', we iterate all its children
  int prefix = pattern[tokenidx + tokenlen] == '*';

  TrieMapIterator *it = TrieMap_Iterate(trie, pattern + tokenidx, tokenlen + prefix);
  if (!it) return NULL;
  TrieMapIterator_SetTimeout(it, timeout);
  it->mode = prefix ? TM_WILDCARD_MODE : TM_WILDCARD_FIXED_LEN_MODE;

  arrayof(char*) arr = _getWildcardArray(it, pattern, len, maxPrefixExpansions);

  // token does not have hits
  if (array_len(arr) == 0) {
    array_free(arr);
    return NULL;
  }

  return arr;
}

void suffixTrieMap_freeCallback(void *payload) {
  suffixTrie_freeCallback(payload);
  rm_free(payload);  
}
