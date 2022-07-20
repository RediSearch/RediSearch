#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include "wildcard/wildcard.h"
#include "config.h"

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
    RS_LOG_ASSERT(data, "all suffixes must exist");
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
    if (sufCtx->callback(array[i], strlen(array[i]), sufCtx->cbCtx) != REDISMODULE_OK) {
      return REDISEARCH_ERR;
    }
  }
  return REDISMODULE_OK;
}

static int recursiveAdd(TrieNode *node, SuffixCtx *sufCtx) {
//TrieSuffixCallback callback, void *ctx) {
  if (node->payload) {
    size_t rlen;
    // printf("nodestr %s len %d rlen %ld", runesToStr(node->str, node->len, &rlen), node->len, rlen);
    suffixData *data = Suffix_GetData(node);
    if (processSuffixData(data, sufCtx) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }
  if (node->numChildren) {
    TrieNode **children = __trieNode_children(node);
    for (int i = 0; i < node->numChildren; ++i) {
      // printf("child %d ", i);
      if (recursiveAdd(children[i], sufCtx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    }
  }
  return REDISMODULE_OK;
}

void Suffix_IterateContains(SuffixCtx *sufCtx) {
  //TrieNode *n, const rune *str, size_t nstr, bool prefix,
  //                          TrieSuffixCallback callback, void *ctx) {
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
  } else { // SUFFIX_TYPE_WILDCARD

  }
}

void Suffix_CB_Wildcard(SuffixCtx *sufCtx, ) {
  char *s;
  tm_len_t sl;
  suffixData *nodeData;;
  arrayof(char*) resArray = NULL;

  while (TrieMapIterator_NextWildcard(it, &s, &sl, (void **)&nodeData)) {
    for (int i = 0; i < array_len(nodeData->array); ++i) {
      if (array_len(resArray) > RSGlobalConfig.maxPrefixExpansions) {
        goto end;
      }
      if (Wildcard_MatchChar(str, slen, nodeData->array[i], strlen(nodeData->array[i])) == FULL_MATCH) {
        resArray = array_ensure_append_1(resArray, nodeData->array[i]);
      }
    }
  }
}

void Suffix_IterateWildcard(SuffixCtx *sufCtx) {
  size_t idx[sufCtx->cstrlen];
  size_t lens[sufCtx->cstrlen];
  //int useIdx = Wildcard_StarBreak(sufCtx->cstr, sufCtx->cstrlen, idx, lens);
  int useIdx = Wildcard_StarBreak_rune(sufCtx->rune, sufCtx->runelen, idx, lens);

  rune *token = sufCtx->rune + idx[useIdx];
  size_t toklen = lens[useIdx];
  if (token[toklen] == (rune)'*') {
    toklen++;
  }
  token[toklen] = (rune)'\0';

  TrieNode_IterateWildcard(sufCtx->root, token, toklen, Suffix_CB_Wildcard, sufCtx, sufCtx->timeout);
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


void addSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
  suffixData *data = TrieMap_Find(trie, (char *)str, len);

  // if we found a node and term exists, we already have the term in the suffix
  if (data != TRIEMAP_NOTFOUND && data->term) {
    return;
  }

  char *copyStr = rm_strndup(str, len);
  if (data == TRIEMAP_NOTFOUND) {    // node doesn't exist even as suffix of another term
    data = rm_calloc(1, sizeof(*data));
    data->term = copyStr;
    data->array = array_ensure_append_1(data->array, copyStr);
    TrieMap_Add(trie, copyStr, len, data, NULL);
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
      TrieMap_Add(trie, copyStr + j, len - j, data, NULL);
    } else {
      data->array = array_ensure_append_1(data->array, copyStr);
    }
  }
}

void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
  char *oldTerm = NULL;

  // iterate all matching terms and remove word
  for (int j = 0; j < len - MIN_SUFFIX + 1; ++j) {
    suffixData *data = TrieMap_Find(trie, str + j, len - j);
    RS_LOG_ASSERT(data != TRIEMAP_NOTFOUND, "all suffixes must exist");
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

static arrayof(char*) _checkCountWildcard(TrieMap *trie, const char *str, uint32_t slen, size_t idx, size_t idxlen, struct timespec timeout) {
  int prefix = str[idx + idxlen] == '*';
  TrieMapIterator *it = TrieMap_Iterate(trie, &str[idx], idxlen + prefix);
  if (!it) return NULL;
  TrieMapIterator_SetTimeout(it, timeout);
  // If there is no '*`, the length is known which can be used for optimization
  it->mode = prefix ? TM_WILDCARD_MODE : TM_WILDCARD_FIXED_LEN_MODE;

  char *s;
  tm_len_t sl;
  suffixData *nodeData;;
  arrayof(char*) resArray = NULL;

  while (TrieMapIterator_NextWildcard(it, &s, &sl, (void **)&nodeData)) {
    for (int i = 0; i < array_len(nodeData->array); ++i) {
      if (array_len(resArray) > RSGlobalConfig.maxPrefixExpansions) {
        goto end;
      }
      if (Wildcard_MatchChar(str, slen, nodeData->array[i], strlen(nodeData->array[i])) == FULL_MATCH) {
        resArray = array_ensure_append_1(resArray, nodeData->array[i]);
      }
    }
  }

end:
  TrieMapIterator_Free(it);

  return resArray;
}


arrayof(char*) GetList_SuffixTrieMap_Wildcard(TrieMap *trie, const char *str, uint32_t len,
                                              struct timespec timeout) {
  size_t idx[len];
  size_t lens[len];
  int useIdx = Wildcard_StarBreak(str, len, idx, lens);

  if (useIdx == -1) {
    return NULL;
  }

  arrayof(char*) arr = _checkCountWildcard(trie, str, len, idx[useIdx], lens[useIdx], timeout);

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
