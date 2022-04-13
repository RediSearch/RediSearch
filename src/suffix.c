#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include <string.h>

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);

typedef struct suffixData {
  //int wordExists; // exact match to string exists already
  rune *rune;
  char *term;             // string is used in the array of all suffix tokens
  arrayof(char *) array;   // list of words containing the string. weak pointers
} suffixData;

void delCb(void *val) {
  suffixData *node = val;
  array_free(node->array);
  if (node->term) {
    rm_free(node->term);
  }
  rm_free(node);
}

static suffixData *createSuffixNode(char *term, int keepPtr) {
  suffixData *node = rm_calloc(1, sizeof(*node));
  if (keepPtr) {
    node->term = term;
  }
  node->array = array_ensure_append_1(node->array, term);
  return node;
}

static void freeSuffixNode(suffixData *node) {
  array_free(node->array);
  rm_free(node->term);
  rm_free(node);
}

void addSuffixTrie(Trie *trie, const char *str, uint32_t len) {
  // size_t rlen = 0;
  // rune *runes = strToRunesN(str, len, &rlen);
  rune runes[len];
  size_t rlen = strToRunesN(str, len, runes);
  TrieNode *trienode = TrieNode_Get(trie->root, runes, rlen, 1, NULL);
  suffixData *node = NULL;
  if (trienode && trienode->payload) {
    // suffixData *node = TrieNode_GetValue(trie->root, runes, rlen, 1);
    node = trienode->payload->data;
    // if string was added in the past, skip
    if (node && node->term) {
      //rm_free(runes);
      return;
    }
  }

  char *copyStr = rm_strndup(str, len);
  // printf("string %s len %ld rlen %ld\n", str, len, rlen);
  if (!node) {
    node = createSuffixNode(copyStr, 1);
    RSPayload payload = { .data = (char*)node, .len = sizeof(*node) };
    TrieNode_Add(&trie->root, runes, rlen, &payload, 1, ADD_IGNORE);
  } else {
    RS_LOG_ASSERT(!node->term, "can't reach here");
    node->term = copyStr;
    node->array = array_ensure_append_1(node->array, copyStr);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    TrieNode *trienode = TrieNode_Get(trie->root, runes + j, rlen - j, 1, NULL);
    
    node = trienode ? trienode->payload->data : NULL;
    void *payload = trienode ? trienode->payload : NULL;
    size_t len;
    // printf("%s %p %p %p\n", runesToStr(runes + j, rlen - j, &len), trienode, payload, node);
    if (!trienode || !trienode->payload) {
      node = createSuffixNode(copyStr, 0);
      RSPayload payload = { .data = (char*)node, .len = sizeof(*node) };
      TrieNode_Add(&trie->root, runes + j, rlen - j, &payload, 1, ADD_IGNORE);
    } else {
      node->array = array_ensure_append_1(node->array, copyStr);
    }
  }
  //rm_free(runes);
}

static void removeSuffix(const char *str, size_t rlen, arrayof(char*) array) {
  for (int i = 0; i < array_len(array); ++i) {
    if (!strncmp(array[i], str, rlen)) {
      array_del_fast(array, i);
      return;
    }
  }
}

void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len) {
  //size_t rlen = 0;
  //rune *runes = strToRunesN(str, len, &rlen);
  rune runes[len];
  size_t rlen = strToRunesN(str, len, &runes);
  rune *oldRune = NULL;

  // iterate all matching terms and remove word
  for (int j = 0; j < len; ++j) {
    suffixData *node = TrieNode_GetValue(trie->root, runes + j, rlen - j, 1);
    // suffixData *node = TrieMap_Find(trie, str + j, len - j);
    if (j == 0) {
      // keep pointer to word string to free after it was found in al sub tokens.
      oldRune = node->term;
      node->term = NULL;
    }
    // remove from array
    removeSuffix(str, len, node->array);
    // if array is empty, remove the node
    if (array_len(node->array) == 0) {
      RS_LOG_ASSERT(!node->term, "array should contain a pointer to the string");
      TrieNode_Delete(trie->root, runes + j, rlen - j);
      freeSuffixNode(node);
    }
  }
  rm_free(oldRune);
  rm_free(runes);
}

static int processSuffixData(suffixData *data, TrieSuffixCallback callback, void *ctx) {
  if (!data) {
    return REDISMODULE_OK;
  }
  printf("term: %s ", data->term);
  arrayof(char *) array = data->array;
  for (int i = 0; i < array_len(array); ++i) {
    printf("%d %s ",i, array[i]);
    if (callback(array[i], strlen(array[i]), ctx) != REDISMODULE_OK) {
      printf("\n");
      return REDISEARCH_ERR;
    }
  }
  printf("\n");
  return REDISMODULE_OK;
}

static int recursiveAdd(TrieNode *node, TrieSuffixCallback callback, void *ctx) {
  if (node->payload) {
    size_t rlen;
    printf("nodestr %s len %ld rlen %ld", runesToStr(node->str, node->len, &rlen), node->len, rlen);
    suffixData *data = node->payload->data;
    processSuffixData(data, callback, ctx);
  }
  if (node->numChildren) {
    TrieNode **children = __trieNode_children(node);
    for (int i = 0; i < node->numChildren; ++i) {
      printf("child %d ", i);
      if (recursiveAdd(children[i], callback, ctx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    }
  }
  return REDISMODULE_OK;
}

void Suffix_IterateContains(TrieNode *n, const rune *str, size_t nstr, bool prefix,
                            TrieSuffixCallback callback, void *ctx) {
  if (prefix) {
    TrieNode *node = TrieNode_Get(n, str, nstr, 0, NULL);
    if (!node) {
      return;
    }
    recursiveAdd(node, callback, ctx);
  } else {
    suffixData *data = TrieNode_GetValue(n, str, nstr, 1);
    if (data) {
      processSuffixData(data, callback, ctx);
    }
  }                              
}

arrayof(char **) findSuffixContains(Trie *trie, const char *str, uint32_t len) {

  return NULL;
}

/***********************************************************************

void addSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
  suffixData *node = TrieMap_Find(trie, (char *)str, len);

  // if string was added in the past, skip
  if (node != TRIEMAP_NOTFOUND && node->term) {
    return;
  }

  char *strCopy = rm_strndup(str, len);
  if (node == TRIEMAP_NOTFOUND) {
    node = createSuffixNode(strCopy, 1);
    TrieMap_Add(trie, str, len, node, NULL);
  } else {
    //node->wordExists = 1;
    RS_LOG_ASSERT(!node->term, "can't reach here");
    node->term = strCopy;
    node->array = array_ensure_append_1(node->array, strCopy);
    //TrieMap_Add(trie, str, len, strCopy, addTermCb);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    node = TrieMap_Find(trie, str + j, len - j);
    if (node == TRIEMAP_NOTFOUND) {
      node = createSuffixNode(strCopy, 0);
      TrieMap_Add(trie, str + j, len - j, node, NULL);
    } else {
      node->array = array_ensure_append_1(node->array, strCopy);
      //TrieMap_Add(trie, str + j, len - j, strCopy, addSuffixCb);
    }
  }
}

void deleteSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
  // iterate all matching terms and remove word
  char *term = NULL;
  for (int j = 0; j < len; ++j) {
    suffixData *node = TrieMap_Find(trie, str + j, len - j);
    RS_LOG_ASSERT(node != TRIEMAP_NOTFOUND, "node should exist");
    if (j == 0) {
      // keep pointer to word string to free after it was found in al sub tokens.
      term = node->term;
      node->term = NULL;
      if (array_len(node->array) == 0) {
        
        TrieMap_Delete(trie, str, len, delCb);
      }
      continue;
    }
    for (int k = 0; k < array_len(node->array); ++k) {
      if (!strncmp(str, node->array[k], len)) {
        // found 
        array_del_fast(node->array, k);
        if (!node->term && array_len(node->array) == 0) {
          TrieMap_Delete(trie, str + j, len - j, delCb);
        }
        break;
      }
    }
  }
  rm_free(term);
}


void deleteSuffix(IndexSpec *spec, char *str, uint32_t len) {
  if (!(spec->flags & Index_HasSuffix)) {
    return;
  }

  for (int i = 0; i < spec->numFields; ++i) {
    FieldSpec *field = spec->fields + i;
    if (!(field->options & FieldSpec_Suffix)) {
      continue;
    }
    deleteSuffixTrie(field->suffixTrie, str, len);
  }
} * /

TrieMap *SuffixTrieMapCreate() {
  return NewTrieMap();
}

void SuffixTrieFreeMap(TrieMap *suffix) {
  TrieMap_Free(suffix, delCb);
}

arrayof(char *) findSuffix(TrieMap *suffix, const char *str, uint32_t len) {
  TrieMapNode *n = TrieMap_Find(suffix, str, len);
  if (n == TRIEMAP_NOTFOUND) {
    return NULL;
  }
  suffixData *node = n->value;
  return node->array;
}

static arrayof(char **) getArrayFromChild(TrieMapNode *n, arrayof(char **) array) {
  if (n->numChildren == 0) {
    return;
  }
  TrieMapNode **children = __trieMapNode_children(n);
  for (int i = 0; i < n->numChildren; ++i) {
    TrieMapNode *child = children[i];
    suffixData *node = child->value;
    array = array_ensure_append_1(array, node->array);
    array = getArrayFromChild(child, array);
  }
  return array;
}

arrayof(char **) findSuffixContains(TrieMap *suffix, const char *str, uint32_t len) {
  TrieMapNode *n = TrieMap_Find(suffix, str, len);
  if (n == TRIEMAP_NOTFOUND) {
    return NULL;
  }

  arrayof(char **) ret = array_new(char **, RSGlobalConfig.maxPrefixExpansions);
  suffixData *node = n->value;
  ret = array_ensure_append_1(ret, node->array);
  return getArrayFromChild(n, ret);
}
*/