#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "config.h"

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);

typedef struct suffixData {
  //int wordExists; // exact match to string exists already
  rune *rune;
  char *term;             // string is used in the array of all suffix tokens
  arrayof(rune *) array;   // list of words containing the string. weak pointers
} suffixData;

void delCb(void *val) {
  suffixData *node = val;
  array_free(node->array);
  if (node->term) {
    rm_free(node->term);
  }
  rm_free(node);
}

static suffixData *createSuffixNode(rune *rune, int term) {
  suffixData *node = rm_calloc(1, sizeof(*node));
  if (term) {
    node->rune = rune;
  }
  array_ensure_append_1(node->array, rune);
  return node;
}

static void freeSuffixNode(suffixData *node) {
  array_free(node->array);
  rm_free(node->rune);
  rm_free(node);
}

void writeSuffixTrie(Trie *trie, const char *str, uint32_t len) {
  size_t rlen = 0;
  rune *runes = strToRunes(str, &rlen);
  suffixData *node = TrieNode_GetPayload(trie->root, runes, rlen);

  // if string was added in the past, skip
  if (node && node->rune) {
    rm_free(runes);
    return;
  }

  if (!node) {
    node = createSuffixNode(runes, 1);
    TrieNode_Add(&trie->root, runes, rlen, (RSPayload*)node, 0, ADD_IGNORE);
  } else {
    RS_LOG_ASSERT(!node->rune, "can't reach here");
    node->rune = runes;
    node->array = array_ensure_append_1(node->array, runes);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    node = TrieNode_GetPayload(trie->root, runes + j, rlen - j);
    if (!node) {
      node = createSuffixNode(runes, 0);
      TrieNode_Add(&trie->root, runes, rlen, (RSPayload*)node, 0, ADD_IGNORE);
    } else {
      node->array = array_ensure_append_1(node->array, runes);
    }
  }
}

static void removeSuffix(rune *str, size_t rlen, arrayof(rune*) array) {
  for (int i = 0; i < array_len(array); ++i) {
    if (!runesncmp(array[i], str, rlen)) {
      array_del_fast(array, i);
      break;
    }
  }
}

void deleteSuffixTrie(Trie *trie, const char *str, uint32_t len) {
  size_t rlen = 0;
  rune *runes = strToRunes(str, &rlen);

  // iterate all matching terms and remove word
  rune *oldRune = NULL;
  for (int j = 0; j < len; ++j) {
    suffixData *node = TrieNode_GetPayload(trie->root, runes + j, rlen - j);
    // suffixData *node = TrieMap_Find(trie, str + j, len - j);
    RS_LOG_ASSERT(node, "node should exist");
    if (j == 0) {
      // keep pointer to word string to free after it was found in al sub tokens.
      oldRune = node->rune;
      node->rune = NULL;
    }
    // remove from array
    removeSuffix(runes, rlen, node->array);
    // if array is empty, remove the node
    if (array_len(node->array) == 0) {
      RS_LOG_ASSERT(!node->rune, "array should contain a pointer to the string");
      TrieNode_Delete(trie->root, runes + j, rlen - j);
      freeSuffixNode(node);
    }
  }
  rm_free(oldRune);
  rm_free(runes);
}

arrayof(rune **) findSuffixContains(Trie *trie, const char *str, uint32_t len) {

}

/****************************************************************************

void writeSuffixTrieMap(TrieMap *trie, const char *str, uint32_t len) {
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