#include "suffix.h"
#include "rmutil/rm_assert.h"

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);

typedef struct suffixNodeInfo {
  int wordExists; // exact match to string exists already
  char **array;   // list of words containing the string
} suffixNodeInfo;

void *addCb(void *oldval, void *newval) {
  suffixNodeInfo *node = oldval;
  char *str = newval;
  node->array = array_append(node->array, str);
  return node;
}

void delCb(void *val) {
  suffixNodeInfo *node = val;
  array_free_ex(node->array, { rm_free(ptr); });
  rm_free(node);
}

static suffixNodeInfo *createSuffixNode(char *str, int exists) {
  suffixNodeInfo *node = rm_malloc(sizeof(*node));
  node->wordExists = exists;
  node->array = array_new(char *, 1);
  array_append(node->array, str);
  return node;
}

void writeSuffixTrie(TrieMap *trie, const char *str, uint32_t len) {
  suffixNodeInfo *node = TrieMap_Find(trie, (char *)str, len);
  RS_LOG_ASSERT(node, "Node should not be null")

  // if string was added in the past, skip
  if (node != TRIEMAP_NOTFOUND && node->wordExists) {
    return;
  }

  char *strCopy = rm_strndup(str, len);
  if (node == TRIEMAP_NOTFOUND) {
    node = createSuffixNode(strCopy, 1);
    TrieMap_Add(trie, str, len, node, NULL);
  } else {
    node->wordExists = 1;
    TrieMap_Add(trie, str, len, strCopy, addCb);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    node = TrieMap_Find(trie, str, len);
    if (node == TRIEMAP_NOTFOUND) {
      node = createSuffixNode(strCopy, 0);
      TrieMap_Add(trie, str, len, node, NULL);
    } else {
      TrieMap_Add(trie, str, len, strCopy, addCb);
    }
  }
}

void deleteSuffixTrie(TrieMap *trie, const char *str, uint32_t len) {
  // iterate all matching terms and remove word
  char *arrayStr = NULL;
  for (int j = 0; j < len; ++j) {
    suffixNodeInfo *node = TrieMap_Find(trie, str, len);
    RS_LOG_ASSERT(node, "node should exist");
    for (int k = 0; k < array_len(node->array); ++k) {
      if (!strncmp(str + j, node->array[k], len -j)) {
        arrayStr = node->array[k];
        array_del_fast(node->array, k);
        if (array_len(node->array) == 0) {
          TrieMap_Delete(trie, str, len, NULL);
        }
        break;
      }
    }
  }
  if (arrayStr) rm_free(arrayStr);
}

/*
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
} */

void SuffixTrieFree(TrieMap *suffix) {
  TrieMap_Free(suffix, delCb);
}