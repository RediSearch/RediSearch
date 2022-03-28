#include "suffix.h"
#include "rmutil/rm_assert.h"

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);

typedef struct suffixNode {
  //int wordExists; // exact match to string exists already
  char *term;             // string is used in the array of all suffix tokens
  arrayof(char *)array;   // list of words containing the string
} suffixNode;

//TODO: remove if not used
void *addSuffixCb(void *oldval, void *newval) {
  suffixNode *node = oldval;
  char *str = newval;
  node->array = array_append(node->array, str);
  return node;
}

//TODO: remove if not used
void *addTermCb(void *oldval, void *newval) {
  suffixNode *node = oldval;
  node->term = newval;
  return node;
}

void delCb(void *val) {
  suffixNode *node = val;
  array_free(node->array);
  if (node->term) {
    rm_free(node->term);
  }
  rm_free(node);
}

static suffixNode *createSuffixNode(char *str, int term) {
  suffixNode *node = rm_calloc(1, sizeof(*node));
  //node->wordExists = exists;
  node->array = array_new(char *, 1);
  if (term) {
    node->term = str;
  } else {
    array_append(node->array, str);
  }
  return node;
}

void writeSuffixTrie(TrieMap *trie, const char *str, uint32_t len) {
  suffixNode *node = TrieMap_Find(trie, (char *)str, len);

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
    node->term = strCopy;
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

void deleteSuffixTrie(TrieMap *trie, const char *str, uint32_t len) {
  // iterate all matching terms and remove word
  char *term = NULL;
  for (int j = 0; j < len; ++j) {
    suffixNode *node = TrieMap_Find(trie, str + j, len - j);
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

TrieMap *SuffixTrieCreate() {
  return NewTrieMap();
}

void SuffixTrieFree(TrieMap *suffix) {
  TrieMap_Free(suffix, delCb);
}

arrayof(char *) findSuffix(TrieMap *suffix, const char *str, uint32_t len) {
  TrieMapNode *n = TrieMap_Find(suffix, str, len);
  if (n == TRIEMAP_NOTFOUND) {
    return NULL;
  }
  suffixNode *node = n->value;
  return node->array;
}
