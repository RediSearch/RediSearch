/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include "util/strconv.h"
#include "wildcard.h"

#include <string.h>
#include <strings.h>

/***********************************************************/
/*****************      RuneTrieMap     ********************/
/***********************************************************/

static suffixData *findRuneSuffixData(RuneTrieMap *trie, const rune *runes, size_t rlen) {
  void *p = RuneTrieMap_FindRune(trie, runes, rlen);
  if (p == RUNETRIEMAP_NOTFOUND) {
    return NULL;
  }
  return (suffixData *)p;
}

void addSuffixTrie(RuneTrieMap *trie, const char *str, uint32_t len) {
  size_t rlen = 0;
  runeBuf buf;
  rune *runes = runeBufFill(str, len, &buf, &rlen);

  // if string was added in the past, skip
  suffixData *existing = findRuneSuffixData(trie, runes, rlen);
  if (existing && existing->term) {
    runeBufFree(&buf);
    return;
  }

  char *copyStr = rm_strndup(str, len);
  if (!existing) {
    suffixData *data = rm_calloc(1, sizeof(*data));
    data->term = copyStr;
    data->array = array_ensure_append_1(data->array, copyStr);
    RuneTrieMap_InsertRune(trie, runes, rlen, data, NULL);
  } else {
    RS_LOG_ASSERT(!existing->term, "can't reach here");
    existing->term = copyStr;
    existing->array = array_ensure_append_1(existing->array, copyStr);
  }

  // Save string copy to all suffixes of it
  // If it exists, move to the next field
  for (int j = 1; j < len - MIN_SUFFIX + 1; ++j) {
    suffixData *data = findRuneSuffixData(trie, runes + j, rlen - j);
    if (!data) {
      data = rm_calloc(1, sizeof(*data));
      data->array = array_ensure_append_1(data->array, copyStr);
      RuneTrieMap_InsertRune(trie, runes + j, rlen - j, data, NULL);
    } else {
      data->array = array_ensure_append_1(data->array, copyStr);
    }
  }
  runeBufFree(&buf);
}

static void removeSuffix(const char *str, size_t rlen, arrayof(char*) array) {
  for (int i = 0; i < array_len(array); ++i) {
    if (STR_EQ(str, rlen, array[i])) {
      array = array_del_fast(array, i);
      return;
    }
  }
}

void deleteSuffixTrie(RuneTrieMap *trie, const char *str, uint32_t len) {
  size_t rlen = 0;
  runeBuf buf;
  rune *runes = runeBufFill(str, len, &buf, &rlen);
  char *oldTerm = NULL;

  // iterate all matching terms and remove word
  for (int j = 0; j < len - MIN_SUFFIX + 1; ++j) {
    suffixData *data = findRuneSuffixData(trie, runes + j, rlen - j);
    // suffix trie is shared between all text fields in index, even if they don't use it.
    // if the trie is owned by other fields and not any one containing this suffix,
    // then failure to find the suffix is not an error. just move along.
    if (!data) continue;
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
      RuneTrieMap_DeleteRune(trie, runes + j, rlen - j, suffixTrie_freeCallback);
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

static int suffix_prefixed_cb(const rune *runes, size_t len, void *payload, void *ctx) {
  // RuneTrieMap iteration stops on non-zero return; processSuffixData
  // returns REDISEARCH_ERR (=1) to stop and REDISMODULE_OK (=0) to continue,
  // so the contracts line up exactly.
  return processSuffixData((suffixData *)payload, (SuffixCtx *)ctx);
}

void Suffix_IterateContains(SuffixCtx *sufCtx) {
  if (sufCtx->type == SUFFIX_TYPE_CONTAINS) {
    // visit every key with the requested rune prefix
    RuneTrieMap_IteratePrefixedRune(sufCtx->trie, sufCtx->rune, sufCtx->runelen,
                                    suffix_prefixed_cb, sufCtx);
  } else if (sufCtx->type == SUFFIX_TYPE_SUFFIX) {
    // exact match. Get strings from a single node
    void *p = RuneTrieMap_FindRune(sufCtx->trie, sufCtx->rune, sufCtx->runelen);
    if (p != RUNETRIEMAP_NOTFOUND) {
      processSuffixData((suffixData *)p, sufCtx);
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

int Suffix_CB_Wildcard(const rune *runes, size_t len, void *payload, void *ctx) {
  SuffixCtx *sufCtx = ctx;
  if (!payload) {
    return REDISMODULE_OK;
  }

  suffixData *data = (suffixData *)payload;
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

  RuneTrieMap_IterateWildcardRune(sufCtx->trie, token, toklen, Suffix_CB_Wildcard, sufCtx,
                                  sufCtx->timeout, sufCtx->skipTimeoutChecks);
  return 1;
}

void suffixTrie_freeCallback(void *payload) {
  suffixData *data = payload;
  array_free(data->array);
  rm_free(data->term);
  rm_free(data);
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
      // keep pointer to word string to free after it was found in all sub tokens.
      oldTerm = data->term;
      data->term = NULL;
    }
    // remove from array
    removeSuffix(str, len, data->array);
    // if array is empty, remove the node
    if (array_len(data->array) == 0) {
      RS_LOG_ASSERT(!data->term, "array should contain a pointer to the string");
      TrieMap_Delete(trie, str + j, len - j, suffixTrie_freeCallback);
    }
  }
  rm_free(oldTerm);
}

arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, const char *str, uint32_t len,
                                          bool prefix, struct timespec timeout, bool skipTimeoutChecks) {
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
    TrieMapIterator *it = TrieMap_IterateWithFilter(trie, str, len, TM_PREFIX_MODE);
    if (!skipTimeoutChecks) {
      TrieMapIterator_SetTimeout(it, timeout);
    }
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

  while (TrieMapIterator_Next(it, &s, &sl, (void **)&nodeData)) {
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
                                              struct timespec timeout, long long maxPrefixExpansions, bool skipTimeoutChecks) {
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

  TrieMapIterator *it = TrieMap_IterateWithFilter(trie, pattern + tokenidx, tokenlen + prefix, TM_WILDCARD_MODE);
  if (!it) return NULL;
  if (!skipTimeoutChecks) {
    TrieMapIterator_SetTimeout(it, timeout);
  }

  arrayof(char*) arr = _getWildcardArray(it, pattern, len, maxPrefixExpansions);

  // token does not have hits
  if (array_len(arr) == 0) {
    array_free(arr);
    return NULL;
  }

  return arr;
}

