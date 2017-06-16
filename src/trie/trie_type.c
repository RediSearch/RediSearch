#include "../dep/libnu/libnu.h"
#include "../rmutil/strings.h"
#include "../rmutil/util.h"
#include "../util/heap.h"
#include "rune_util.h"

#include "trie_type.h"
#include "../commands.h"
#include <math.h>
#include <sys/param.h>
#include <time.h>
#include <string.h>

void split(char *src, const char *separator, char **dest, int *num) {
  char *pNext;
  int count = 0;
  if (src == NULL || strlen(src) == 0) return;
  if (separator == NULL || strlen(separator) == 0) return;
  if (dest == NULL || num == NULL || *num <= 0) return;
  pNext = strtok(src, separator);
  while (pNext != NULL) {
    *dest++ = pNext;
    ++count;
    if (count >= *num) break;
    pNext = strtok(NULL, separator);
  }
  *num = count;
}

Trie *NewTrie() {
  Trie *tree = RedisModule_Alloc(sizeof(Trie));
  rune *rs = strToRunes("", 0);
  rune *info = strToRunes("", 0);
  tree->root = __newTrieNode(rs, 0, 0, info, 0, 0, 0, 0);
  tree->size = 0;
  free(rs);
  free(info);
  return tree;
}

int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr) {
  size_t len;
  char *str = (char *)RedisModule_StringPtrLen(s, &len);
  int ret = Trie_InsertStringBuffer(t, str, len, score, incr);
  return ret;
}

int Trie_InsertStringBuffer(Trie *t, char *s, size_t len, double score, int incr) {
  char *str_info[2];
  memset(str_info, 0, sizeof(char *) * 2);
  int split_num = 2;
  split(s, STR_INFO_SPLIT_SEP, str_info, &split_num);
  if (split_num > 0) {
    size_t str_len = strlen(str_info[0]);
    rune *str = strToRunes(str_info[0], &str_len);
    size_t info_len = 0;
    rune *info = NULL;
    if (split_num > 1) {
      info_len = strlen(str_info[1]);
      info = strToRunes(str_info[1], &info_len);
    }
    if (str_len && str_len < MAX_STRING_LEN && info_len < MAX_STRING_LEN) {
      int rc = TrieNode_Add(&t->root, str, str_len, info, info_len, (float)score,
                            incr ? ADD_INCR : ADD_REPLACE);
      free(str);
      if (info != NULL) free(info);
      t->size += rc;
      return rc;
    } else {
      free(str);
      if (info != NULL) free(info);
    }
  }
  return 0;
}

int Trie_Delete(Trie *t, char *s, size_t len) {

  rune *runes = strToRunes(s, &len);
  int rc = TrieNode_Delete(t->root, runes, len);
  t->size -= rc;
  free(runes);
  return rc;
}

void TrieSearchResult_Free(TrieSearchResult *e) {
  if (e->str) {
    free(e->str);
    e->str = NULL;
  }
  free(e);
}

static int cmpEntries(const void *p1, const void *p2, const void *udata) {
  const TrieSearchResult *e1 = p1, *e2 = p2;

  if (e1->score < e2->score) {
    return 1;
  } else if (e1->score > e2->score) {
    return -1;
  }
  return 0;
}

TrieIterator *Trie_IteratePrefix(Trie *t, char *prefix, size_t len, int maxDist) {
  size_t rlen;
  rune *runes = strToFoldedRunes(prefix, &rlen);
  DFAFilter *fc = malloc(sizeof(*fc));
  *fc = NewDFAFilter(runes, rlen, maxDist, 1);

  TrieIterator *it = TrieNode_Iterate(t->root, FilterFunc, StackPop, fc);
  free(runes);
  return it;
}

Vector *Trie_Search(Trie *tree, char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize) {
  heap_t *pq = malloc(heap_sizeof(num));
  heap_init(pq, cmpEntries, NULL, num);

  size_t rlen;
  rune *runes = strToFoldedRunes(s, &rlen);
  DFAFilter fc = NewDFAFilter(runes, rlen, maxDist, prefixMode);

  TrieIterator *it = TrieNode_Iterate(tree->root, FilterFunc, StackPop, &fc);
  // TrieIterator *it = TrieNode_Iterate(tree->root,NULL, NULL, NULL);
  rune *rstr;
  t_len slen;
  float score;
  rune *rinfo;
  t_len ilen;

  TrieSearchResult *pooledEntry = NULL;
  int dist = maxDist + 1;
  while (TrieIterator_Next(it, &rstr, &slen, &rinfo, &ilen, &score, &dist)) {
    if (pooledEntry == NULL) {
      pooledEntry = malloc(sizeof(TrieSearchResult));
      pooledEntry->str = NULL;
    }
    TrieSearchResult *ent = pooledEntry;

    ent->score = score;
    if (maxDist > 0) {
      // factor the distance into the score
      ent->score *= exp((double)-(2 * dist));
    }
    // in prefix mode we also factor in the total length of the suffix
    if (prefixMode) {
      ent->score /= sqrt(1 + (slen >= len ? slen - len : len - slen));
    }

    if (heap_count(pq) < heap_size(pq)) {
      size_t str_len = 0;
      char *str = runesToStr(rstr, slen, &str_len);
      size_t info_len = 0;
      char *info = rinfo == NULL ? NULL : runesToStr(rinfo, ilen, &info_len);
      ent->len = str_len;
      ent->str = calloc(ent->len + 1, sizeof(char));
      memcpy(ent->str, str, str_len);
      free(str);
      if (info != NULL) {
        if (info_len > 0) {
          ent->len = ent->len + 1 + info_len;
          ent->str = realloc(ent->str, (ent->len + 1) * sizeof(char));
          memcpy(ent->str + str_len, STR_INFO_SPLIT_SEP, 1);
          memcpy(ent->str + str_len + 1, info, info_len);
        }
        free(info);
      }
      heap_offerx(pq, ent);
      pooledEntry = NULL;

      if (heap_count(pq) == heap_size(pq)) {
        TrieSearchResult *qe = heap_peek(pq);
        it->minScore = qe->score;
      }

    } else {
      if (ent->score >= it->minScore) {
        pooledEntry = heap_poll(pq);
        free(pooledEntry->str);
        pooledEntry->str = NULL;
        // ent->str = runesToStr(rstr, slen, &ent->len);
        size_t str_len = 0;
        char *str = runesToStr(rstr, slen, &str_len);
        size_t info_len = 0;
        char *info = rinfo == NULL ? NULL : runesToStr(rinfo, ilen, &info_len);
        ent->len = str_len;
        ent->str = calloc(ent->len + 1, sizeof(char));
        memcpy(ent->str, str, str_len);
        free(str);
        if (info != NULL) {
          if (info_len > 0) {
            ent->len = ent->len + 1 + info_len;
            ent->str = realloc(ent->str, (ent->len + 1) * sizeof(char));
            memcpy(ent->str + str_len, STR_INFO_SPLIT_SEP, 1);
            memcpy(ent->str + str_len + 1, info, info_len);
          }
          free(info);
        }
        heap_offerx(pq, ent);

        // get the new minimal score
        TrieSearchResult *qe = heap_peek(pq);
        if (qe->score > it->minScore) {
          it->minScore = qe->score;
        }

      } else {
        pooledEntry = ent;
      }
    }

    // dist = maxDist + 3;
  }

  if (pooledEntry) {
    TrieSearchResult_Free(pooledEntry);
  }

  // printf("Nodes consumed: %d/%d (%.02f%%)\n", it->nodesConsumed,
  //        it->nodesConsumed + it->nodesSkipped,
  //        100.0 * (float)(it->nodesConsumed) / (float)(it->nodesConsumed +
  //        it->nodesSkipped));

  // put the results from the heap on a vector to return
  size_t n = MIN(heap_count(pq), num);
  Vector *ret = NewVector(TrieSearchResult *, n);
  for (int i = 0; i < n; ++i) {
    TrieSearchResult *h = heap_poll(pq);
    Vector_Put(ret, n - i - 1, h);
  }

  // trim the results to remove irrelevant results
  if (trim) {
    float maxScore = 0;
    int i;
    for (i = 0; i < n; ++i) {
      TrieSearchResult *h;
      Vector_Get(ret, i, &h);

      if (maxScore && h->score < maxScore / SCORE_TRIM_FACTOR) {
        // TODO: Fix trimming the vector
        ret->top = i;
        break;
      }
      maxScore = MAX(maxScore, h->score);
    }

    for (; i < n; ++i) {
      TrieSearchResult *h;
      Vector_Get(ret, i, &h);
      TrieSearchResult_Free(h);
    }
  }

  free(runes);
  TrieIterator_Free(it);
  DFAFilter_Free(&fc);
  heap_free(pq);

  return ret;
}

/***************************************************************
*
*                       Trie type methods
*
***************************************************************/

/* declaration of the type for redis registration. */
RedisModuleType *TrieType;

void *TrieType_GenericLoad(RedisModuleIO *rdb) {

  uint64_t elements = RedisModule_LoadUnsigned(rdb);
  Trie *tree = NewTrie();

  while (elements--) {
    size_t len;
    char *str = RedisModule_LoadStringBuffer(rdb, &len);
    double score = RedisModule_LoadDouble(rdb);
    Trie_InsertStringBuffer(tree, str, len - 1, score, 0);
    RedisModule_Free(str);
  }
  // TrieNode_Print(tree->root, 0, 0);
  return tree;
}

void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver != 0) {
    return NULL;
  }
  return TrieType_GenericLoad(rdb);
}

void TrieType_RdbSave(RedisModuleIO *rdb, void *value) {
  Trie *tree = (Trie *)value;
  RedisModule_SaveUnsigned(rdb, tree->size);
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  RedisModule_Log(ctx, "notice", "Trie: saving %zd nodes.", tree->size);
  int count = 0;
  if (tree->root) {
    TrieIterator *it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
    rune *rstr;
    t_len len;
    float score;
    rune *rinfo;
    t_len ilen;

    while (TrieIterator_Next(it, &rstr, &len, &rinfo, &ilen, &score, NULL)) {
      size_t str_len = 0;
      char *str = runesToStr(rstr, len, &str_len);
      size_t info_len = 0;
      char *info = rinfo == NULL ? NULL : runesToStr(rinfo, ilen, &info_len);
      size_t slen = str_len;
      char *s = calloc(slen + 1, sizeof(char));
      memcpy(s, str, str_len);
      free(str);
      if (info != NULL) {
        if (info_len > 0) {
          slen = slen + 1 + info_len;
          s = realloc(s, (slen + 1) * sizeof(char));
          memcpy(s + str_len, STR_INFO_SPLIT_SEP, 1);
          memcpy(s + str_len + 1, info, info_len);
        }
        free(info);
      }
      RedisModule_SaveStringBuffer(rdb, s, slen + 1);
      RedisModule_SaveDouble(rdb, (double)score);
      free(s);
      count++;
    }
    if (count != tree->size) {
      RedisModule_Log(ctx, "warning", "Trie: saving %zd nodes actually iterated only %zd nodes",
                      tree->size, count);
    }
    TrieIterator_Free(it);
  }
}

void TrieType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  Trie *tree = (Trie *)value;

  if (tree->root) {
    TrieIterator *it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
    rune *rstr;
    t_len len;
    float score;
    rune *rinfo;
    t_len ilen;

    while (TrieIterator_Next(it, &rstr, &len, &rinfo, &ilen, &score, NULL)) {
      size_t str_len = 0;
      char *str = runesToStr(rstr, len, &str_len);
      size_t info_len = 0;
      char *info = rinfo == NULL ? NULL : runesToStr(rinfo, ilen, &info_len);
      size_t slen = str_len;
      char *s = calloc(slen + 1, sizeof(char));
      memcpy(s, str, str_len);
      free(str);
      if (info != NULL) {
        if (info_len > 0) {
          slen = slen + 1 + info_len;
          s = realloc(s, (slen + 1) * sizeof(char));
          memcpy(s + str_len, STR_INFO_SPLIT_SEP, 1);
          memcpy(s + str_len + 1, info, info_len);
        }
        free(info);
      }
      RedisModule_EmitAOF(aof, TRIE_ADD_CMD, "sbd", key, s, slen, (double)score);
      free(s);
    }

    TrieIterator_Free(it);
  }
}

void TrieType_Digest(RedisModuleDigest *digest, void *value) {
  /* TODO: The DIGEST module interface is yet not implemented. */
}

void TrieType_Free(void *value) {
  Trie *tree = value;
  if (tree->root) {

    TrieNode_Free(tree->root);
  }

  RedisModule_Free(tree);
}

int TrieType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = TrieType_RdbLoad,
                               .rdb_save = TrieType_RdbSave,
                               .aof_rewrite = TrieType_AofRewrite,
                               .free = TrieType_Free};

  TrieType = RedisModule_CreateDataType(ctx, "trietype0", 0, &tm);
  if (TrieType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
