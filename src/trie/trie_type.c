/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "libnu/libnu.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/heap.h"
#include "util/misc.h"
#include "rune_util.h"
#include "trie_type.h"
#include "rmalloc.h"
#include "rdb.h"

#include <math.h>
#include <sys/param.h>
#include <time.h>
#include <string.h>
#include <limits.h>

Trie *NewTrie(TrieFreeCallback freecb, TrieSortMode sortMode) {
  Trie *tree = rm_malloc(sizeof(Trie));
  rune *rs = strToRunes("", 0);
  tree->root = __newTrieNode(rs, 0, 0, NULL, 0, 0, 0, 0, sortMode);
  tree->size = 0;
  tree->freecb = freecb;
  tree->sortMode = sortMode;
  rm_free(rs);
  return tree;
}

int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr, RSPayload *payload) {
  size_t len;
  const char *str = RedisModule_StringPtrLen(s, &len);
  int ret = Trie_InsertStringBuffer(t, str, len, score, incr, payload);
  return ret;
}

int Trie_InsertStringBuffer(Trie *t, const char *s, size_t len, double score, int incr,
                            RSPayload *payload) {
  if (len > TRIE_INITIAL_STRING_LEN * sizeof(rune)) {
    return 0;
  }
  runeBuf buf;
  rune *runes = runeBufFill(s, len, &buf, &len);
  int rc = Trie_InsertRune(t, runes, len, score, incr, payload);
  runeBufFree(&buf);
  return rc;
}

int Trie_InsertRune(Trie *t, const rune *runes, size_t len, double score, int incr,
                    RSPayload *payload) {
  int rc = 0;                              
  if (runes && len && len < TRIE_INITIAL_STRING_LEN) {
    rc = TrieNode_Add(&t->root, runes, len, payload, (float)score, incr ? ADD_INCR : ADD_REPLACE, t->freecb);
    t->size += rc;
  }
  return rc;
}

void *Trie_GetValueStringBuffer(Trie *t, const char *s, size_t len, bool exact) {
  if (len > TRIE_INITIAL_STRING_LEN * sizeof(rune)) {
    return 0;
  }
  runeBuf buf;
  rune *runes = runeBufFill(s, len, &buf, &len);
  void *val = Trie_GetValueRune(t, runes, len, exact);
  runeBufFree(&buf);
  return val;
}

void *Trie_GetValueRune(Trie *t, const rune *runes, size_t len, bool exact) {
  return TrieNode_GetValue(t->root, runes, len, exact);
}

int Trie_Delete(Trie *t, const char *s, size_t len) {
  runeBuf buf;
  rune *runes = runeBufFill(s, len, &buf, &len);
  if (!runes || len > TRIE_INITIAL_STRING_LEN) {
    return 0;
  }
  int rc = Trie_DeleteRunes(t, runes, len);
  runeBufFree(&buf);
  return rc;
}

int Trie_DeleteRunes(Trie *t, const rune *runes, size_t len) {
  int rc = TrieNode_Delete(t->root, runes, len, t->freecb);
  t->size -= rc;
  return rc;
}

void TrieSearchResult_Free(TrieSearchResult *e) {
  if (e->str) {
    rm_free(e->str);
    e->str = NULL;
  }
  e->payload = NULL;
  e->plen = 0;
  rm_free(e);
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

TrieIterator *Trie_Iterate(Trie *t, const char *prefix, size_t len, int maxDist, int prefixMode) {
  size_t rlen;
  rune *runes = strToFoldedRunes(prefix, &rlen);
  if (!runes || rlen > TRIE_MAX_PREFIX) {
    if (runes) {
      rm_free(runes);
    }
    return NULL;
  }

  DFAFilter *fc = NewDFAFilter(runes, rlen, maxDist, prefixMode);

  TrieIterator *it = TrieNode_Iterate(t->root, FilterFunc, StackPop, fc);
  rm_free(runes);
  return it;
}

Vector *Trie_Search(Trie *tree, const char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize) {

  if (len > TRIE_MAX_PREFIX * sizeof(rune)) {
    return NULL;
  }
  size_t rlen;
  rune *runes = strToFoldedRunes(s, &rlen);
  // make sure query length does not overflow
  if (!runes || rlen >= TRIE_MAX_PREFIX) {
    rm_free(runes);
    return NULL;
  }

  heap_t *pq = rm_malloc(heap_sizeof(num));
  heap_init(pq, cmpEntries, NULL, num);

  DFAFilter *fc = NewDFAFilter(runes, rlen, maxDist, prefixMode);

  TrieIterator *it = TrieNode_Iterate(tree->root, FilterFunc, StackPop, fc);
  // TrieIterator *it = TrieNode_Iterate(tree->root,NULL, NULL, NULL);
  rune *rstr;
  t_len slen;
  float score;
  RSPayload payload = {.data = NULL, .len = 0};

  TrieSearchResult *pooledEntry = NULL;
  int dist = maxDist + 1;
  while (TrieIterator_Next(it, &rstr, &slen, &payload, &score, &dist)) {
    if (pooledEntry == NULL) {
      pooledEntry = rm_malloc(sizeof(TrieSearchResult));
      pooledEntry->str = NULL;
      pooledEntry->payload = NULL;
      pooledEntry->plen = 0;
    }
    TrieSearchResult *ent = pooledEntry;

    ent->score = slen > 0 && slen == rlen && memcmp(runes, rstr, slen) == 0 ? (float)INT_MAX : score;

    if (maxDist > 0) {
      // factor the distance into the score
      ent->score *= exp((double)-(2 * dist));
    }
    // in prefix mode we also factor in the total length of the suffix
    if (prefixMode) {
      ent->score /= sqrt(1 + (slen >= len ? slen - len : len - slen));
    }

    if (heap_count(pq) < heap_size(pq)) {
      ent->str = runesToStr(rstr, slen, &ent->len);
      ent->payload = payload.data;
      ent->plen = payload.len;
      heap_offerx(pq, ent);
      pooledEntry = NULL;

      if (heap_count(pq) == heap_size(pq)) {
        TrieSearchResult *qe = heap_peek(pq);
        it->minScore = qe->score;
      }

    } else {
      if (ent->score > it->minScore) {
        pooledEntry = heap_poll(pq);
        rm_free(pooledEntry->str);
        pooledEntry->str = NULL;
        ent->str = runesToStr(rstr, slen, &ent->len);
        ent->payload = payload.data;
        ent->plen = payload.len;
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

  rm_free(runes);
  TrieIterator_Free(it);
  heap_free(pq);

  return ret;
}

int Trie_RandomKey(Trie *t, char **str, t_len *len, double *score) {
  if (t->size == 0) {
    return 0;
  }

  rune *rstr;
  t_len rlen;

  // TODO: deduce steps from cardinality properly
  TrieNode *n =
      TrieNode_RandomWalk(t->root, 2 + rand() % 8 + (int)round(logb(1 + t->size)), &rstr, &rlen);
  if (!n) {
    return 0;
  }
  size_t sz;
  *str = runesToStr(rstr, rlen, &sz);
  *len = sz;
  rm_free(rstr);

  *score = n->score;
  return 1;
}

/***************************************************************
 *
 *                       Trie type methods
 *
 ***************************************************************/

/* declaration of the type for redis registration. */
RedisModuleType *TrieType;

void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > TRIE_ENCVER_CURRENT) {
    return NULL;
  }
  return TrieType_GenericLoad(rdb, encver > TRIE_ENCVER_NOPAYLOADS);
}

void *TrieType_GenericLoad(RedisModuleIO *rdb, int loadPayloads) {

  Trie *tree = NULL;
  char *str = NULL;
  uint64_t elements = LoadUnsigned_IOError(rdb, goto cleanup);
  tree = NewTrie(NULL, Trie_Sort_Score);

  while (elements--) {
    size_t len;
    RSPayload payload = {.data = NULL, .len = 0};
    str = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
    double score = LoadDouble_IOError(rdb, goto cleanup);
    if (loadPayloads) {
      payload.data = LoadStringBuffer_IOError(rdb, &payload.len, goto cleanup);
      // load an extra space for the null terminator
      payload.len--;
    }
    Trie_InsertStringBuffer(tree, str, len - 1, score, 0, payload.len ? &payload : NULL);
    RedisModule_Free(str);
    if (payload.data != NULL) RedisModule_Free(payload.data);
  }
  // TrieNode_Print(tree->root, 0, 0);
  return tree;

cleanup:
  if (str) {
    RedisModule_Free(str);
  }
  if (tree) {
    TrieType_Free(tree);
  }
  return NULL;
}

void TrieType_RdbSave(RedisModuleIO *rdb, void *value) {
  TrieType_GenericSave(rdb, (Trie *)value, 1);
}

void TrieType_GenericSave(RedisModuleIO *rdb, Trie *tree, int savePayloads) {
  RedisModule_SaveUnsigned(rdb, tree->size);
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  //  RedisModule_Log(ctx, "notice", "Trie: saving %zd nodes.", tree->size);
  int count = 0;
  if (tree->root) {
    TrieIterator *it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
    rune *rstr;
    t_len len;
    float score;
    RSPayload payload = {.data = NULL, .len = 0};

    while (TrieIterator_Next(it, &rstr, &len, &payload, &score, NULL)) {
      size_t slen = 0;
      char *s = runesToStr(rstr, len, &slen);
      RedisModule_SaveStringBuffer(rdb, s, slen + 1);
      RedisModule_SaveDouble(rdb, (double)score);

      if (savePayloads) {
        // save an extra space for the null terminator to make the payload null terminated on load
        if (payload.data != NULL && payload.len > 0) {
          RedisModule_SaveStringBuffer(rdb, payload.data, payload.len + 1);
        } else {
          // If there's no payload - we save an empty string
          RedisModule_SaveStringBuffer(rdb, "", 1);
        }
      }
      // TODO: Save a marker for empty payload!
      rm_free(s);
      count++;
    }
    if (count != tree->size) {
      RedisModule_Log(ctx, "warning", "Trie: saving %zd nodes actually iterated only %d nodes",
                      tree->size, count);
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
    TrieNode_Free(tree->root, tree->freecb);
  }

  rm_free(tree);
}

size_t TrieType_MemUsage(const void *value) {
  const Trie *t = value;
  return t->size * (sizeof(TrieNode) +    // size of struct
                    sizeof(TrieNode *) +  // size of ptr to struct in parent node
                    sizeof(rune) +        // rune key to children in parent node
                    2 * sizeof(rune));    // each node contains some runes as str[]
}

int TrieType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = TrieType_RdbLoad,
                               .rdb_save = TrieType_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .free = TrieType_Free,
                               .mem_usage = TrieType_MemUsage};

  TrieType = RedisModule_CreateDataType(ctx, "trietype0", TRIE_ENCVER_CURRENT, &tm);
  if (TrieType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
