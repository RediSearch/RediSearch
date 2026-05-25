/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "libnu/libnu.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/heap.h"
#include "util/misc.h"
#include "rune_util.h"
#include "trie.h"
#include "rmalloc.h"
#include "rdb.h"
#include "term_stream_codec.h"

#include <math.h>
#include <sys/param.h>
#include <time.h>
#include <string.h>
#include <limits.h>

struct Trie {
  TrieNode *root;
  size_t size;
  TrieFreeCallback freecb;
  TrieSortMode sortMode;
};

size_t Trie_Size(const Trie *t) {
  RS_ASSERT(t);
  return t->size;
}

Trie *NewTrie(TrieFreeCallback freecb, TrieSortMode sortMode) {
  Trie *tree = rm_malloc(sizeof(Trie));
  rune *rs = strToRunes("", 0);
  tree->root = __newTrieNode(rs, 0, 0, NULL, 0, 0, 0, 0, sortMode, 0);
  tree->size = 0;
  tree->freecb = freecb;
  tree->sortMode = sortMode;
  rm_free(rs);
  return tree;
}

int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr, RSPayload *payload,
                size_t numDocs) {
  size_t len;
  const char *str = RedisModule_StringPtrLen(s, &len);
  int ret = Trie_InsertStringBuffer(t, str, len, score, incr, payload, numDocs);
  return ret;
}

int Trie_InsertStringBuffer(Trie *t, const char *s, size_t len, double score, int incr,
                            RSPayload *payload, size_t numDocs) {
  if (len > TRIE_INITIAL_STRING_LEN * sizeof(rune)) {
    return 0;
  }
  runeBuf buf;
  rune *runes = runeBufFill(s, len, &buf, &len);
  int rc = Trie_InsertRune(t, runes, len, score, incr, payload, numDocs);
  runeBufFree(&buf);
  return rc;
}

int Trie_InsertRune(Trie *t, const rune *runes, size_t len, double score, int incr,
                    RSPayload *payload, size_t numDocs) {
  int rc = 0;
  if (runes && len && len < TRIE_INITIAL_STRING_LEN) {
    rc = TrieNode_Add(&t->root, runes, len, payload, (float)score, incr ? ADD_INCR : ADD_REPLACE,
                      t->freecb, numDocs);
    if (rc == TRIE_OK_NEW) {
      t->size += rc;
    }
  }
  return rc;
}

int Trie_InsertRuneNoSize(Trie *t, const rune *runes, size_t len, double score, int incr,
                          RSPayload *payload, size_t numDocs) {
  return TrieNode_Add(&t->root, runes, len, payload, (float)score, incr ? ADD_INCR : ADD_REPLACE,
                      t->freecb, numDocs);
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

TrieNode *Trie_GetNode(Trie *t, const rune *str, t_len len, bool exact, int *offsetOut) {
  return TrieNode_Get(t->root, str, len, exact, offsetOut);
}

void Trie_IterateRange(Trie *t, const rune *min, int minlen, bool includeMin,
                       const rune *max, int maxlen, bool includeMax,
                       TrieRangeCallback callback, void *ctx) {
  TrieNode_IterateRange(t->root, min, minlen, includeMin, max, maxlen, includeMax, callback, ctx);
}

void Trie_IterateContains(Trie *t, const rune *str, int nstr, bool prefix, bool suffix,
                          TrieRangeCallback callback, void *ctx, struct timespec *timeout,
                          bool skipTimeoutChecks) {
  TrieNode_IterateContains(t->root, str, nstr, prefix, suffix, callback, ctx, timeout,
                           skipTimeoutChecks);
}

void Trie_IterateWildcard(Trie *t, const rune *str, int nstr,
                          TrieRangeCallback callback, void *ctx, struct timespec *timeout,
                          bool skipTimeoutChecks) {
  TrieNode_IterateWildcard(t->root, str, nstr, callback, ctx, timeout, skipTimeoutChecks);
}

// Forward declaration for the internal rune-based function
static TrieDecrResult Trie_DecrementNumDocsRunes(Trie *t, const rune *runes, size_t len, size_t delta);

TrieDecrResult Trie_DecrementNumDocs(Trie *t, const char *s, size_t len, size_t delta) {
  if (len > TRIE_INITIAL_STRING_LEN * sizeof(rune)) {
    return TRIE_DECR_NOT_FOUND;
  }
  runeBuf buf;
  size_t runeLen = len;
  rune *runes = runeBufFill(s, len, &buf, &runeLen);
  if (!runes) {
    return TRIE_DECR_NOT_FOUND;
  }
  TrieDecrResult rc = Trie_DecrementNumDocsRunes(t, runes, runeLen, delta);
  runeBufFree(&buf);
  return rc;
}

static TrieDecrResult Trie_DecrementNumDocsRunes(Trie *t, const rune *runes, size_t len, size_t delta) {
  if (!runes || len == 0 || len >= TRIE_INITIAL_STRING_LEN) {
    return TRIE_DECR_NOT_FOUND;
  }

  // Find the node for this term
  TrieNode *node = TrieNode_Get(t->root, runes, len, true, NULL);
  if (!node) {
    return TRIE_DECR_NOT_FOUND;
  }

  // Only terminal nodes represent actual terms in the trie.
  // Non-terminal nodes are internal split/prefix nodes and should not be modified.
  // TrieNode_Delete only succeeds on terminal nodes, so we must check this first
  // to avoid corrupting numDocs on non-terminal nodes.
  if (!TrieNode_IsTerminal(node)) {
    return TRIE_DECR_NOT_FOUND;
  }

  // Decrement numDocs, clamping to 0 to avoid underflow
  if (delta >= node->numDocs) {
    node->numDocs = 0;
  } else {
    node->numDocs -= delta;
  }

  // If numDocs reached 0, delete the node
  if (node->numDocs == 0) {
    int deleted = TrieNode_Delete(t->root, runes, len, t->freecb);
    if (deleted) {
      t->size -= 1;
      return TRIE_DECR_DELETED;
    }
    // Node was already deleted or couldn't be deleted
    return TRIE_DECR_UPDATED;
  }

  return TRIE_DECR_UPDATED;
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

TrieIterator *Trie_IterateAll(Trie *t) {
  return TrieNode_Iterate(t->root, NULL, NULL, NULL);
}

TrieIterator *Trie_Iterate(Trie *t, const char *prefix, size_t len, int maxDist, int prefixMode) {
  size_t rlen;
  rune *runes = strToLowerRunes(prefix, len, &rlen);
  if (!runes || rlen > TRIE_MAX_PREFIX) {
    if (runes) {
      rm_free(runes);
    }
    return NULL;
  }

  DFAFilter *fc = NewDFAFilter(runes, rlen, maxDist, prefixMode);

  TrieIterator *it = TrieNode_Iterate(t->root, LoweringFilterFunc, StackPop, fc);
  rm_free(runes);
  return it;
}

Vector *Trie_Search(Trie *tree, const char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize) {

  if (len > TRIE_MAX_PREFIX * sizeof(rune)) {
    return NULL;
  }
  size_t rlen;
  rune *runes = strToSingleCodepointFoldedRunes(s, len, &rlen);
  // make sure query length does not overflow
  if (!runes || rlen >= TRIE_MAX_PREFIX) {
    rm_free(runes);
    return NULL;
  }

  heap_t *pq = rm_malloc(heap_sizeof(num));
  heap_init(pq, cmpEntries, NULL, num);

  DFAFilter *fc = NewDFAFilter(runes, rlen, maxDist, prefixMode);

  TrieIterator *it = TrieNode_Iterate(tree->root, FoldingFilterFunc, StackPop, fc);
  // TrieIterator *it = TrieNode_Iterate(tree->root,NULL, NULL, NULL);
  rune *rstr;
  t_len slen;
  float score;
  RSPayload payload = {.data = NULL, .len = 0};

  TrieSearchResult *pooledEntry = NULL;
  int dist = maxDist + 1;
  while (TrieIterator_Next(it, &rstr, &slen, &payload, &score, NULL, &dist)) {
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

/* Enumerator state for saving a C `Trie *` through the term-stream codec.
 * Owns the active TrieIterator and the per-step `runesToStr` scratch
 * buffer; the enum function frees the previous scratch on each advance
 * (matches the original inline save loop's `rm_free(s)` at end of step). */
typedef struct {
  Trie *tree;
  TrieIterator *it;
  char *scratch;
  RSPayload payload;
  size_t count;
} CTrieEnumCtx;

static bool c_trie_enum(void *ctx, const char **term, size_t *term_len, double *score,
                        const char **payload, size_t *payload_len, size_t *num_docs) {
  CTrieEnumCtx *s = ctx;
  if (s->scratch) {
    rm_free(s->scratch);
    s->scratch = NULL;
  }
  if (!s->it) {
    return false;
  }
  rune *rstr;
  t_len rlen;
  float fscore;
  s->payload.data = NULL;
  s->payload.len = 0;
  size_t numDocs = 0;
  if (!TrieIterator_Next(s->it, &rstr, &rlen, &s->payload, &fscore, &numDocs, NULL)) {
    return false;
  }
  size_t slen = 0;
  s->scratch = runesToStr(rstr, rlen, &slen);
  *term = s->scratch;
  *term_len = slen;
  *score = (double)fscore;
  *payload = s->payload.data;
  *payload_len = s->payload.len;
  *num_docs = numDocs;
  s->count++;
  return true;
}

static void c_trie_enum_dispose(CTrieEnumCtx *s) {
  if (s->scratch) {
    rm_free(s->scratch);
    s->scratch = NULL;
  }
  if (s->it) {
    TrieIterator_Free(s->it);
    s->it = NULL;
  }
}

/* Sink state for loading a C `Trie *` through the term-stream codec. */
typedef struct {
  Trie *tree;
} CTrieSinkCtx;

static int c_trie_sink(void *ctx, const char *term, size_t term_len, double score,
                       const char *payload, size_t payload_len, size_t num_docs) {
  CTrieSinkCtx *s = ctx;
  RSPayload p = {.data = (char *)payload, .len = payload_len};
  int rc =
      Trie_InsertStringBuffer(s->tree, term, term_len, score, 0, payload_len ? &p : NULL, num_docs);
  return (rc == TRIE_ERR_PAYLOAD_OVERFLOW) ? -1 : 0;
}

void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > TRIE_ENCVER_CURRENT) {
    return NULL;
  }
  return TrieType_GenericLoad(rdb, encver >= TRIE_ENCVER_PAYLOADS, encver >= TRIE_ENCVER_NUMDOCS);
}

void *TrieType_GenericLoad(RedisModuleIO *rdb, bool loadPayloads, bool loadNumDocs) {
  CTrieSinkCtx ctx = {.tree = NewTrie(NULL, Trie_Sort_Score)};
  int rc = TermStream_RdbLoad(rdb, c_trie_sink, &ctx, loadPayloads, loadNumDocs);
  if (rc != 0) {
    TrieType_Free(ctx.tree);
    return NULL;
  }
  return ctx.tree;
}

void TrieType_RdbSave(RedisModuleIO *rdb, void *value) {
  TrieType_GenericSave(rdb, (Trie *)value, true, true);
}

void TrieType_GenericSave(RedisModuleIO *rdb, Trie *tree, bool savePayloads, bool saveNumDocs) {
  CTrieEnumCtx ctx = {.tree = tree, .it = NULL, .scratch = NULL, .count = 0};
  if (tree->root) {
    ctx.it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
  }
  TermStream_RdbSave(rdb, tree->size, c_trie_enum, &ctx, savePayloads, saveNumDocs);
  if (tree->root && ctx.count != tree->size) {
    RedisModuleCtx *rctx = RedisModule_GetContextFromIO(rdb);
    RedisModule_Log(rctx, "warning", "Trie: saving %zd nodes actually iterated only %zd nodes",
                    tree->size, ctx.count);
  }
  c_trie_enum_dispose(&ctx);
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
