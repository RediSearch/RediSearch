#include "trie/trie_type.h"
#include "trie/rune_util.h"

#include "commands.h"
#include "rmalloc.h"

#include "libnu/libnu.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/heap.h"
#include "util/misc.h"

#include <math.h>
#include <sys/param.h>
#include <time.h>
#include <string.h>
#include <limits.h>

///////////////////////////////////////////////////////////////////////////////////////////////

Trie::Trie() {
  Runes rs("");
  root->ctor(rs, 0, 0, NULL, 0, 0, 0, 0);
  size = 0;
}

//---------------------------------------------------------------------------------------------

int Trie::Insert(RedisModuleString *s, double score, int incr, RSPayload *payload) {
  size_t len;
  const char *str = RedisModule_StringPtrLen(s, &len);
  int ret = InsertStringBuffer(str, len, score, incr, payload);
  return ret;
}

//---------------------------------------------------------------------------------------------

int Trie::InsertStringBuffer(const char *s, size_t len, double score, int incr, RSPayload *payload) {
  if (len > TRIE_INITIAL_STRING_LEN * sizeof(rune)) {
    return 0;
  }

  Runes runes(s);
  if (!runes || runes.len() == 0 || runes.len() >= TRIE_INITIAL_STRING_LEN) {
    return 0;
  }

  int rc = root->Add(runes, len, payload, (float)score, incr ? ADD_INCR : ADD_REPLACE);
  size += rc;

  return rc;
}

//---------------------------------------------------------------------------------------------

// Delete the string from the trie. Return true if node was found and deleted, false otherwise.

bool Trie::Delete(const char *s) {
  Runes runes(s);
  if (!runes || runes.len() > TRIE_INITIAL_STRING_LEN) {
    return false;
  }
  int rc = root->Delete(runes);
  size -= rc;
  return rc > 0;
}

//---------------------------------------------------------------------------------------------

TrieSearchResult::~TrieSearchResult() {
  if (str) {
    delete str;
  }
  payload = NULL;
  plen = 0;
}

//---------------------------------------------------------------------------------------------

static int cmpEntries(const void *p1, const void *p2, const void *udata) {
  const TrieSearchResult *e1 = p1, *e2 = p2;
  if (e1->score < e2->score) {
    return 1;
  } else if (e1->score > e2->score) {
    return -1;
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

// Iterate the trie, using maxDist edit distance, returning a trie iterator that the
// caller needs to free. If prefixmode is 1 we treat the string as only a prefix to iterate.
// Otherwise we return an iterator to all strings within maxDist Levenshtein distance.

TrieIterator Trie::Iterate(const char *prefix, size_t len, int maxDist, int prefixMode) {
  size_t rlen;
  Runes runes(prefix, &rlen);
  if (!runes || rlen > TRIE_MAX_PREFIX) {
    return NULL;
  }
  DFAFilter *fc = new DFAFilter(runes, rlen, maxDist, prefixMode);

  TrieIterator it = root->Iterate(FilterFunc, StackPop, fc);
  return it;
}

//---------------------------------------------------------------------------------------------

Vector<TrieSearchResult*> Trie::Search(const char *s, size_t len, size_t num, int maxDist, int prefixMode,
                                       int trim, int optimize) {

  if (len > TRIE_MAX_PREFIX * sizeof(rune)) {
    return Vector<TrieSearchResult *>();
  }

  Runes runes(s, Runes::Folded::Yes);
  // make sure query length does not overflow
  if (!runes || runes.len() >= TRIE_MAX_PREFIX) {
    return Vector<TrieSearchResult *>();
  }

  Heap pq(cmpEntries, NULL, num);

  DFAFilter fc{runes, maxDist, prefixMode};

  TrieIterator it = root->Iterate(FilterFunc, StackPop, &fc);
  rune *rstr;
  t_len slen;
  float score;
  RSPayload payload;

  TrieSearchResult *pooledEntry;
  int dist = maxDist + 1;
  while (it->Next(&rstr, &slen, &payload, &score, &dist)) {
    if (pooledEntry == NULL) {
      pooledEntry->str = NULL;
      pooledEntry->payload = NULL;
      pooledEntry->plen = 0;
    }
    TrieSearchResult *ent = pooledEntry;

    ent->score = slen > 0 && slen == rlen && memcmp(runes, rstr, slen) == 0 ? INT_MAX : score;

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
      if (ent->score >= it->minScore) {
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

  // printf("Nodes consumed: %d/%d (%.02f%%)\n", it->nodesConsumed,
  //        it->nodesConsumed + it->nodesSkipped,
  //        100.0 * (float)(it->nodesConsumed) / (float)(it->nodesConsumed +
  //        it->nodesSkipped));

  // put the results from the heap on a vector to return
  size_t n = MIN(heap_count(pq), num);
  Vector<TrieSearchResult *> ret(n);
  for (int i = 0; i < n; ++i) {
    TrieSearchResult *h = heap_poll(pq);
    ret[n - i - 1] = h;
  }

  // trim the results to remove irrelevant results
  if (trim) {
    float maxScore = 0;
    int i;
    for (i = 0; i < n; ++i) {
      TrieSearchResult *h;
      ret->Get(i, &h);

      if (maxScore && h->score < maxScore / SCORE_TRIM_FACTOR) {
        // TODO: Fix trimming the vector
        ret->top = i;
        break;
      }
      maxScore = MAX(maxScore, h->score);
    }

    for (; i < n; ++i) {
      TrieSearchResult *h = ret[i];
      delete h;
    }
  }

  return ret;
}

//---------------------------------------------------------------------------------------------

// Get a random key from the trie, and put the node's score in the score pointer.
// Returns 0 if the trie is empty and we cannot do that.

bool Trie::RandomKey(char **str, t_len *len, double *score) {
  if (size == 0) {
    return 0;
  }

  rune *rstr;
  t_len rlen;

  // TODO: deduce steps from cardinality properly
  TrieNode *n =
      root->RandomWalk(2 + rand() % 8 + (int)round(logb(1 + size)), &rstr, &rlen);
  if (!n) {
    return false;
  }
  size_t sz;
  *str = runesToStr(rstr, rlen, &sz);
  *len = sz;

  *score = n->score;
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Trie type methods

// declaration of the type for redis registration
RedisModuleType *TrieType;

//---------------------------------------------------------------------------------------------

void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > TRIE_ENCVER_CURRENT) {
    return NULL;
  }
  return TrieType_GenericLoad(rdb, encver > TRIE_ENCVER_NOPAYLOADS);
}

//---------------------------------------------------------------------------------------------

void *TrieType_GenericLoad(RedisModuleIO *rdb, int loadPayloads) {

  uint64_t elements = RedisModule_LoadUnsigned(rdb);
  Trie *tree;

  while (elements--) {
    size_t len;
    RSPayload payload;
    char *str = RedisModule_LoadStringBuffer(rdb, &len);
    double score = RedisModule_LoadDouble(rdb);
    if (loadPayloads) {
      payload.data = RedisModule_LoadStringBuffer(rdb, &payload.len);
      // load an extra space for the null terminator
      payload.len--;
    }
    tree->InsertStringBuffer(str, len - 1, score, 0, payload.len ? &payload : NULL);
    RedisModule_Free(str);
    if (payload.data != NULL) RedisModule_Free(payload.data);
  }
  // tree->root->Print(0, 0);
  return tree;
}

//---------------------------------------------------------------------------------------------

void TrieType_RdbSave(RedisModuleIO *rdb, void *value) {
  TrieType_GenericSave(rdb, (Trie *)value, 1);
}

//---------------------------------------------------------------------------------------------

void TrieType_GenericSave(RedisModuleIO *rdb, Trie *tree, int savePayloads) {
  RedisModule_SaveUnsigned(rdb, tree->size);
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  //  RedisModule_Log(ctx, "notice", "Trie: saving %zd nodes.", tree->size);
  int count = 0;
  if (tree->root) {
    TrieIterator *it = tree->root->Iterate(NULL, NULL, NULL);
    rune *rstr;
    t_len len;
    float score;
    RSPayload payload;

    while (it->Next(&rstr, &len, &payload, &score, NULL)) {
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
      RedisModule_Log(ctx, "warning", "Trie: saving %zd nodes actually iterated only %zd nodes",
                      tree->size, count);
    }
    delete it;
  }
}

//---------------------------------------------------------------------------------------------

void TrieType_Digest(RedisModuleDigest *digest, void *value) {
  // TODO: The DIGEST module interface is yet not implemented
}

//---------------------------------------------------------------------------------------------

void TrieType_Free(void *value) {
  Trie *tree = value;
  if (tree->root) {
    delete tree->root;
  }
}

//---------------------------------------------------------------------------------------------

int TrieType_Register(RedisModuleCtx *ctx) {

  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = TrieType_RdbLoad,
                               .rdb_save = TrieType_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .free = TrieType_Free};

  TrieType = RedisModule_CreateDataType(ctx, "trietype0", TRIE_ENCVER_CURRENT, &tm);
  if (TrieType == NULL) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
