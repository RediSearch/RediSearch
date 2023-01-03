/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <aggregate/reducer.h>
#include "redisearch_rs/trie_rs/src/triemap.h"

typedef struct {
  RS_TrieMap *values;
  const RLookupKey *srckey;
} tolistCtx;

static void *tolistNewInstance(Reducer *rbase) {
  tolistCtx *ctx = Reducer_BlkAlloc(rbase, sizeof(*ctx), 100 * sizeof(*ctx));
  ctx->values = RS_NewTrieMap();
  ctx->srckey = rbase->srckey;
  return ctx;
}

static int tolistAdd(Reducer *rbase, void *ctx, const RLookupRow *srcrow) {
  tolistCtx *tlc = ctx;
  RSValue *v = RLookup_GetItem(tlc->srckey, srcrow);
  if (!v) {
    return 1;
  }

  // for non array values we simply add the value to the list */
  if (v->t != RSValue_Array) {
    uint64_t hval = RSValue_Hash(v, 0);
    if (RS_TrieMap_Get(tlc->values, (char *)&hval, sizeof(hval)) == NULL) {

      RS_TrieMap_Add(tlc->values, (char *)&hval, sizeof(hval), RSValue_IncrRef(RSValue_MakePersistent(v)));
    }
  } else {  // For array values we add each distinct element to the list
    uint32_t len = RSValue_ArrayLen(v);
    for (uint32_t i = 0; i < len; i++) {
      RSValue *av = RSValue_ArrayItem(v, i);
      uint64_t hval = RSValue_Hash(av, 0);
      if (RS_TrieMap_Get(tlc->values, (char *)&hval, sizeof(hval)) == NULL) {

          RS_TrieMap_Add(tlc->values, (char *)&hval, sizeof(hval), RSValue_IncrRef(RSValue_MakePersistent(av)));
      }
    }
  }
  return 1;
}

static RSValue *tolistFinalize(Reducer *rbase, void *ctx) {
  tolistCtx *tlc = ctx;
  RS_SubTrieIterator *it = RS_TrieMap_Find(tlc->values, "", 0);
  char *c;
  size_t l;
  void *ptr;
  RSValue **arr = rm_calloc(RS_TrieMap_Size(tlc->values), sizeof(RSValue));
  size_t i = 0;
  while (RS_SubTrieIterator_Next(it, &c, &l, &ptr)) {
    if (ptr) {
      arr[i++] = ptr;
    }
  }

  RSValue *ret = RSValue_NewArrayEx(arr, i, RSVAL_ARRAY_ALLOC);
  RS_SubTrieIterator_Free(it);
  return ret;
}

static void freeValues(void *ptr) {
  RSValue_Decref((RSValue *)ptr);
}

static void tolistFreeInstance(Reducer *parent, void *p) {
  tolistCtx *tlc = p;
  RS_TrieMap_Free(tlc->values, freeValues);
}

Reducer *RDCRToList_New(const ReducerOptions *opts) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOptions_GetKey(opts, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->Add = tolistAdd;
  r->Finalize = tolistFinalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = tolistFreeInstance;
  r->NewInstance = tolistNewInstance;
  return r;
}
