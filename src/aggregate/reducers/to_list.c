/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <aggregate/reducer.h>

static uint64_t hashFunction_RSValue(const void *key) {
  return RSValue_Hash(key, 0);
}
static void *dup_RSValue(void *p, const void *key) {
  return RSValue_IncrRef((RSValue *)key);
}
static int compare_RSValue(void *privdata, const void *key1, const void *key2) {
  return RSValue_Equal(key1, key2, NULL);
}
static void destructor_RSValue(void *privdata, void *val) {
  RSValue *v = val;
  RSValue_Decref(v);
}

static dictType RSValueSet = {
  .hashFunction = hashFunction_RSValue,
  .keyDup = dup_RSValue,
  .valDup = NULL,
  .keyCompare = compare_RSValue,
  .keyDestructor = destructor_RSValue,
  .valDestructor = NULL,
};

typedef struct {
  dict *values;
  const RLookupKey *srckey;
} tolistCtx;

static void *tolistNewInstance(Reducer *rbase) {
  tolistCtx *ctx = Reducer_BlkAlloc(rbase, sizeof(*ctx), 100 * sizeof(*ctx));
  ctx->values = dictCreate(&RSValueSet, NULL);
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
    dictAdd(tlc->values, v, NULL);
  } else {  // For array values we add each distinct element to the list
    uint32_t len = RSValue_ArrayLen(v);
    for (uint32_t i = 0; i < len; i++) {
      dictAdd(tlc->values, RSValue_ArrayItem(v, i), NULL);
    }
  }
  return 1;
}

static RSValue *tolistFinalize(Reducer *rbase, void *ctx) {
  tolistCtx *tlc = ctx;
  size_t len = dictSize(tlc->values);
  dictIterator *it = dictGetIterator(tlc->values);
  RSValue **arr = RSValue_AllocateArray(len);
  for (size_t i = 0; i < len; i++) {
    dictEntry *de = dictNext(it);
    arr[i] = RSValue_IncrRef(dictGetKey(de));
  }
  dictReleaseIterator(it);
  RSValue *ret = RSValue_NewArray(arr, len);
  return ret;
}

static void tolistFreeInstance(Reducer *parent, void *p) {
  tolistCtx *tlc = p;
  dictRelease(tlc->values);
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
