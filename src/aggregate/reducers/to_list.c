/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
static void destructor_RSValue(void *privdata, void *key) {
  RSValue_Decref((RSValue *)key);
}

static dictType RSValueSet = {
  .hashFunction = hashFunction_RSValue,
  .keyDup = dup_RSValue,
  .valDup = NULL,
  .keyCompare = compare_RSValue,
  .keyDestructor = destructor_RSValue,
  .valDestructor = NULL,
};

static void *tolistNewInstance(Reducer *rbase) {
  dict *values = dictCreate(&RSValueSet, NULL);
  return values;
}

static int tolistAdd(Reducer *rbase, void *ctx, const RLookupRow *srcrow) {
  dict *values = ctx;
  RSValue *v = RLookup_GetItem(rbase->srckey, srcrow);
  if (!v) {
    return 1;
  }

  // for non array values we simply add the value to the list */
  if (v->t != RSValue_Array) {
    dictAdd(values, v, NULL);
  } else {  // For array values we add each distinct element to the list
    uint32_t len = RSValue_ArrayLen(v);
    for (uint32_t i = 0; i < len; i++) {
      dictAdd(values, RSValue_ArrayItem(v, i), NULL);
    }
  }
  return 1;
}

static RSValue *tolistFinalize(Reducer *rbase, void *ctx) {
  dict *values = ctx;
  size_t len = dictSize(values);
  dictIterator *it = dictGetIterator(values);
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
  dict *values = p;
  dictRelease(values);
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
