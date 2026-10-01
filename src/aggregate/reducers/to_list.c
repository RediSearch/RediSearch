/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <aggregate/reducer.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "value_ffi.h"
#include "rlookup.h"
#include "rlookup_ffi.h"
#include "rmalloc.h"
#include "util/dict/dict.h"

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
  RSValue_DecrRef((RSValue *)key);
}

static dictType RSValueSet = {
    .hashFunction = hashFunction_RSValue,
    .keyDup = dup_RSValue,
    .valDup = NULL,
    .keyCompare = compare_RSValue,
    .keyDestructor = destructor_RSValue,
    .valDestructor = NULL,
};

// Keys borrow references from the owning vector, which outlives the index.
static dictType RSValueSetBorrowed = {
    .hashFunction = hashFunction_RSValue,
    .keyDup = NULL,
    .valDup = NULL,
    .keyCompare = compare_RSValue,
    .keyDestructor = NULL,
    .valDestructor = NULL,
};

// Small groups avoid a hash-table allocation; larger groups use a membership index.
#define TOLIST_INLINE_CAP 8
#define TOLIST_LINEAR_MAX 16

typedef struct {
  RSValue **vals;
  uint32_t len;
  uint32_t cap;
  dict *index;
  RSValue *inlineVals[TOLIST_INLINE_CAP];
} TolistCtx;

static void *tolistNewInstance(Reducer *rbase) {
  TolistCtx *ctx = rm_calloc(1, sizeof(*ctx));
  ctx->vals = ctx->inlineVals;
  ctx->cap = TOLIST_INLINE_CAP;
  return ctx;
}

static bool tolistContains(const TolistCtx *ctx, RSValue *v) {
  if (ctx->index) {
    return dictFind(ctx->index, v) != NULL;
  }
  for (uint32_t i = 0; i < ctx->len; i++) {
    if (RSValue_Equal(ctx->vals[i], v, NULL)) {
      return true;
    }
  }
  return false;
}

// Append `v`, taking a reference. Caller guarantees `v` is not already held.
static void tolistAppend(TolistCtx *ctx, RSValue *v) {
  if (ctx->len == ctx->cap) {
    uint32_t newCap = ctx->cap * 2;
    if (ctx->vals == ctx->inlineVals) {
      ctx->vals = rm_malloc(newCap * sizeof(*ctx->vals));
      memcpy(ctx->vals, ctx->inlineVals, ctx->len * sizeof(*ctx->vals));
    } else {
      ctx->vals = rm_realloc(ctx->vals, newCap * sizeof(*ctx->vals));
    }
    ctx->cap = newCap;
  }
  ctx->vals[ctx->len++] = RSValue_IncrRef(v);

  if (!ctx->index && ctx->len > TOLIST_LINEAR_MAX) {
    ctx->index = dictCreate(&RSValueSetBorrowed, NULL);
    for (uint32_t i = 0; i < ctx->len; i++) {
      dictAdd(ctx->index, ctx->vals[i], NULL);
    }
  } else if (ctx->index) {
    dictAdd(ctx->index, v, NULL);
  }
}

static void tolistAddValue(TolistCtx *ctx, RSValue *v) {
  if (!tolistContains(ctx, v)) {
    tolistAppend(ctx, v);
  }
}

static int tolistAdd(Reducer *rbase, void *c, const RLookupRow *srcrow) {
  TolistCtx *ctx = c;
  RSValue *v = RLookupRow_Get(rbase->srckey, srcrow);
  if (!v) {
    return 1;
  }

  if (!RSValue_IsArray(v)) {
    tolistAddValue(ctx, v);
  } else {
    uint32_t len = RSValue_ArrayLen(v);
    for (uint32_t i = 0; i < len; i++) {
      tolistAddValue(ctx, RSValue_ArrayItem(v, i));
    }
  }
  return 1;
}

static RSValue *tolistFinalize(Reducer *rbase, void *c) {
  TolistCtx *ctx = c;
  RSValue **arr = RSValue_NewArrayBuilder(ctx->len);
  // Transfer ownership to the result; FreeInstance must not release these references.
  uint32_t n = ctx->len;
  for (uint32_t i = 0; i < n; i++) {
    arr[i] = ctx->vals[i];
  }
  ctx->len = 0;
  return RSValue_NewArrayFromBuilder(arr, n);
}

static void tolistFreeInstance(Reducer *parent, void *p) {
  TolistCtx *ctx = p;
  // Release the borrowing index before the values it points at.
  if (ctx->index) {
    dictRelease(ctx->index);
    ctx->index = NULL;
  }
  for (uint32_t i = 0; i < ctx->len; i++) {
    RSValue_DecrRef(ctx->vals[i]);
  }
  if (ctx->vals != ctx->inlineVals) {
    rm_free(ctx->vals);
  }
  rm_free(ctx);
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
