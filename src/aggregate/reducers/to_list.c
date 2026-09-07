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

// Key set for the membership index. Unlike RSValueSet it neither takes nor drops references:
// every key it holds is also held by the owning vector, which outlives the index (see
// tolistFreeInstance). Skipping the refcount pair per element matters because refcount
// traffic, not allocation, dominates this reducer once values are cheap to obtain.
static dictType RSValueSetBorrowed = {
  .hashFunction = hashFunction_RSValue,
  .keyDup = NULL,
  .valDup = NULL,
  .keyCompare = compare_RSValue,
  .keyDestructor = NULL,
  .valDestructor = NULL,
};

// TOLIST accumulates the *distinct* values of a field per group. Deduplication needs a
// membership test, and the obvious structure for that is a hash set - but one is created
// per group, and groups are overwhelmingly tiny (a parent with a handful of children). A
// `dict` then costs a control struct plus a hash table plus an entry allocation per
// element, dwarfing the pointers it stores: a distributed GROUPBY yielding 273K groups
// paid for 273K hash sets to hold ~2 values each.
//
// So hold the values in a flat, insertion-ordered vector and answer membership by linear
// scan while the group is small, promoting to a dict index only once a group grows past
// TOLIST_LINEAR_MAX. Below that bound a handful of RSValue_Equal comparisons beat hashing
// the value, and the common group needs no allocation beyond the instance itself.
//
// The vector stays authoritative for ordering even after promotion, so output order is
// insertion order for every group size rather than the previous hash order.
#define TOLIST_INLINE_CAP 8

// Group size past which membership moves from a linear scan to a dict index. Chosen so the
// scan stays cheaper than hashing: a distributed GROUPBY ships ~1-2 values per group, far
// below this, so the dict is never built on the common path.
#define TOLIST_LINEAR_MAX 16

typedef struct {
  // Distinct values in insertion order. Points at `inlineVals` until the group outgrows
  // it, then at heap storage of `cap` entries.
  RSValue **vals;
  uint32_t len;
  uint32_t cap;
  // Membership index, built only when `len` exceeds TOLIST_LINEAR_MAX. NULL means
  // membership is answered by scanning `vals`.
  dict *index;
  RSValue *inlineVals[TOLIST_INLINE_CAP];
} TolistCtx;

static void *tolistNewInstance(Reducer *rbase) {
  TolistCtx *ctx = rm_calloc(1, sizeof(*ctx));
  ctx->vals = ctx->inlineVals;
  ctx->cap = TOLIST_INLINE_CAP;
  return ctx;
}

// True if `v` is already held. Uses the dict index once one exists, otherwise scans.
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

  // Crossing the bound: build the index over what we already hold, so subsequent
  // membership tests stop being linear.
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

  // for non array values we simply add the value to the list */
  if (!RSValue_IsArray(v)) {
    tolistAddValue(ctx, v);
  } else {  // For array values we add each distinct element to the list
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
  // Move, don't clone: the grouper calls FreeInstance immediately after Finalize
  // (cleanupGroup in group_by.c), so the instance's references can be handed to the array
  // rather than duplicated and then dropped. `len` is zeroed so FreeInstance releases
  // nothing, and a repeat Finalize would yield an empty array rather than double-free.
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
