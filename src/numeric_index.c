/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// This file contains C code that interfaces with the Rust NumericRangeTree implementation.
// The core tree implementation is now in Rust (numeric_range_tree_ffi crate).
// This file provides:
// - Query iterator creation functions
// - Compatibility wrappers for C code that expects Vector* from Find operations
// - Index opening/creation helpers

#include "numeric_index.h"
#include "redis_index.h"
#include "iterators/inverted_index_iterator.h"
#include "iterators/union_iterator.h"
#include "sys/param.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "util/arr.h"
#include <math.h>
#include "redismodule.h"
#include "util/misc.h"

// Forward declaration for iterator reopen callback
void NumericRangeIterator_OnReopen(void *privdata);

// Create an iterator for a single numeric range
static QueryIterator *NewNumericRangeIterator(const RedisSearchCtx *sctx, const NumericRange *nr,
                                               const NumericFilter *f,
                                               const FieldFilterContext *filterCtx) {
  const FieldSpec *fs = f ? f->fieldSpec : NULL;
  NumericRangeTree *rt = NULL;

  // Get range bounds using accessor functions
  double minVal = NumericRange_MinVal(nr);
  double maxVal = NumericRange_MaxVal(nr);

  // Determine if we can skip filtering during iteration:
  // If the filter is numeric and both min and max values are matched,
  // we can skip the filter check during iteration.
  const NumericFilter *readerFilter = f;
  if (NumericFilter_IsNumeric(f) &&
      NumericFilter_Match(f, minVal) && NumericFilter_Match(f, maxVal)) {
    // make the filter NULL so the reader will ignore it
    readerFilter = NULL;
  }

  if (fs) {
    rt = openNumericOrGeoIndex(sctx->spec, (FieldSpec *)fs, DONT_CREATE_INDEX);
    RS_ASSERT(rt);
  }

  // Create an IndexReader from the Rust numeric range
  // This properly handles the type differences between C and Rust inverted indexes
  IndexReader *reader = NumericRange_NewIndexReader(nr, readerFilter);

  // Create the iterator from the pre-created reader
  return NewInvIndIterator_NumericQueryFromReader(reader, sctx, filterCtx, rt, minVal, maxVal);
}

// Wrapper around Rust's NumericRangeTree_Find that returns Vector* for compatibility
// with existing C code. The caller must free the returned Vector.
static Vector *NumericRangeTree_FindCompat(NumericRangeTree *t, const NumericFilter *nf) {
  NumericRangeTreeFindResult result = NumericRangeTree_Find(t, nf);

  // Convert the result to a Vector
  Vector *v = NewVector(NumericRange *, result.len > 0 ? result.len : 1);
  for (size_t i = 0; i < result.len; i++) {
    Vector_Push(v, (void *)result.ranges[i]);
  }

  // Free the Rust result (but not the ranges themselves - they're owned by the tree)
  NumericRangeTreeFindResult_Free(result);

  return v;
}

/* Create a union iterator from the numeric filter, over all the sub-ranges in the tree that fit
 * the filter */
QueryIterator *createNumericIterator(const RedisSearchCtx *sctx, NumericRangeTree *t,
                                     const NumericFilter *f, IteratorsConfig *config,
                                     const FieldFilterContext *filterCtx) {

  Vector *v = NumericRangeTree_FindCompat(t, f);
  if (!v || Vector_Size(v) == 0) {
    if (v) {
      Vector_Free(v);
    }
    return NULL;
  }

  int n = Vector_Size(v);
  // if we only selected one range - we can just iterate it without union or anything
  if (n == 1) {
    NumericRange *rng;
    Vector_Get(v, 0, &rng);
    QueryIterator *it = NewNumericRangeIterator(sctx, rng, f, filterCtx);
    Vector_Free(v);
    return it;
  }

  // We create a union iterator, advancing a union on all the selected ranges,
  // treating them as one consecutive range
  QueryIterator **its = rm_calloc(n, sizeof(QueryIterator *));

  for (size_t i = 0; i < n; i++) {
    NumericRange *rng;
    Vector_Get(v, i, &rng);
    if (!rng) {
      continue;
    }

    its[i] = NewNumericRangeIterator(sctx, rng, f, filterCtx);
  }
  Vector_Free(v);

  QueryNodeType type = (!f || NumericFilter_IsNumeric(f)) ? QN_NUMERIC : QN_GEO;
  return NewUnionIterator(its, n, true, 1.0, type, NULL, config);
}

NumericRangeTree *openNumericOrGeoIndex(IndexSpec *spec, FieldSpec *fs, bool create_if_missing) {
  RS_ASSERT(FIELD_IS(fs, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO));
  if (!fs->tree && create_if_missing) {
    fs->tree = NewNumericRangeTree(RSGlobalConfig.numericCompress);
    // Get the initial inverted index size from the root node's range
    const NumericRangeNode *root = NumericRangeTree_GetRoot(fs->tree);
    const NumericRange *range = root ? NumericRangeNode_GetRange(root) : NULL;
    size_t initialSize = range ? NumericRange_InvertedIndexSize(range) : 0;
    spec->stats.invertedSize += initialSize;
  }
  return fs->tree;
}

QueryIterator *NewNumericFilterIterator(const RedisSearchCtx *ctx, const NumericFilter *flt,
                                        FieldType forType, IteratorsConfig *config,
                                        const FieldFilterContext *filterCtx) {
  const FieldSpec *fs = flt->fieldSpec;

  NumericRangeTree *t = openNumericOrGeoIndex(ctx->spec, (FieldSpec *)fs, DONT_CREATE_INDEX);
  if (!t) {
    return NULL;
  }

  return createNumericIterator(ctx, t, flt, config, filterCtx);
}
