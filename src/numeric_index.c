/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// This file contains the C glue code for integrating the Rust NumericRangeTree
// implementation with the rest of the RediSearch codebase.
//
// The core tree implementation has been moved to Rust:
// - src/redisearch_rs/numeric_range_tree/
// - src/redisearch_rs/c_entrypoint/numeric_range_tree_ffi/
//
// This file provides:
// - openNumericOrGeoIndex: Opens or creates a numeric/geo index
// - NewNumericFilterIterator: Creates a query iterator for numeric filters
// - createNumericIterator: Internal function to create numeric iterators
// - NewNumericRangeIterator: Creates an iterator for a single range

#include "numeric_index.h"
#include "redis_index.h"
#include "iterators/inverted_index_iterator.h"
#include "iterators/union_iterator.h"
#include "sys/param.h"
#include "util/arr.h"
#include "redismodule.h"
#include "util/misc.h"

// Creates an iterator for a single numeric range.
// Uses the Rust FFI to create an IndexReader for the range's entries.
static QueryIterator *NewNumericRangeIterator(const RedisSearchCtx *sctx,
                                              const NumericRange *nr,
                                              const NumericFilter *f,
                                              const FieldFilterContext* filterCtx) {
  const FieldSpec *fs = f->fieldSpec;
  const NumericRangeTree *rt = NULL;

  double minVal = NumericRange_GetMinVal(nr);
  double maxVal = NumericRange_GetMaxVal(nr);

  // For numeric (non-geo) queries, if the range is fully contained in the filter,
  // we don't need to check each record.
  // For geo queries, we always keep the filter to check the distance.
  const NumericFilter *filterForReader = f;
  if (NumericFilter_IsNumeric(f) &&
      NumericFilter_Match(f, minVal) && NumericFilter_Match(f, maxVal)) {
    filterForReader = NULL;
  }

  if (fs) {
    rt = openNumericOrGeoIndex(sctx->spec, (FieldSpec *)fs, DONT_CREATE_INDEX);
    RS_ASSERT(rt);
  }

  // Create an IndexReader for this range using Rust FFI
  IndexReader *reader = NumericRange_CreateReader(nr, filterForReader);
  if (!reader) {
    return NULL;
  }

  return NewInvIndIterator_FromReader(reader, sctx, filterCtx, rt, minVal, maxVal);
}

// Creates a union iterator from the numeric filter, over all the sub-ranges
// in the tree that fit the filter.
static QueryIterator *createNumericIterator(const RedisSearchCtx *sctx,
                                            const NumericRangeTree *t,
                                            const NumericFilter *f,
                                            IteratorsConfig *config,
                                            const FieldFilterContext* filterCtx) {
  // Use Rust FFI to find matching ranges
  Vec______NumericRange *v = NumericRangeTree_Find(t, f);
  if (!v) {
    return NULL;
  }

  size_t n = NumericRangeTree_VectorSize(v);
  if (n == 0) {
    NumericRangeTree_FreeVector(v);
    return NULL;
  }

  // If we only selected one range, we can just iterate it without a union
  if (n == 1) {
    const NumericRange *rng = NumericRangeTree_VectorGet(v, 0);
    QueryIterator *it = NewNumericRangeIterator(sctx, rng, f, filterCtx);
    NumericRangeTree_FreeVector(v);
    return it;
  }

  // Multiple ranges - create a union iterator
  QueryIterator **its = rm_calloc(n, sizeof(QueryIterator *));

  for (size_t i = 0; i < n; i++) {
    const NumericRange *rng = NumericRangeTree_VectorGet(v, i);
    if (!rng) {
      continue;
    }
    its[i] = NewNumericRangeIterator(sctx, rng, f, filterCtx);
  }

  NumericRangeTree_FreeVector(v);

  QueryNodeType type = (!f || NumericFilter_IsNumeric(f)) ? QN_NUMERIC : QN_GEO;
  return NewUnionIterator(its, n, true, 1.0, type, NULL, config);
}

// Opens or creates a numeric/geo index for a field.
NumericRangeTree *openNumericOrGeoIndex(IndexSpec* spec, FieldSpec* fs, bool create_if_missing) {
  RS_ASSERT(FIELD_IS(fs, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO));
  if (!fs->tree && create_if_missing) {
    fs->tree = NewNumericRangeTree();
    // Get the initial inverted index size from the root node's range
    const NumericRangeNode *root = NumericRangeTree_GetRoot(fs->tree);
    if (root) {
      const NumericRange *range = NumericRangeNode_GetRange(root);
      if (range) {
        spec->stats.invertedSize += NumericRange_GetInvertedIndexSize(range);
      }
    }
  }
  return fs->tree;
}

// Creates a query iterator for a numeric filter.
// This is the main entry point for numeric range queries.
QueryIterator *NewNumericFilterIterator(const RedisSearchCtx *ctx, const NumericFilter *flt,
                                        FieldType forType, IteratorsConfig *config,
                                        const FieldFilterContext* filterCtx) {
  const FieldSpec *fs = flt->fieldSpec;

  NumericRangeTree *t = openNumericOrGeoIndex(ctx->spec, (FieldSpec *)fs, DONT_CREATE_INDEX);
  if (!t) {
    return NULL;
  }

  return createNumericIterator(ctx, t, flt, config, filterCtx);
}
