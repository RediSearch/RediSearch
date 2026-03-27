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
// - Index opening/creation helpers

#include "numeric_index.h"
#include "redis_index.h"
#include "redisearch_rs/headers/iterators_rs.h"
#include "sys/param.h"
#include "rmutil/util.h"
#include "util/arr.h"
#include <math.h>
#include "redismodule.h"
#include "util/misc.h"

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

  return CreateNumericIterator(ctx, t, flt, config, filterCtx);
}
