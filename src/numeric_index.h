/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __NUMERIC_INDEX_H__
#define __NUMERIC_INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include "rmutil/vector.h"
#include "redisearch.h"
#include "index_result.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "inverted_index.h"
#include "numeric_filter.h"
#include "config.h"
#include "iterators/iterator_api.h"

// Include Rust-generated types for NumericRangeTree, NumericRange, NumericRangeNode, etc.
#include "numeric_range_tree.h"

#ifdef __cplusplus
extern "C" {
#endif

QueryIterator *NewNumericFilterIterator(const RedisSearchCtx *ctx, const NumericFilter *flt, FieldType forType,
                                        IteratorsConfig *config, const FieldFilterContext* filterCtx);

NumericRangeTree *openNumericOrGeoIndex(IndexSpec* spec, FieldSpec* fs, bool create_if_missing);

// Passes RSGlobalConfig.numericTreeMaxDepthRange automatically
#define NumericRangeTree_Add(t, docId, value, isMulti) \
    _NumericRangeTree_Add((t), (docId), (value), (isMulti), RSGlobalConfig.numericTreeMaxDepthRange)

#ifdef __cplusplus
}
#endif
#endif
