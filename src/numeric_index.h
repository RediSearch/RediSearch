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
#include "redisearch.h"
#include "index_result.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "concurrent_ctx.h"
#include "numeric_filter.h"
#include "config.h"
#include "iterators/iterator_api.h"

// Include the Rust-generated header for NumericRangeTree types and functions
#include "numeric_range_tree.h"

#ifdef __cplusplus
extern "C" {
#endif

// HLL precision constants (must match Rust implementation)
#define NR_BIT_PRECISION 6 // For error rate of `1.04 / sqrt(2^6)` = 13%
#define NR_REG_SIZE (1 << NR_BIT_PRECISION)

// Create a numeric filter iterator
QueryIterator *NewNumericFilterIterator(const RedisSearchCtx *ctx, const NumericFilter *flt, FieldType forType,
                                        IteratorsConfig *config, const FieldFilterContext* filterCtx);

// Open or create a numeric/geo index for a field
NumericRangeTree *openNumericOrGeoIndex(IndexSpec* spec, FieldSpec* fs, bool create_if_missing);

#ifdef __cplusplus
}
#endif
#endif
