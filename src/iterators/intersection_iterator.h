/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "iterator_api.h"

/**
 * Create a new intersection iterator.
 * The implementation is provided by Rust (see `iterators_ffi/src/intersection.rs`).
 *
 * @param its The iterators to intersect. Takes ownership of both the array and each iterator.
 * @param num The number of iterators.
 * @param max_slop The maximum slop allowed for the intersection. Negative value for no slop validation.
 * @param in_order Whether the intersection should be in order.
 * @param weight The weight of the intersection result.
 * @return A new intersection iterator, or an empty/single-child iterator after reduction.
 */
QueryIterator *NewIntersectionIterator(QueryIterator **its, size_t num, int max_slop, bool in_order, double weight);

#ifdef __cplusplus
}
#endif
