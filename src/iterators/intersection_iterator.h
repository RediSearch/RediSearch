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

#include <stddef.h>
#include "iterator_api.h"

/**
 * Create a new intersection iterator.
 *
 * @param its The iterators to intersect. Takes ownership of both the array and each iterator.
 * @param num The number of iterators.
 * @param max_slop The maximum slop allowed for the intersection. Negative value for no slop validation.
 * @param in_order Whether the intersection should be in order.
 * @param weight The weight of the intersection result.
 * @return A new intersection iterator, or an empty/single-child iterator after reduction.
 */
QueryIterator *NewIntersectionIterator(QueryIterator **its, size_t num, int max_slop, bool in_order, double weight);

/**
 * Returns the number of child iterators held by the intersection iterator.
 *
 * @param header Must be a valid non-null pointer to an INTERSECT_ITERATOR.
 */
size_t GetIntersectionIteratorNumChildren(const QueryIterator *header);

/**
 * Returns a non-owning pointer to the child at `idx`.
 *
 * @param header Must be a valid non-null pointer to an INTERSECT_ITERATOR.
 * @param idx Must be less than GetIntersectionIteratorNumChildren(header).
 */
const QueryIterator *GetIntersectionIteratorChild(const QueryIterator *header, size_t idx);

/**
 * Append a new child iterator to the intersection.
 *
 * Transfers ownership of `child` to the intersection. Updates the estimated result count
 * if the new child has a lower estimate than the current minimum.
 *
 * @param header Must be a valid non-null pointer to an INTERSECT_ITERATOR.
 * @param child Must be a valid non-null pointer to a QueryIterator, not aliased.
 */
void AddIntersectionIteratorChild(QueryIterator *header, QueryIterator *child);

/**
 * Apply `callback` to each child iterator slot, passing a mutable QueryIterator**.
 *
 * Designed for use with Profile_AddIters, which replaces each child with a
 * profile-wrapping iterator in place.
 *
 * @param header Must be a valid non-null pointer to an INTERSECT_ITERATOR.
 * @param callback Must be a valid function pointer. The callback receives a pointer
 *                 to each child slot and may replace it with a new iterator that
 *                 takes ownership of the original (e.g. NewProfileIterator semantics).
 */
void ForEachIntersectionChildMut(QueryIterator *header, void (*callback)(QueryIterator **));

#ifdef __cplusplus
}
#endif
