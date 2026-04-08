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
 * Reduce the raw child list before building an intersection iterator.
 *
 * Rules applied in order:
 *  1. Wildcard iterators are removed; if ALL children were wildcards, the last one is returned.
 *  2. If any child is NULL or EMPTY_ITERATOR, all others are freed and that iterator
 *     (or a fresh empty iterator for a NULL slot) is returned.
 *  3. If exactly one non-wildcard child remains, it is returned directly.
 *  4. Otherwise NULL is returned and *num is updated to the compacted count —
 *     the caller should build a real intersection from `its[0..*num]`
 *     (wildcards beyond that index are already freed).
 *
 * When non-NULL is returned, `its` has already been freed.
 * When NULL is returned, `its` is still valid and owned by the caller.
 */
QueryIterator *IntersectionIteratorReducer(QueryIterator **its, size_t *num);

#ifdef __cplusplus
}
#endif
