/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "iterators/iterator_api.h"

typedef struct ProfileCounters ProfileCounters;

size_t ProfileCounters_GetReadCount(ProfileCounters *c);
size_t ProfileCounters_GetSkipToCount(ProfileCounters *c);
int ProfileCounters_GetEof(ProfileCounters *c);

/**
 * @brief Create a new profile iterator that wraps a child iterator
 *
 * Profile iterator is used for profiling query execution. It collects
 * performance metrics from the child iterator.
 *
 * @param child The iterator to wrap
 * @return QueryIterator* The new profile iterator
 */
typedef struct ProfileIterator ProfileIterator;

QueryIterator *NewProfileIterator(QueryIterator *child);

QueryIterator *ProfileIterator_GetChild(ProfileIterator *it);
ProfileCounters *ProfileIterator_GetCounters(ProfileIterator *it);
rs_wall_clock_ns_t ProfileIterator_GetWallTimeNs(ProfileIterator *it);
