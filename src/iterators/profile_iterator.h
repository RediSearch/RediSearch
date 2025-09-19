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

typedef struct {
  size_t read;
  size_t skipTo;
  int eof;
} ProfileCounters;

typedef struct {
  QueryIterator base;
  QueryIterator *child;
  ProfileCounters counters;
  rs_wall_clock_ns_t wallTime; // This field serves as a time accumulator, so using rs_wall_clock_ns_t is required.
} ProfileIterator;

/**
 * @brief Create a new profile iterator that wraps a child iterator
 *
 * Profile iterator is used for profiling query execution. It collects
 * performance metrics from the child iterator.
 *
 * @param child The iterator to wrap
 * @return QueryIterator* The new profile iterator
 */
QueryIterator *NewProfileIterator(QueryIterator *child);
