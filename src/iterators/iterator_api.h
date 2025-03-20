/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __ITERATOR_API_H__
#define __ITERATOR_API_H__

#include <stdint.h>
#include "src/redisearch.h"
#include "src/index_result.h"

struct RLookupKey; // Forward declaration

typedef enum IteratorStatus {
   ITERATOR_OK,
   ITERATOR_NOTFOUND,
   ITERATOR_EOF,
   ITERATOR_TIMEOUT,
} IteratorStatus;

enum IteratorType {
  READ_ITERATOR,
  HYBRID_ITERATOR,
  UNION_ITERATOR,
  INTERSECT_ITERATOR,
  NOT_ITERATOR,
  OPTIONAL_ITERATOR,
  WILDCARD_ITERATOR,
  EMPTY_ITERATOR,
  ID_LIST_ITERATOR,
  METRIC_ITERATOR,
  PROFILE_ITERATOR,
  OPTIMUS_ITERATOR,
  MAX_ITERATOR,
};

/* An abstract interface used by readers / intersectors / uniones etc.
Basically query execution creates a tree of iterators that activate each other
recursively */
typedef struct QueryIterator {
  enum IteratorType type;

  // Can the iterator yield more results?
  bool atEOF;

  // the last docId read. Initially should be 0.
  t_docId lastDocId;

  // Current result. Should always point to a valid current result, except when `lastDocId` is 0
  RSIndexResult *current;

  /** Return an upper-bound estimation for the number of results the iterator is going to yield */
  size_t (*NumEstimated)(struct QueryIterator *self);

  /** Read the next entry from the iterator.
   *  On a successful read, the iterator must:
   *  1. Set its `lastDocId` member to the new current result id
   *  2. Set its `current` pointer to its current result, for the caller to access if desired
   *  @returns ITERATOR_OK on normal operation, or any other `IteratorStatus` except `ITERATOR_NOTFOUND`
   */
  IteratorStatus (*Read)(struct QueryIterator *self);

  /** Skip to the next ID of the iterator, which is greater or equal to `docId`.
   *  It is assumed that when `SkipTo` is called, `self->lastDocId < docId`.
   *  On a successful read, the iterator must:
   *  1. Set its `lastDocId` member to the new current result id
   *  2. Set its `current` pointer to its current result, for the caller to access if desired.
   *  A read is successful if the iterator has a valid result to yield.
   *  @returns ITERATOR_OK if the iterator has found `docId`.
   *  @returns ITERATOR_NOTFOUND if the iterator has only found a result greater than `docId`.
   *  In any other case, `current` and `lastDocId` should be untouched, and the relevant IteratorStatus is returned.
   */
  IteratorStatus (*SkipTo)(struct QueryIterator *self, t_docId docId);

  /* release the iterator's context and free everything needed */
  void (*Free)(struct QueryIterator *self);

  /* Rewind the iterator to the beginning and reset its state (including `atEOF` and `lastDocId`) */
  void (*Rewind)(struct QueryIterator *self);
} QueryIterator;

// Scaffold for the iterator API. TODO: Remove this when the old API is removed
#define IT_V2(api_name) api_name##_V2

#endif
