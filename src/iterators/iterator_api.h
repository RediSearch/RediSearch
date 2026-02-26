/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef __ITERATOR_API_H__
#define __ITERATOR_API_H__

#include <stdint.h>
#include "redisearch.h"
#include "index_result.h" // IWYU pragma: keep

struct RLookupKey; // Forward declaration

typedef enum IteratorStatus {
  ITERATOR_OK,
  ITERATOR_NOTFOUND,
  ITERATOR_EOF,
  ITERATOR_TIMEOUT,
} IteratorStatus;

typedef enum ValidateStatus {
  VALIDATE_OK,      // The iterator is still valid and at the same position - if wasn't at EOF,
                    // the `current` result is still valid
  VALIDATE_MOVED,   // The iterator is still valid but lastDocID changed, and `current` is a new valid result or
                    // at EOF. If not at EOF, the `current` result should be used before the next read, or it will be overwritten.
  VALIDATE_ABORTED, // The iterator is no longer valid, and should not be used or rewound. Should be freed.
} ValidateStatus;

enum IteratorType {
  INV_IDX_NUMERIC_ITERATOR,
  INV_IDX_TERM_ITERATOR,
  INV_IDX_WILDCARD_ITERATOR,
  INV_IDX_MISSING_ITERATOR,
  INV_IDX_TAG_ITERATOR,
  HYBRID_ITERATOR,
  UNION_ITERATOR,
  INTERSECT_ITERATOR,
  NOT_ITERATOR,
  OPTIONAL_ITERATOR,
  OPTIONAL_OPTIMIZED_ITERATOR,
  WILDCARD_ITERATOR,
  EMPTY_ITERATOR,
  ID_LIST_SORTED_ITERATOR,
  ID_LIST_UNSORTED_ITERATOR,
  METRIC_SORTED_BY_ID_ITERATOR,
  METRIC_SORTED_BY_SCORE_ITERATOR,
  PROFILE_ITERATOR,
  OPTIMUS_ITERATOR,
  MAX_ITERATOR,
};

/* An abstract interface used by readers / intersectors / uniones etc.
Basically query execution creates a tree of iterators that activate each other
recursively */
typedef struct QueryIterator {
  enum IteratorType type;

  // Can the iterator yield more results? The Iterator must ensure that `atEOF` is set correctly when it is sure that the Next Read returns `ITERATOR_EOF`.
  // For instance, NotIterator needs to know if the ChildIterator finishes, otherwise it may not skip the last result correctly.
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

  /**
   * Called when the iterator is being revalidated after a concurrent index change.
   * The iterator should check if it is still valid.
   *
   * @return VALIDATE_OK if the iterator is still valid
   * @return VALIDATE_MOVED if the iterator is still valid, but the lastDocId has changed (moved forward)
   * @return VALIDATE_ABORTED if the iterator is no longer valid
   */
  ValidateStatus (*Revalidate)(struct QueryIterator *self);

  /* release the iterator's context and free everything needed */
  void (*Free)(struct QueryIterator *self);

  /* Rewind the iterator to the beginning and reset its state (including `atEOF` and `lastDocId`) */
  void (*Rewind)(struct QueryIterator *self);
} QueryIterator;

static inline ValidateStatus Default_Revalidate(struct QueryIterator *base) {
  // Default implementation does nothing.
  return VALIDATE_OK;
}

#endif
