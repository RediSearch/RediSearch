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
#include "rqe_iterator_type.h"

struct RLookupKey; // Forward declaration
struct IndexSpec;
typedef struct MapBuilder RsMapBuilder; // Opaque Rust type (redis_reply::MapBuilder)
typedef struct ProfilePrintCtx RsProfilePrintCtx; // Opaque Rust type (rqe_iterators::profile_print::ProfilePrintCtx)

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
  size_t (*NumEstimated)(const struct QueryIterator *self);

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
   * @param spec The index spec, provided by the caller (result processor).
   * @return VALIDATE_OK if the iterator is still valid
   * @return VALIDATE_MOVED if the iterator is still valid, but the lastDocId has changed (moved forward)
   * @return VALIDATE_ABORTED if the iterator is no longer valid
   */
  ValidateStatus (*Revalidate)(struct QueryIterator *self, struct IndexSpec *spec);

  /**
   * Called immediately before the spec read lock is released, to inform the iterator that it
   * must drop any state that depends on the lock being held (in particular, borrows into
   * inverted-index data). After `Suspend` returns, the only callback that may be invoked on
   * the iterator (until the lock is re-acquired and `Revalidate` is called) is `Free`.
   *
   * Most iterators have no lock-dependent state and use `Default_Suspend` (a no-op).
   * Rust-wrapped iterators flip their internal typestate from Active to Suspended here, so
   * that subsequent calls to read/skip/current/rewind would hard-fail at the FFI boundary.
   */
  void (*Suspend)(struct QueryIterator *self);

  /* release the iterator's context and free everything needed */
  void (*Free)(struct QueryIterator *self);

  /* Rewind the iterator to the beginning and reset its state (including `atEOF` and `lastDocId`) */
  void (*Rewind)(struct QueryIterator *self);

  /* Recursively wrap every child iterator with a Profile layer.
   * Composite iterators call IntoProfiled() on each child and return `self`.
   * Leaf iterators leave this as NULL (no children to profile). */
  struct QueryIterator* (*ProfileChildren)(struct QueryIterator *self);

  /* Print this iterator's profile as a Redis reply.
   * Set by Rust iterators at construction time. C iterators set this to a
   * Rust-exported function. */
  void (*PrintProfile)(const struct QueryIterator *self, RsMapBuilder *map, RsProfilePrintCtx *ctx);
} QueryIterator;

static inline ValidateStatus Default_Revalidate(struct QueryIterator *base, struct IndexSpec *spec) {
  // Default implementation does nothing.
  return VALIDATE_OK;
}

static inline void Default_Suspend(struct QueryIterator *base) {
  // Default implementation does nothing. Used by iterators that hold no lock-dependent state.
  (void)base;
}

#endif
