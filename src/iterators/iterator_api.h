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
                    // The result set is still reported as complete: the query ends normally.
  VALIDATE_TIMEOUT, // The deadline expired while revalidating. The iterator is no longer valid and
                    // should be freed, exactly as for `VALIDATE_ABORTED`. The two differ only in
                    // what the caller reports: a timeout leaves the result set INCOMPLETE, so the
                    // caller must surface it (`RS_RESULT_TIMEDOUT`) rather than end-of-results.
} ValidateStatus;

/* An abstract interface used by readers / intersectors / uniones etc.
Basically query execution creates a tree of iterators that activate each other
recursively */
typedef struct QueryIterator {
  enum IteratorType type;

  // Has the iterator run *past* its last result? Set once a `Read`/`SkipTo` has returned
  // `ITERATOR_EOF`, or a `Revalidate` moved past the end — never while the iterator is still
  // positioned on its last result, which it still owes to its caller.
  //
  // Consumers that pick the live members out of a set rely on that boundary: a composite
  // rebuilding its active children after a revalidation keeps every child that still owes a
  // result, and drops only those with nothing left. Setting this a step early costs that
  // child's last document.
  //
  // Iterators that need to know in advance whether the next `Read` will produce anything keep
  // that as their own private state; it answers a different question and flips a step earlier.
  bool atEOF;

  // the last docId read. Initially should be 0.
  t_docId lastDocId;

  // Current result: the document the iterator is positioned on, or NULL when it is
  // positioned on none.
  //
  // Non-NULL after a `Read` or `SkipTo` returning ITERATOR_OK or ITERATOR_NOTFOUND, and
  // after a `Revalidate` returning VALIDATE_MOVED with a result.
  //
  // NULL before the first read (`lastDocId` is 0), and again once an operation has
  // reported that there is nothing to point at: an ITERATOR_EOF, or a VALIDATE_MOVED
  // that landed past the end. The two answers are the same state, so the field is the C
  // face of Rust's `RQEIterator::current`, which returns `None` in exactly these cases —
  // that is what lets a caller distinguish "moved onto a document" from "moved past the
  // end" after a revalidation. Clearing it is also what keeps the pointer safe: an
  // iterator that owns the result it hands out frees it on the way past the end, so a
  // pointer left behind would dangle rather than merely go stale.
  //
  // Caveat, until the port completes: `OptimizerIterator` and `HybridIterator` are still
  // implemented in C and do not clear this field, so after they report EOF it keeps
  // pointing at the last result they yielded. Both are slated to be replaced by the Rust
  // `TopKIterator`, which clears it like every other Rust iterator; until then, a caller
  // that inspects this field after a non-OK status may get a document it has already
  // consumed instead of NULL. Check the status, not this field.
  RSIndexResult *current;

  /** Return an upper-bound estimation for the number of results the iterator is going to yield */
  size_t (*NumEstimated)(const struct QueryIterator *self);

  /** Read the next entry from the iterator.
   *  On a successful read, the iterator must:
   *  1. Set its `lastDocId` member to the new current result id
   *  2. Set its `current` pointer to its current result, for the caller to access if desired
   *  On ITERATOR_EOF it must instead set `atEOF` and clear `current` to NULL, leaving
   *  `lastDocId` on the last result it yielded: there is nothing to point at, and an
   *  iterator that owns its result frees it on the way past the end.
   *  On ITERATOR_TIMEOUT both `current` and `lastDocId` are untouched — the iterator has
   *  not moved, and a later call may still find a result where it stands.
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
   *  Otherwise the relevant IteratorStatus is returned, under the same post-conditions as
   *  `Read`: ITERATOR_EOF sets `atEOF` and clears `current` to NULL while leaving
   *  `lastDocId` where the last yield left it — never on `docId`, which the iterator has no
   *  result to back — and ITERATOR_TIMEOUT touches neither.
   */
  IteratorStatus (*SkipTo)(struct QueryIterator *self, t_docId docId);

  /**
   * Called when the iterator is being revalidated after a concurrent index change.
   * The iterator should check if it is still valid.
   *
   * @param spec The index spec, provided by the caller (result processor).
   * @return VALIDATE_OK if the iterator is still valid
   * @return VALIDATE_MOVED if the iterator is still valid, but the lastDocId has changed
   * (moved forward). A move that landed on a document publishes it in `current`; one that
   * ran past the end sets `atEOF` and clears `current` to NULL, which is how a caller tells
   * the two apart.
   * @return VALIDATE_ABORTED if the iterator is no longer valid
   * @return VALIDATE_TIMEOUT if the deadline expired mid-revalidation. Composite iterators must
   *         propagate it to their caller rather than folding it into VALIDATE_ABORTED: freeing is
   *         a single act at the root for both, but only a timeout says the results are partial.
   */
  ValidateStatus (*Revalidate)(struct QueryIterator *self, struct IndexSpec *spec);

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

#endif
