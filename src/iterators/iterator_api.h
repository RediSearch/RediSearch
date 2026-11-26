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

#include <math.h>
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
  ITERATOR_STOPPED, // Ended early on a timeout or a requested stop; the request's limits say which.
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
                    // caller must surface it (`RS_RESULT_STOPPED`) rather than end-of-results.
} ValidateStatus;

/* An abstract interface used by readers / intersectors / uniones etc.
Basically query execution creates a tree of iterators that activate each other
recursively.
New callbacks go last, so that adding one leaves the offsets of the existing ones unchanged for
iterators built outside this module, such as the disk API's. */
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
   *  On ITERATOR_STOPPED both `current` and `lastDocId` are untouched — the iterator has
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
   *  result to back — and ITERATOR_STOPPED touches neither.
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

  /* Score-bound hooks. All four are required, like `Read`: an iterator that reports no bounds
   * sets them with `QueryIterator_SetNoScoreBounds`. */

  /* Whether `CurrentMaxScore` can ever be finite for this tree. Fixed when the tree is built, so
   * a caller may check it once. */
  bool (*ReportsScoreBounds)(const struct QueryIterator *self);

  /* Upper bound on the final score of the document the iterator is positioned on, under the
   * query's scorer, computed only from data the iterator already holds: it must not cost I/O, and
   * must never be less than the score the scorer would compute. `+INFINITY` means "unknown" and is
   * always correct. `ReadWithThreshold` drops a document whose value cannot reach the threshold, so
   * how tight it is decides how many candidates are dropped before their document is read. It
   * covers this one document, where `CurrentBlockBound` covers a whole block: any value from the
   * exact score up is valid, and the block maximum is the loosest useful one. The block maximum
   * drops nothing beyond what skipping blocks already does; a per-document bound, or the exact
   * score when computable, also drops weak documents inside kept blocks. It only decides drops: a
   * kept candidate is still read and scored as usual, even when it is exact. Only valid while
   * positioned on a result. */
  double (*CurrentMaxScore)(const struct QueryIterator *self);

  /* Bound on the scores of every document the iterator can yield from the current result up to
   * `*lastDocId`, the end of the storage block that holds it. Returns false, leaving the outputs
   * untouched, when there is no block to report. Only valid while positioned on a result. */
  bool (*CurrentBlockBound)(const struct QueryIterator *self, t_docId *lastDocId, double *maxScore);

  /* `Read`, except that the iterator may move past documents whose `CurrentMaxScore` is below
   * `minCompetitiveScore` without yielding them; it never moves past a document that could reach
   * it, and never yields a document whose `CurrentMaxScore` is below it, so the caller takes every
   * result as competitive. Same post-conditions as `Read`. `minCompetitiveScore` is always
   * positive (see `QueryProcessingCtx::minCompetitiveScore`): a caller without a threshold calls
   * `Read`.
   * `Default_ReadWithThreshold`, a plain `Read`, is only correct for an iterator that does not
   * report bounds: one that does must implement this. */
  IteratorStatus (*ReadWithThreshold)(struct QueryIterator *self, double minCompetitiveScore);

  /**
   * Called immediately before the spec read lock is released, to inform the iterator that it
   * must drop any state that depends on the lock being held (in particular, borrows into
   * inverted-index data). After `Suspend` returns, the only callback that may be invoked on
   * the iterator (until the lock is re-acquired and `Revalidate` is called) is `Free`.
   *
   * Iterators with no lock-dependent state install `Default_Suspend` (a no-op). Not every
   * iterator installs a callback yet, so `Suspend` may be NULL and must not be called until
   * they all do.
   *
   * Rust-wrapped iterators flip their internal typestate from Active to Suspended here, so
   * that subsequent calls to read/skip/current/rewind would hard-fail at the FFI boundary.
   */
  void (*Suspend)(struct QueryIterator *self);

  /* The factor an intersection multiplies this iterator's `NumEstimated` by to order its
   * children when it is built: the lowest product leads. Required, like `Read`: an iterator with
   * no opinion sets `Default_IntersectionSortWeight`. Dispatching through the header, rather than
   * on `type`, keeps it right for an iterator that reports another iterator's type. */
  double (*IntersectionSortWeight)(const struct QueryIterator *self, bool prioritizeUnionChildren);
} QueryIterator;

static inline ValidateStatus Default_Revalidate(struct QueryIterator *base, struct IndexSpec *spec) {
  // Default implementation does nothing.
  return VALIDATE_OK;
}

// The score-bound hooks of an iterator that reports no bounds.
static inline bool Default_ReportsScoreBounds(const struct QueryIterator *self) {
  return false;
}

static inline double Default_CurrentMaxScore(const struct QueryIterator *self) {
  return INFINITY;
}

static inline bool Default_CurrentBlockBound(const struct QueryIterator *self, t_docId *lastDocId,
                                             double *maxScore) {
  return false;
}

static inline IteratorStatus Default_ReadWithThreshold(struct QueryIterator *self,
                                                       double minCompetitiveScore) {
  return self->Read(self);
}

// The sort weight of an iterator that does not influence its intersection's child order.
static inline double Default_IntersectionSortWeight(const struct QueryIterator *self,
                                                    bool prioritizeUnionChildren) {
  return 1.0;
}

static inline void QueryIterator_SetNoScoreBounds(QueryIterator *it) {
  it->ReportsScoreBounds = Default_ReportsScoreBounds;
  it->CurrentMaxScore = Default_CurrentMaxScore;
  it->CurrentBlockBound = Default_CurrentBlockBound;
  it->ReadWithThreshold = Default_ReadWithThreshold;
}

static inline void Default_Suspend(struct QueryIterator *base) {
  // Default implementation does nothing. Used by iterators that hold no lock-dependent state.
  (void)base;
}

#endif
