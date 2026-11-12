/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Drop rule for score-bound candidate filtering: a candidate whose `CurrentMaxScore` cannot reach
// the score threshold of a full top-K heap is dropped before its document metadata is read.

#ifndef RS_SCORE_BOUND_H_
#define RS_SCORE_BOUND_H_

#include <math.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Relative margin under the heap threshold inside which a candidate is never dropped.
// Bounds are computed separately from the scorer and may round differently (the C scorers use
// `float` constants), so only clearly non-competitive candidates are dropped. Ties and near ties
// go through the exact score and the sorter's own tie-break.
#define SCORE_BOUND_REL_EPSILON 1e-6

// Sets `*minCompetitiveScore` to the value passed down as the threshold for heap minimum `theta`,
// and returns true, when that threshold can drop anything; otherwise returns false and the caller
// reads without one. Leaves only report bounds under scorers that never score below 0, so only a
// positive `theta` can. Every threshold passed to `NextWithThreshold` or `ReadWithThreshold`
// comes from here, so it is always positive.
static inline bool ScoreBound_MinCompetitive(double theta, double *minCompetitiveScore) {
  if (!(theta > 0)) {
    return false;
  }
  *minCompetitiveScore = theta * (1 - SCORE_BOUND_REL_EPSILON);
  return true;
}

// Whether a candidate with upper bound `bound` cannot reach the results.
static inline bool ScoreBound_CanDrop(double bound, double minCompetitiveScore) {
  return bound < minCompetitiveScore;
}

#ifdef __cplusplus
}
#endif

#endif  // RS_SCORE_BOUND_H_
