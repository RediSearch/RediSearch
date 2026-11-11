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

// The value pushed to iterators through `SetScoreThreshold` for heap threshold `theta`.
// Thresholds that are not positive are withdrawn: the supported scorers never score below 0, so
// such a threshold could not drop anything.
static inline double ScoreBound_MinCompetitive(double theta) {
  return theta > 0 ? theta * (1 - SCORE_BOUND_REL_EPSILON) : -INFINITY;
}

// Whether a candidate with upper bound `bound` cannot reach the results.
static inline bool ScoreBound_CanDrop(double bound, double minCompetitiveScore) {
  return bound < minCompetitiveScore;
}

// Whether the composition rules of `CurrentMaxScore` hold for the scorer named `scorerName`.
// Any other scorer disables score-bound filtering for the query.
bool ScoreBound_ScorerSupported(const char *scorerName);

// `ScoreBoundCtx.filterLeafBound` for a supported scorer.
double ScoreBound_FilterLeafBound(const char *scorerName);

#ifdef __cplusplus
}
#endif

#endif  // RS_SCORE_BOUND_H_
