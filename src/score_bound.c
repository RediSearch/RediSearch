/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "score_bound.h"

#include <string.h>

#include "ext/default.h"

bool ScoreBound_ScorerSupported(const char *scorerName) {
  // Each of these sums per-term contributions scaled by the aggregate weights, or ignores the
  // query tree altogether (DOCSCORE), so a sum of child bounds scaled by `max(weight, 1)` bounds
  // it. Normalizing or max-combining scorers (DISMAX, BM25STD.TANH, BM25STD.NORM, HAMMING) and
  // extension scorers do not have that shape.
  return !strcmp(scorerName, TFIDF_SCORER_NAME) || !strcmp(scorerName, TFIDF_DOCNORM_SCORER_NAME) ||
         !strcmp(scorerName, BM25_SCORER_NAME) || !strcmp(scorerName, BM25_STD_SCORER_NAME) ||
         !strcmp(scorerName, DOCSCORE_SCORER);
}

double ScoreBound_FilterLeafBound(const char *scorerName) {
  // BM25STD scores a numeric or geo result 0 and a tag result through its BM25 IDF, which tag
  // terms leave at 0. TFIDF and legacy BM25 add `weight * freq` for such results, scaled by
  // document-level factors a filter leaf does not know; DOCSCORE is not a sum at all.
  return !strcmp(scorerName, BM25_STD_SCORER_NAME) ? 0 : INFINITY;
}
