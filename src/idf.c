/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "idf.h"

inline double CalculateIDF(size_t totalDocs, size_t termDocs) {
  return logb(1.0F + totalDocs / (double)(termDocs ?: 1));
}

// IDF computation for BM25 standard scoring algorithm (which is slightly different from the regular
// IDF computation).
inline double CalculateIDF_BM25(size_t totalDocs, size_t termDocs) {
  // totalDocs should never be less than termDocs, as that causes an underflow
  // wraparound in the below calculation.
  // Yet, that can happen in some scenarios of deletions/updates, until fixed in
  // the next GC run.
  // In that case, we set totalDocs to termDocs, as a temporary fix.
  totalDocs = MAX(totalDocs, termDocs);
  return log(1.0F + (totalDocs - termDocs + 0.5F) / (termDocs + 0.5F));
}
