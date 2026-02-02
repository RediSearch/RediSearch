/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "idf.h"
#include "util/minmax.h"

#include <math.h>

double CalculateIDF(size_t totalDocs, size_t termDocs) {
  // (totalDocs + 1) because logb is used, and logb(1.99) = 0 and logb(2.00) = 1)
  return logb(1.0F + (totalDocs + 1) / (double)(termDocs ?: 1));
}

// IDF computation for BM25 standard scoring algorithm (which is slightly different from the regular
// IDF computation).
double CalculateIDF_BM25(size_t totalDocs, size_t termDocs) {
  // totalDocs should never be less than termDocs, as that causes an underflow
  // wraparound in the below calculation.
  // Yet, that can happen in some scenarios of deletions/updates, until fixed in
  // the next GC run.
  // In that case, we set totalDocs to termDocs, as a temporary fix.
  totalDocs = MAX(totalDocs, termDocs);
  return log(1.0F + (totalDocs - termDocs + 0.5F) / (termDocs + 0.5F));
}
