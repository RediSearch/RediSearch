/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "redisearch.h"
#include "search_ctx.h"
#include "rmutil/args.h"
#include "query_error.h"
#include "query_node.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NumericFilter {
  const FieldSpec *field;   // the numeric field
  double min;               // beginning of range
  double max;               // end of range
  const void *geoFilter;    // geo filter
  bool inclusiveMin;        // range includes min value
  bool inclusiveMax;        // range includes max val

  // used by optimizer
  bool asc;                 // order of SORTBY asc/desc
  size_t limit;             // minimum number of result needed
  size_t offset;            // record number of documents in iterated ranges. used to skip them
} NumericFilter;

#define NumericFilter_IsNumeric(f) (!(f)->geoFilter)

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax,
                                bool asc);
NumericFilter *NumericFilter_LegacyParse(ArgsCursor *ac, QueryError *status);
int NumericFilter_EvalParams(dict *params, QueryNode *node, QueryError *status);
void NumericFilter_Free(NumericFilter *nf);

int parseDoubleRange(const char *s, bool *inclusive, double *target, int isMin,
                     int sign, QueryError *status);

/*
A numeric index allows indexing of documents by numeric ranges, and intersection
of them with fulltext indexes.
*/
static inline int NumericFilter_Match(const NumericFilter *f, double score) {

  int rc = 0;
  // match min - -inf or x >/>= score
  int matchMin = (f->inclusiveMin ? score >= f->min : score > f->min);

  if (matchMin) {
    // match max - +inf or x </<= score
    rc = (f->inclusiveMax ? score <= f->max : score < f->max);
  }
  return rc;
}

#ifdef __cplusplus
}
#endif
