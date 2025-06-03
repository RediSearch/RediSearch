/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "redisearch.h"
#include "search_ctx.h"
#include "rmutil/args.h"
#include "query_error.h"
#include "query_node.h"
#include "obfuscation/hidden.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NumericFilter {
  const FieldSpec *fieldSpec;
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

// LegacyNumericFilter is a numeric filter that is used in the legacy query syntax
// it is a wrapper around the NumericFilter struct
// it is used to parse the legacy query syntax and convert it to the new query syntax
// When parsing the legacy filters we do not have the index spec and we only have the field name
// For that reason during the parsing phase the base.fieldSpec will be NULL
// We will fill the fieldSpec during the apply context phase where we will use the field name to find the field spec
// This struct was added in order to fix previous behaviour where the string pointer was stored inside the field spec pointer
typedef struct LegacyNumericFilter {
  NumericFilter base;     // the numeric filter base details
  HiddenString *field;    // the numeric field name
} LegacyNumericFilter;

#define NumericFilter_IsNumeric(f) (!(f)->geoFilter)

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax,
                                bool asc, const FieldSpec *fs);
LegacyNumericFilter *NumericFilter_LegacyParse(ArgsCursor *ac, bool *hasEmptyFilterValue, QueryError *status);
int NumericFilter_EvalParams(dict *params, QueryNode *node, QueryError *status);
void NumericFilter_Free(NumericFilter *nf);
void LegacyNumericFilter_Free(LegacyNumericFilter *nf);

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
