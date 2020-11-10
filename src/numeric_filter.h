#ifndef __NUMERIC_FILTER_H__
#define __NUMERIC_FILTER_H__
#include "redisearch.h"
#include "search_ctx.h"
#include "rmutil/args.h"
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NF_INFINITY (1.0 / 0.0)
#define NF_NEGATIVE_INFINITY (-1.0 / 0.0)

typedef struct NumericFilter {
  char *fieldName;
  double min;
  double max;
  int inclusiveMin;
  int inclusiveMax;
  const void *geoFilter;
} NumericFilter;

typedef struct {
  int sz;
  int numRecords;
  uint32_t changed;
} NRN_AddRv;

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax);
NumericFilter *NumericFilter_Parse(ArgsCursor *ac, QueryError *status);
void NumericFilter_Free(NumericFilter *nf);

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
#endif
