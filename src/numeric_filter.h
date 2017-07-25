#ifndef __NUMERIC_FILTER_H__
#define __NUMERIC_FILTER_H__
#include "redisearch.h"
#include "search_ctx.h"
#include "rmutil/vector.h"

#define NF_INFINITY (1.0 / 0.0)
#define NF_NEGATIVE_INFINITY (-1.0 / 0.0)

typedef struct numericFilter {
  const char *fieldName;
  double min;
  double max;
  int inclusiveMin;
  int inclusiveMax;

} NumericFilter;

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax);
void NumericFilter_Free(NumericFilter *nf);
NumericFilter *ParseNumericFilter(RedisSearchCtx *ctx, RedisModuleString **argv, int argc);
Vector *ParseMultipleFilters(RedisSearchCtx *ctx, RedisModuleString **argv, int argc);

/*
A numeric index allows indexing of documents by numeric ranges, and intersection
of them with fulltext indexes.
*/
static inline int NumericFilter_Match(NumericFilter *f, double score) {

  int rc = 0;
  // match min - -inf or x >/>= score
  int matchMin = (f->inclusiveMin ? score >= f->min : score > f->min);

  if (matchMin) {
    // match max - +inf or x </<= score
    rc = (f->inclusiveMax ? score <= f->max : score < f->max);
  }
  return rc;
}

#endif