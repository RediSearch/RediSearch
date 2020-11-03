
#pragma once

#include "redisearch.h"
#include "search_ctx.h"
#include "rmutil/args.h"
#include "query_error.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define NF_INFINITY (1.0 / 0.0)
#define NF_NEGATIVE_INFINITY (-1.0 / 0.0)

class NumericFilter {
  int parseDoubleRange(const char *s, bool &inclusive, double &target, bool isMin, QueryError *status);

public:
  char *fieldName;
  double min;
  double max;
  bool inclusiveMin;
  bool inclusiveMax;

  NumericFilter(double min, double max, bool inclusiveMin, bool inclusiveMax);
  NumericFilter(ArgsCursor *ac, QueryError *status);
  NumericFilter(const NumericFilter &nf);
  ~NumericFilter();

  // A numeric index allows indexing of documents by numeric ranges, and intersection
  // of them with fulltext indexes.

  bool Match(double score) const {
    bool rc = false;
    // match min - -inf or x >/>= score
    bool matchMin = inclusiveMin ? score >= min : score > min;
    if (matchMin) {
      // match max - +inf or x </<= score
      rc = inclusiveMax ? score <= max : score < max;
    }
    return rc;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////
