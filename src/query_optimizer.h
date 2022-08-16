#pragma once

#include "field_spec.h"
#include "aggregate/aggregate.h"

struct AREQ;

typedef enum {
  // No optimization
  Q_OPT_NONE = -1,

  // Optimization was not assigned
  Q_OPT_UNDECIDED = 0,

  // Reduce numeric range. No additional filter
  Q_OPT_PARTIAL_RANGE = 1,

  // If there is no sorting, remove sorter (similar to FT.AGGREGATE)
  Q_OPT_NO_SORTER = 2,

  // Attempt reduced numeric range.
  // Additional filter might reduce number of matches.
  // May require additional iteration or change of optimization
  Q_OPT_HYBRID = 3,

  // Use `FILTER` result processor instead of numeric range
  Q_OPT_FILTER = 4,
} Q_Optimize_Type;

typedef struct QOptimizer {
    Q_Optimize_Type type;       // type of optimization

    size_t limit;               // number of required results

    bool scorerReq;             // does the query require a scorer (WITHSCORES does not count)

    const char *fieldName;      // name of sortby field
    const FieldSpec *field;     // spec of sortby field
    QueryNode *sortbyNode;      // pointer to QueryNode
    bool asc;                   // ASC/DESC order of sortby
 } QOptimizer;

/* parse query parameter for optimizer */
void QOptimizer_Parse(AREQ *req);

/* iterate over query nodes and find:
 * 1. does the query requires scoring
 * 2. can the sortby field be extracted for optimization
 **/
void QOptimizer_QueryNodes(QueryNode *root, QOptimizer *opt);

/* iterate over index iterator, check estimations and performs further optimizations */
void QOptimizer_Iterators(AREQ *req, QOptimizer *opt);
