/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "field_spec.h"
#include "aggregate/aggregate.h"

struct AREQ;

// decision table
/**********************************************************
* NUM * TEXT  *     with SORTBY       *    w/o SORTBY     *
***********************************************************
*  Y  *   Y   *    Q_OPT_HYBRID       *      (note1)      *
***********************************************************
*  Y  *   N   *  Q_OPT_PARTIAL_RANGE  *  Q_OPT_NO_SORTER  *
***********************************************************
*  N  *   Y   *    Q_OPT_HYBRID       *     Q_OPT_NONE    *
***********************************************************
*  N  *   N   *  Q_OPT_PARTIAL_RANGE  *  Q_OPT_NO_SORTER  *
**********************************************************/
// note1: potential for filter or no sorter

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

  // sortby other field. currently no optimization
  // Q_OPT_SORTBY_OTHER
} Q_Optimize_Type;

typedef enum {
  SCORER_TYPE_NONE = 0,
  SCORER_TYPE_TERM = 1,
  SCORER_TYPE_DOC = 2,
} ScorerType;

/* A range of the sort field, with each end open or closed. */
typedef struct SortFieldBounds {
    double min;
    double max;
    bool minInclusive;
    bool maxInclusive;
} SortFieldBounds;

typedef struct QOptimizer {
    Q_Optimize_Type type;       // type of optimization

    size_t limit;               // number of required results

    bool scorerReq;             // does the query require a scorer (WITHSCORES does not count)
    ScorerType scorerType;

    const char *fieldName;      // name of sortby field
    const FieldSpec *field;     // spec of sortby field
    QueryNode *sortbyNode;      // pointer to QueryNode
    NumericFilter *nf;          // filter with required parameters
    bool asc;                   // ASC/DESC order of sortby

    // The sort keys after the numeric one, which the optimizer's heap breaks ties on the way the
    // pipeline's sorter does. Each is a SORTABLE schema field, read from the sorting vector.
    int16_t tieBreakSortIdx[SORTASCMAP_MAXFIELDS - 1];
    size_t numTieBreakKeys;
    uint64_t sortAscMap;        // ASC/DESC order of every sort key, as the arrange step holds it

    // The range of the sort field every matching document lies in, when no range on the field
    // could be lifted out of the query but the query still confines the field to one. The
    // optimizer walks this range rather than the whole field.
    bool hasImpliedBounds;
    SortFieldBounds impliedBounds;

    // Whether a document can hold several values for the sortby field (a JSON path such as
    // `$.n[*]`). A query predicate then constrains any of them, not the one the document sorts by,
    // and the field's numeric index lists such a document once per value.
    bool sortFieldMayHoldManyValues;

    QueryIterator *numIter;
    QueryIterator *root;

    RedisSearchCtx *sctx;
} QOptimizer;

#ifdef __cplusplus
extern "C" {
#endif

/* create a new QOptimizer struct */
QOptimizer *QOptimizer_New();

/* free QOptimizer struct */
void QOptimizer_Free(QOptimizer *opt);

/* parse query parameter for optimizer */
void QOptimizer_Parse(AREQ *req);

/* iterate over query nodes and find:
 * 1. does the query requires scoring
 * 2. can the sortby field be extracted for optimization
 **/
void QOptimizer_QueryNodes(QueryNode *root, QOptimizer *opt);

/* iterate over index iterator, check estimations and performs further
 * optimizations.
 * Returns REDISMODULE_OK on success, REDISMODULE_ERR on failure (allocation overflow). */
int QOptimizer_Iterators(AREQ *req, QOptimizer *opt, QueryError *status);

/* estimate the number of documents that should be checked before reaching the
 * `limit` requirement of the the query. */
size_t QOptimizer_EstimateLimit(size_t numDocs, size_t estimate, size_t limit);

/* update total results to number of returned results. */
void QOptimizer_UpdateTotalResults(AREQ *req);

/* print type of optimizer */
const char *QOptimizer_PrintType(QOptimizer *opt);

#ifdef __cplusplus
}
#endif
