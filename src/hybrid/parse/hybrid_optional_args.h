/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "aggregate/aggregate_plan.h"
#include "query_error.h"
#include "hybrid//hybrid_scoring.h"
#include "util/arg_parser.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MIN_HYBRID_DIALECT 2
#define DIALECT_ERROR_MSG "DIALECT is not supported in FT.HYBRID or any of its subqueries. Please check the documentation on search-default-dialect configuration."

typedef enum {
    SPECIFIED_ARG_NONE = 0,
    SPECIFIED_ARG_LIMIT = 1 << 0,
    SPECIFIED_ARG_SORTBY = 1 << 1,
    SPECIFIED_ARG_WITHCURSOR = 1 << 2,
    SPECIFIED_ARG_PARAMS = 1 << 3,
    SPECIFIED_ARG_FORMAT = 1 << 4,
    SPECIFIED_ARG_WITHSCORES = 1 << 5,
    SPECIFIED_ARG_EXPLAINSCORE = 1 << 6,
    SPECIFIED_ARG_GROUPBY = 1 << 7,
    SPECIFIED_ARG_TIMEOUT = 1 << 8,
    SPECIFIED_ARG_COMBINE = 1 << 9,
    SPECIFIED_ARG_APPLY = 1 << 10,
    SPECIFIED_ARG_LOAD = 1 << 11,
    SPECIFIED_ARG_FILTER = 1 << 12,
    SPECIFIED_ARG_NUM_SSTRING = 1 << 13,
} SpecifiedArg;

/**
 * Context structure for parsing common arguments in hybrid queries
 * Contains both aggregate plan context and hybrid-specific context
 */
typedef struct {
    QueryError *status;                     // Error reporting
    SpecifiedArg specifiedArgs;             // Bitmask of specified arguments
    HybridScoringContext *hybridScoringCtx; // Hybrid scoring context for COMBINE
    size_t numSubqueries;                   // Number of subqueries for weight validation

    AGGPlan *plan;                          // Aggregate plan for LIMIT/SORTBY
    RSSearchOptions *searchopts;            // Search options for PARAMS
    CursorConfig *cursorConfig;             // Cursor configuration
    RequestConfig *reqConfig;               // Request configuration for DIALECT/TIMEOUT
    QEFlags *reqFlags;                      // Request flags
    size_t *maxResults;                     // Maximum results
    arrayof(const char*) *prefixes;          // Prefixes for the index
} HybridParseContext;

/**
 * Parse common arguments that are shared between FT.SEARCH, FT.AGGREGATE, and FT.HYBRID
 *
 * This function handles arguments like:
 * - LIMIT offset count
 * - SORTBY field [ASC|DESC] [field [ASC|DESC] ...]
 * - WITHCURSOR [COUNT count] [MAXIDLE maxidle]
 * - PARAMS param value [param value ...]
 * - TIMEOUT timeout
 * - DIALECT dialect
 * - FORMAT format
 * - WITHSCORES
 * - EXPLAINSCORE
 * - COMBINE [RRF [K k] [WINDOW window]] | [LINEAR weight1 weight2 ...]
 *
 * @param ctx HybridParseContext containing parsing context and output parameters
 * @return 1 if arguments were handled, -1 on error, 0 if no arguments matched
 */
int HybridParseOptionalArgs(HybridParseContext *ctx, ArgsCursor *ac, bool internal);

#ifdef __cplusplus
}
#endif
