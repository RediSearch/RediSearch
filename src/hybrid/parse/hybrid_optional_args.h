#pragma once

#include "aggregate/aggregate_plan.h"
#include "query_error.h"
#include "hybrid//hybrid_scoring.h"
#include "util/arg_parser.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    SPECIFIED_ARG_NONE,
    SPECIFIED_ARG_LIMIT,
    SPECIFIED_ARG_SORTBY,
    SPECIFIED_ARG_WITHCURSOR,
    SPECIFIED_ARG_PARAMS,
    SPECIFIED_ARG_DIALECT,
    SPECIFIED_ARG_FORMAT,
    SPECIFIED_ARG_WITHSCORES,
    SPECIFIED_ARG_EXPLAINSCORE,
    SPECIFIED_ARG_COMBINE,
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
    uint32_t *reqflags;                     // Request flags
    RSSearchOptions *searchopts;            // Search options for PARAMS
    CursorConfig *cursorConfig;             // Cursor configuration
    RequestConfig *reqConfig;               // Request configuration for DIALECT/TIMEOUT
    QEFlags *reqFlags;                      // Request flags
    size_t *maxResults;                     // Maximum results
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
int HybridParseOptionalArgs(HybridParseContext *ctx, ArgsCursor *ac);

#ifdef __cplusplus
}
#endif
