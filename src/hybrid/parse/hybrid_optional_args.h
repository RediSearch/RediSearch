#pragma once

#include "aggregate/aggregate_plan.h"
#include "query_error.h"
#include "hybrid//hybrid_scoring.h"
#include "util/arg_parser.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Context structure for parsing common arguments in hybrid queries
 * Contains both aggregate plan context and hybrid-specific context
 */
typedef struct {
    QueryError *status;                     // Error reporting
    bool hadAdditionalArgs;                 // Did encounter additional arguments related to the tail
    bool dialectSpecified;                  // Whether DIALECT was explicitly set
    HybridScoringContext *hybridScoringCtx; // Hybrid scoring context for COMBINE
    size_t numSubqueries;                   // Number of subqueries for weight validation

    AGGPlan *plan;                          // Aggregate plan for LIMIT/SORTBY
    uint32_t *reqflags;                     // Request flags
    RSSearchOptions *searchopts;            // Search options for PARAMS
    CursorConfig *cursorConfig;             // Cursor configuration
    RSConfig *reqConfig;                    // Request configuration for DIALECT/TIMEOUT
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
