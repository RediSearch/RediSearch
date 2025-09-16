#include "hybrid_callbacks.h"
#include "hybrid/hybrid_scoring.h"
#include "query_error.h"
#include <string.h>

// COMBINE callback - implements exact ParseCombine behavior from hybrid_args.c
void handleCombine(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    ArgsCursor *ac = (ArgsCursor*)value;
    QueryError *status = ctx->status;
    HybridScoringContext *combineCtx = ctx->hybridScoringCtx;
    size_t numWeights = ctx->numSubqueries;

    // Exact implementation of ParseCombine from hybrid_args.c
    // Check if a specific method is provided
    if (AC_AdvanceIfMatch(ac, "LINEAR")) {
        combineCtx->scoringType = HYBRID_SCORING_LINEAR;
    } else if (AC_AdvanceIfMatch(ac, "RRF")) {
        combineCtx->scoringType = HYBRID_SCORING_RRF;
    } else {
        combineCtx->scoringType = HYBRID_SCORING_RRF;
    }

    if (combineCtx->scoringType == HYBRID_SCORING_LINEAR) {
        // Parse LINEAR weights
        ArgsCursor weights = {0};
        int rv = AC_GetVarArgs(ac, &weights);
        if (rv != AC_OK) {
            QueryError_SetError(status, QUERY_ESYNTAX, "LINEAR requires weight arguments");
            return;
        }

        if (AC_NumArgs(&weights) != numWeights) {
            QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "LINEAR requires exactly", " %zu weight values", numWeights);
            return;
        }

        combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
        combineCtx->linearCtx.numWeights = numWeights;

        for (size_t i = 0; i < numWeights; i++) {
            double weight;
            if (AC_GetDouble(&weights, &weight, 0) != AC_OK || weight < 0) {
                QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Invalid weight value in LINEAR", " at weight #%zu", i);
                rm_free(combineCtx->linearCtx.linearWeights);
                combineCtx->linearCtx.linearWeights = NULL;
                return;
            }
            combineCtx->linearCtx.linearWeights[i] = weight;
        }
    } else if (combineCtx->scoringType == HYBRID_SCORING_RRF) {
        // Parse RRF parameters
        ArgsCursor params = {0};
        int rv = AC_GetVarArgs(ac, &params);
        if (rv == AC_OK) {
            while (!AC_IsAtEnd(&params)) {
                const char *paramName = AC_GetStringNC(&params, NULL);

                if (strcasecmp(paramName, "CONSTANT") == 0) {
                    double constant;
                    if (AC_GetDouble(&params, &constant, 0) != AC_OK || k <= 0) {
                        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid CONSTANT value in RRF");
                        return;
                    }
                    combineCtx->rrfCtx.constant = constant;
                } else if (strcasecmp(paramName, "WINDOW") == 0) {
                    long long window;
                    if (AC_GetLongLong(&params, &window, 0) != AC_OK || window <= 0) {
                        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid WINDOW value in RRF");
                        return;
                    }
                    combineCtx->rrfCtx.window = window;
                    combineCtx->rrfCtx.hasExplicitWindow = true;
                } else {
                    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unknown parameter", " `%s` in RRF", paramName);
                    return;
                }
            }
        }
    }
}
