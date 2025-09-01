#include "hybrid_callbacks.h"
#include "hybrid/hybrid_scoring.h"
#include "query_error.h"
#include <string.h>

// COMBINE callback - implements exact ParseCombine behavior from hybrid_args.c
void handleCombine(ArgParser *parser, const void *value, void *user_data) {
    HybridParseContext *ctx = (HybridParseContext*)user_data;
    const char *method = *(const char**)value;
    QueryError *status = ctx->status;
    HybridScoringContext *combineCtx = ctx->hybridScoringCtx;
    ctx->specifiedArgs |= SPECIFIED_ARG_COMBINE;
    size_t numWeights = ctx->numSubqueries;

    // Exact implementation of ParseCombine from hybrid_args.c
    // Check if a specific method is provided
    HybridScoringType parsedScoringType;
    if (strcasecmp(method, "LINEAR") == 0) {
        parsedScoringType = HYBRID_SCORING_LINEAR;
    } else if (strcasecmp(method, "RRF") == 0) {
        parsedScoringType = HYBRID_SCORING_RRF;
    } else {
        QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Unknown COMBINE method", " `%s`", method);
        return;
    }

    ArgsCursor *ac = parser->cursor;
    if (parsedScoringType == HYBRID_SCORING_LINEAR) {
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

        // Change the scoring type to the parsed type only when we actually allocate
        combineCtx->scoringType = parsedScoringType;
        combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
        combineCtx->linearCtx.numWeights = numWeights;

        for (size_t i = 0; i < numWeights; i++) {
            double weight;
            if (AC_GetDouble(&weights, &weight, 0) != AC_OK || weight < 0) {
                QueryError_SetError(status, QUERY_ESYNTAX, "Invalid weight value in LINEAR");
                rm_free(combineCtx->linearCtx.linearWeights);
                combineCtx->linearCtx.linearWeights = NULL;
                return;
            }
            combineCtx->linearCtx.linearWeights[i] = weight;
        }
    } else if (parsedScoringType) {
        // Parse RRF parameters
        ArgsCursor params = {0};
        int rv = AC_GetVarArgs(ac, &params);
        combineCtx->scoringType = parsedScoringType;
        if (rv == AC_OK) {
            while (!AC_IsAtEnd(&params)) {
                const char *paramName = AC_GetStringNC(&params, NULL);

                if (strcasecmp(paramName, "K") == 0) {
                    double k;
                    if (AC_GetDouble(&params, &k, 0) != AC_OK || k <= 0) {
                        QueryError_SetError(status, QUERY_ESYNTAX, "Invalid K value in RRF");
                        return;
                    }
                    combineCtx->rrfCtx.k = k;
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
