#include "hybrid/parse/hybrid_optional_args.h"
#include "hybrid/parse/hybrid_callbacks.h"
#include "config.h"
#include "util/arg_parser.h"
#include <string.h>

// Main function to parse common arguments for hybrid queries
int HybridParseOptionalArgs(HybridParseContext *ctx, ArgsCursor *ac) {

    QueryError *status = ctx->status;

    // Create argument parser
    ArgParser *parser = ArgParser_New(ac, "HybridOptionalArgs");
    if (!parser) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create argument parser");
        return -1;
    }

    // Add all supported arguments with their callbacks
    
    // LIMIT offset count - handles result limiting
    ArgParser_AddSubArgsV(parser, "LIMIT", "Limit results",
                         NULL, 2, 2,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleLimit, ctx,
                         ARG_OPT_END);

    // SORTBY field [ASC|DESC] [field [ASC|DESC] ...] - handles result sorting
    ArgParser_AddSubArgsV(parser, "SORTBY", "Sort results by fields",
                         NULL, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleSortBy, ctx,
                         ARG_OPT_END);

    // WITHCURSOR [COUNT count] [MAXIDLE maxidle] - enables cursor-based pagination
    ArgParser_AddBitflagV(parser, "WITHCURSOR", "Enable cursor-based pagination",
                         ctx->reqflags, sizeof(*ctx->reqflags), QEXEC_F_IS_CURSOR,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleWithCursor, ctx,
                         ARG_OPT_END);

    // PARAMS param value [param value ...] - query parameterization
    ArgParser_AddSubArgsV(parser, "PARAMS", "Query parameters",
                         NULL, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleParams, ctx,
                         ARG_OPT_END);

    // TIMEOUT timeout - query timeout in milliseconds
    // TIMEOUT timeout - query timeout in milliseconds
    // Note: We need to use a temporary int and copy to long long later
    // since ArgParser_AddIntV expects int*, not long long*
    static int tempTimeout = 0;
    ArgParser_AddIntV(parser, "TIMEOUT", "Query timeout in milliseconds",
                      &tempTimeout, sizeof(tempTimeout),
                      ARG_OPT_OPTIONAL, ARG_OPT_END);

    // DIALECT dialect - query dialect version
    ArgParser_AddIntV(parser, "DIALECT", "Query dialect version",
                      NULL, 1, 1,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_CALLBACK, handleDialect, ctx,
                      ARG_OPT_END);

    // FORMAT format - output format
    ArgParser_AddSubArgsV(parser, "FORMAT", "Output format",
                         NULL, 1, 1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleFormat, ctx,
                         ARG_OPT_END);

    // WITHSCORES flag - sets QEXEC_F_SEND_SCORES
    ArgParser_AddBitflagV(parser, "WITHSCORES", "Include scores in results",
                          ctx->reqflags, sizeof(*ctx->reqflags), QEXEC_F_SEND_SCORES,
                          ARG_OPT_OPTIONAL, ARG_OPT_END);

    // EXPLAINSCORE flag - sets QEXEC_F_SEND_SCOREEXPLAIN
    ArgParser_AddBitflagV(parser, "EXPLAINSCORE", "Include score explanations in results",
                          ctx->reqflags, sizeof(*ctx->reqflags), QEXEC_F_SEND_SCOREEXPLAIN,
                          ARG_OPT_OPTIONAL, ARG_OPT_END);

    // COMBINE [RRF [K k] [WINDOW window]] | [LINEAR weight1 weight2 ...] - hybrid fusion method
    ArgParser_AddSubArgsV(parser, "COMBINE", "Fusion method for hybrid search",
                         NULL, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleCombine, ctx,
                         ARG_OPT_END);

    // TODO: Add YIELD_SCORE_AS support for score aliasing

    // Parse the arguments
    ArgParseResult parseResult = ArgParser_Parse(parser);
    
    // Check for errors from callbacks
    if (QueryError_HasError(status)) {
        ArgParser_Free(parser);
        return -1; // ARG_ERROR
    }

    ArgParser_Free(parser);

    // Copy temporary timeout value to actual config
    if (tempTimeout > 0) {
        ctx->reqConfig->requestConfigParams.queryTimeoutMS = tempTimeout;
    }

    // Handle dialect-specific validation (replicated from original)
    if (ctx->dialectSpecified && ctx->reqConfig->requestConfigParams.dialectVersion < APIVERSION_RETURN_MULTI_CMP_FIRST &&
        (*(ctx->reqflags) & QEXEC_F_SEND_SCOREEXPLAIN)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "EXPLAINSCORE is not supported in this dialect version");
        return -1;
    }

    return parseResult.success ? 1 : -1;
}

