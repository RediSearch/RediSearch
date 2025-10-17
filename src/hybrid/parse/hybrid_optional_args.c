/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "hybrid/parse/hybrid_optional_args.h"
#include "hybrid/parse/hybrid_callbacks.h"
#include "config.h"
#include "util/arg_parser.h"
#include <string.h>

/**
 * Applies optimization to skip collecting rich results when they are not needed.
 *
 * Rich results (full result structure and metadata from iterators) can be skipped when:
 * 1. No highlight/summarize step is required (QEXEC_F_SEND_HIGHLIGHT not set)
 * 2. Scores are not explicitly requested (QEXEC_F_SEND_SCORES* flags not set)
 * 3. Either this is not a search query OR the query has explicit sorting (not implicit score sorting)
 *
 * This optimization improves performance by avoiding unnecessary data collection.
 */
static void applyRichResultsOptimization(HybridParseContext *ctx) {
    if (!(*ctx->reqFlags & QEXEC_F_SEND_HIGHLIGHT) &&
        !(*ctx->reqFlags & (QEXEC_F_SEND_SCORES | QEXEC_F_SEND_SCORES_AS_FIELD)) &&
        (!(*ctx->reqFlags & QEXEC_F_IS_SEARCH) || hasQuerySortby(ctx->plan))) {
        ctx->searchopts->flags |= Search_CanSkipRichResults;
    }
}

// Main function to parse common arguments for hybrid queries
int HybridParseOptionalArgs(HybridParseContext *ctx, ArgsCursor *ac, bool internal) {

    QueryError *status = ctx->status;

    // Create argument parser
    ArgParser *parser = ArgParser_New(ac, "HybridOptionalArgs");
    if (!parser) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create argument parser");
        return REDISMODULE_ERR;
    }

    // Add all supported arguments with their callbacks
    ArgsCursor subArgs = {0};
    // LIMIT offset count - handles result limiting
    ArgParser_AddSubArgsV(parser, "LIMIT", "Limit results",
                         &subArgs, 2, 2,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleLimit, ctx,
                         ARG_OPT_END);

    // SORTBY field [ASC|DESC] [field [ASC|DESC] ...] - handles result sorting
    ArgParser_AddSubArgsV(parser, "SORTBY", "Sort results by fields",
                         &subArgs, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleSortBy, ctx,
                         ARG_OPT_END);

    // NOSORT - disables result sorting
    ArgParser_AddBitflagV(parser, "NOSORT", "Disables result sorting",
        ctx->reqFlags, sizeof(*ctx->reqFlags), QEXEC_F_NO_SORT,
        ARG_OPT_OPTIONAL, ARG_OPT_END);

    // WITHCURSOR [COUNT count] [MAXIDLE maxidle] - enables cursor-based pagination
    ArgParser_AddBitflagV(parser, "WITHCURSOR", "Enable cursor-based pagination",
                         ctx->reqFlags, sizeof(*ctx->reqFlags), QEXEC_F_IS_CURSOR,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleWithCursor, ctx,
                         ARG_OPT_END);

    // PARAMS param value [param value ...] - query parameterization
    ArgParser_AddSubArgsV(parser, "PARAMS", "Query parameters",
                         &subArgs, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleParams, ctx,
                         ARG_OPT_END);

    // TIMEOUT timeout - query timeout in milliseconds
    ArgParser_AddLongLongV(parser, "TIMEOUT", "Query timeout in milliseconds",
                      &ctx->reqConfig->queryTimeoutMS,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_DEFAULT_INT, RSGlobalConfig.requestConfigParams.queryTimeoutMS,
                      ARG_OPT_CALLBACK, handleTimeout, ctx,
                      ARG_OPT_END);

    // DIALECT dialect - query dialect version
    unsigned int defaultDialect = RSGlobalConfig.requestConfigParams.dialectVersion;
    if (defaultDialect < MIN_HYBRID_DIALECT) {
        defaultDialect = MIN_HYBRID_DIALECT;
    }
    ArgParser_AddIntV(parser, "DIALECT", "Query dialect version",
                      &ctx->reqConfig->dialectVersion, 1, 1,
                      ARG_OPT_RANGE, (long long)MIN_DIALECT_VERSION, (long long)MAX_DIALECT_VERSION,
                      ARG_OPT_DEFAULT_INT, defaultDialect,
                      ARG_OPT_CALLBACK, handleDialect, ctx,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

    // FORMAT format - output format
    const char *formatTarget = NULL;
    static const char *allowedFormats[] = {"STRING", "EXPAND", NULL};
    ArgParser_AddStringV(parser, "FORMAT", "Output format",
                         &formatTarget, 1, 1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_ALLOWED_VALUES, allowedFormats,
                         ARG_OPT_CALLBACK, handleFormat, ctx,
                         ARG_OPT_END);

    // we only support withscores when parsing commands from the coordinator
    if (internal) {
        // WITHSCORES flag - sets QEXEC_F_SEND_SCORES
        ArgParser_AddBitflagV(parser, "WITHSCORES", "Include scores in results",
                            ctx->reqFlags, sizeof(*ctx->reqFlags), QEXEC_F_SEND_SCORES,
                            ARG_OPT_CALLBACK, handleWithScores, ctx,
                            ARG_OPT_OPTIONAL, ARG_OPT_END);

        // _NUM_SSTRING flag - sets QEXEC_F_TYPED
        ArgParser_AddBitflagV(parser, "_NUM_SSTRING",
                          "Do not stringify result values. Send them in their proper types",
                          ctx->reqFlags, sizeof(*ctx->reqFlags), QEXEC_F_TYPED,
                          ARG_OPT_CALLBACK, handleNumSString, ctx,
                          ARG_OPT_OPTIONAL, ARG_OPT_END);

        ArgParser_AddSubArgsV(parser, "_INDEX_PREFIXES", "Index prefixes",
                             &subArgs, 1, -1,
                             ARG_OPT_OPTIONAL,
                             ARG_OPT_CALLBACK, handleIndexPrefixes, ctx,
                             ARG_OPT_END);
    }
    // EXPLAINSCORE flag - sets QEXEC_F_SEND_SCOREEXPLAIN
    ArgParser_AddBitflagV(parser, "EXPLAINSCORE", "Include score explanations in results",
                          ctx->reqFlags, sizeof(*ctx->reqFlags), QEXEC_F_SEND_SCOREEXPLAIN,
                          ARG_OPT_CALLBACK, handleExplainScore, ctx,
                          ARG_OPT_OPTIONAL, ARG_OPT_END);

    // Local variable to store the selected method for the lifetime of this function
    const char *methodTarget = NULL;
    static const char *allowedCombineMethods[] = {"RRF", "LINEAR", NULL};
    // COMBINE [RRF [K k] [WINDOW window]] | [LINEAR count ALPHA alpha BETA beta] - hybrid fusion method
    ArgParser_AddStringV(parser, "COMBINE", "Fusion method for hybrid search",
                         &methodTarget, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_ALLOWED_VALUES, allowedCombineMethods,
                         ARG_OPT_CALLBACK, handleCombine, ctx,
                         ARG_OPT_POSITION, 1,
                         ARG_OPT_END);

    // GROUPBY nproperties property [property ...] [REDUCE function nargs arg [arg ...] [AS alias]] [...]
    ArgParser_AddSubArgsV(parser, "GROUPBY", "Group results by properties with reducers",
                         &subArgs, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleGroupby, ctx,
                         ARG_OPT_END);

    // APPLY expression [AS alias] - apply expression to each result
    const char *applyTarget = NULL;
    ArgParser_AddStringV(parser, "APPLY", "Apply expression to each result",
                         &applyTarget, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_REPEATABLE,
                         ARG_OPT_CALLBACK, handleApply, ctx,
                         ARG_OPT_END);

    // LOAD nfields field [field ...] | LOAD * - load specific fields or all fields
    const char *loadTarget = NULL;
    ArgParser_AddStringV(parser, "LOAD", "Load specific fields or all fields",
                         &loadTarget, 1, -1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_REPEATABLE,
                         ARG_OPT_CALLBACK, handleLoad, ctx,
                         ARG_OPT_END);

    // FILTER expression - filter results by expression
    const char *filterTarget = NULL;
    ArgParser_AddStringV(parser, "FILTER", "Filter results by expression",
                         &filterTarget, 1, 1,
                         ARG_OPT_OPTIONAL,
                         ARG_OPT_CALLBACK, handleFilter, ctx,
                         ARG_OPT_END);

    // TODO: Add YIELD_SCORE_AS support for score aliasing

    // Parse the arguments
    ArgParseResult parseResult = ArgParser_Parse(parser);

    // Check for errors from callbacks
    if (QueryError_HasError(status)) {
        ArgParser_Free(parser);
        return REDISMODULE_ERR; // ARG_ERROR
    }
    if (!parseResult.success) {
        QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
        ArgParser_Free(parser);
        return REDISMODULE_ERR; // ARG_ERROR
    }

    ArgParser_Free(parser);

    if ((*(ctx->reqFlags) & QEXEC_F_SEND_SCOREEXPLAIN)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "EXPLAINSCORE is not yet supported by FT.HYBRID");
        return REDISMODULE_ERR;
    }

    // Apply optimization for skipping rich results collection when possible
    applyRichResultsOptimization(ctx);

    return parseResult.success ? REDISMODULE_OK : REDISMODULE_ERR;
}
