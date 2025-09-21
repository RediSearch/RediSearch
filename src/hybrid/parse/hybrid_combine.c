/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "hybrid_callbacks.h"
#include "hybrid/hybrid_scoring.h"
#include "query_error.h"
#include "util/arg_parser.h"
#include <string.h>

static void parseLinearClause(ArgsCursor *ac, HybridLinearContext *linearCtx, QueryError *status) {
  // LINEAR 4 ALPHA 0.1 BETA 0.9 ...
  //        ^

  // Variables to hold parsed values
  double alphaValue = 0.0;
  double betaValue = 0.0;

  ArgsCursor linear;
  int rc = AC_GetVarArgs(ac, &linear);
  if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments", " for LINEAR: %s", AC_Strerror(rc));
    return;
  }

  // Create ArgParser for clean argument parsing
  ArgParser *parser = ArgParser_New(&linear, "LINEAR");
  if (!parser) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create LINEAR argument parser");
    return;
  }

  // Define the required arguments
  ArgParser_AddDouble(parser, "ALPHA", "Alpha weight value", &alphaValue);
  ArgParser_AddDouble(parser, "BETA", "Beta weight value", &betaValue);

  // Parse the arguments
  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return;
  }

  // Check that both required arguments were parsed
  if (!ArgParser_WasParsed(parser, "ALPHA")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing value for ALPHA");
    ArgParser_Free(parser);
    return;
  }
  if (!ArgParser_WasParsed(parser, "BETA")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing value for BETA");
    ArgParser_Free(parser);
    return;
  }

  // Store the parsed values
  linearCtx->linearWeights[0] = alphaValue;
  linearCtx->linearWeights[1] = betaValue;

  ArgParser_Free(parser);
}

static void parseRRFClause(ArgsCursor *ac, HybridRRFContext *rrfCtx, QueryError *status) {
  // RRF 4 CONSTANT 6 WINDOW 20 ...
  //     ^

  // Variables to hold parsed values
  int constantValue = 0;
  int windowValue = 0;

  ArgsCursor rrf;
  int rc = AC_GetVarArgs(ac, &rrf);
  if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments", " for RRF: %s", AC_Strerror(rc));
    return;
  }

  // Create ArgParser for clean argument parsing
  ArgParser *parser = ArgParser_New(&rrf, "RRF");
  if (!parser) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create RRF argument parser");
    return;
  }

  // Define the optional arguments with validation
  ArgParser_AddIntV(parser, "CONSTANT", "RRF constant value (must be positive)", 
                      &constantValue, ARG_OPT_OPTIONAL,
                      ARG_OPT_DEFAULT_INT, HYBRID_DEFAULT_RRF_CONSTANT,
                      ARG_OPT_RANGE, 1LL, LLONG_MAX,
                      ARG_OPT_END);
  ArgParser_AddIntV(parser, "WINDOW", "RRF window size (must be positive)", 
                     &windowValue, ARG_OPT_OPTIONAL,
                     ARG_OPT_DEFAULT_INT, HYBRID_DEFAULT_WINDOW,
                     ARG_OPT_RANGE, 1LL, LLONG_MAX,
                     ARG_OPT_END);

  // Parse the arguments
  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return;
  }

  // Store the parsed values
  rrfCtx->constant = constantValue;
  rrfCtx->window = (size_t)windowValue;
  rrfCtx->hasExplicitWindow = ArgParser_WasParsed(parser, "WINDOW");

  ArgParser_Free(parser);
}

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
        combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
        combineCtx->linearCtx.numWeights = numWeights;
        parseLinearClause(ac, &combineCtx->linearCtx, status);
    } else if (parsedScoringType == HYBRID_SCORING_RRF) {
        parseRRFClause(ac, &combineCtx->rrfCtx, status);
    }
}
