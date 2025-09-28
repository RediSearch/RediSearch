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

static inline bool getVarArgsForClause(ArgsCursor* ac, ArgsCursor* target, const char *clause, QueryError* status) {
  unsigned int count = 0; 
  int rc = AC_GetUnsigned(ac, &count, 0);
  if (rc == AC_ERR_NOARG) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_EPARSEARGS, "Missing %s argument count", clause);
    return false;
  } else if (rc != AC_OK) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_EPARSEARGS, "Invalid %s argument count", clause);
    return false;
  }

  rc = AC_GetSlice(ac, target, count);
  if (rc == AC_ERRNot enough arguments_NOARG) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Not enough arguments", "in %s, specified %u but provided only %u", clause, count, AC_NumRemaining(ac));
    return false;
  } else if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Bad arguments", "in %s: %s", clause, AC_Strerror(rc));
    return false;
  }
  return true;
}

static void parseLinearClause(ArgsCursor *ac, HybridLinearContext *linearCtx, QueryError *status) {
  // LINEAR 4 ALPHA 0.1 BETA 0.9 ...
  //        ^

  // Variables to hold parsed values
  double alphaValue = 0.0;
  double betaValue = 0.0;

  ArgsCursor linear = {0};
  if (!getVarArgsForClause(ac, &linear, "LINEAR", status)) {
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

static int parseRRFArgs(ArgsCursor *ac, double *constant, int *window, bool *hasExplicitWindow, QueryError *status) {
  *hasExplicitWindow = false;
  ArgsCursor rrf = {0};
  if (!getVarArgsForClause(ac, &rrf, "RRF", status)) {
    return false;
  }

  ArgParser *parser = ArgParser_New(&rrf, "RRF");
  if (!parser) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create RRF argument parser");
    return false;
  }

  double defaultConstant = HYBRID_DEFAULT_RRF_CONSTANT;
  // Define the optional arguments with validation
  ArgParser_AddDoubleV(parser, "CONSTANT", "RRF constant value (must be positive)", 
                       constant, ARG_OPT_OPTIONAL,
                       ARG_OPT_DEFAULT_DOUBLE, defaultConstant,
                       ARG_OPT_END);
  ArgParser_AddIntV(parser, "WINDOW", "RRF window size (must be positive)", 
                    window, ARG_OPT_OPTIONAL,
                    ARG_OPT_DEFAULT_INT, HYBRID_DEFAULT_WINDOW,
                    ARG_OPT_RANGE, 1LL, LLONG_MAX,
                    ARG_OPT_END);

  // Parse the arguments
  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return false;
  }
  *hasExplicitWindow = ArgParser_WasParsed(parser, "WINDOW");
  ArgParser_Free(parser);
  return true;
}


static void parseRRFClause(ArgsCursor *ac, HybridRRFContext *rrfCtx, QueryError *status) {
  // RRF 4 CONSTANT 6 WINDOW 20 ...
  //     ^
  // RRF LIMIT

  // Variables to hold parsed values
  double constantValue = HYBRID_DEFAULT_RRF_CONSTANT;
  int windowValue = HYBRID_DEFAULT_WINDOW;
  bool hasExplicitWindow = false;

  if (!parseRRFArgs(ac, &constantValue, &windowValue, &hasExplicitWindow, status)) {
    return;
  }
  
  // Store the parsed values
  rrfCtx->constant = constantValue;
  rrfCtx->window = windowValue;
  rrfCtx->hasExplicitWindow = hasExplicitWindow;

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

  combineCtx->scoringType = parsedScoringType;
  ArgsCursor *ac = parser->cursor;
  if (parsedScoringType == HYBRID_SCORING_LINEAR) {
    combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    combineCtx->linearCtx.numWeights = numWeights;
    parseLinearClause(ac, &combineCtx->linearCtx, status);
  } else if (parsedScoringType == HYBRID_SCORING_RRF) {
    parseRRFClause(ac, &combineCtx->rrfCtx, status);
  }
}
