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

static bool parseLinearClause(ArgsCursor *ac, HybridLinearContext *linearCtx, QueryError *status) {
  // LINEAR 4 ALPHA 0.1 BETA 0.9 ...
  //        ^

  // Variables to hold parsed values
  double alphaValue = 0.0;
  double betaValue = 0.0;

  
  unsigned int count = 0; 
  int rc = AC_GetUnsigned(ac, &count, 0);
  if (rc == AC_ERR_NOARG) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing LINEAR argument count");
    return false;
  } else if (rc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid LINEAR argument count");
    return false;
  }

  ArgsCursor linear;
  rc = AC_GetSlice(ac, &linear, count);
  if (rc == AC_ERR_NOARG) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Not enough arguments in LINEAR", ", specified %u but only %u provided", count, AC_NumRemaining(ac));
    return false;
  } else if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Bad arguments in LINEAR", ": %s", AC_Strerror(rc));
    return false;
  }

  // Create ArgParser for clean argument parsing
  ArgParser *parser = ArgParser_New(&linear, "LINEAR");
  if (!parser) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to create LINEAR argument parser");
    return false;
  }

  // Define the required arguments
  ArgParser_AddDouble(parser, "ALPHA", "Alpha weight value", &alphaValue);
  ArgParser_AddDouble(parser, "BETA", "Beta weight value", &betaValue);

  // Parse the arguments
  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return false;
  }

  // Check that both required arguments were parsed
  if (!ArgParser_WasParsed(parser, "ALPHA")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing value for ALPHA");
    ArgParser_Free(parser);
    return false;
  }
  if (!ArgParser_WasParsed(parser, "BETA")) {
    QueryError_SetError(status, QUERY_ESYNTAX, "Missing value for BETA");
    ArgParser_Free(parser);
    return false;
  }

  // Store the parsed values
  linearCtx->linearWeights[0] = alphaValue;
  linearCtx->linearWeights[1] = betaValue;

  ArgParser_Free(parser);
  return true;
}

static bool parseRRFArgs(ArgsCursor *ac, double *constant, int *window, bool *hasExplicitWindow, QueryError *status) {
  *hasExplicitWindow = false;
  ArgsCursor rrf;
  int rc = AC_GetVarArgs(ac, &rrf);
  if (rc == AC_ERR_NOARG) {
    // Apparently we allow no arguments for RRF
    *constant = HYBRID_DEFAULT_RRF_CONSTANT;
    *window = HYBRID_DEFAULT_WINDOW;
    return true;
  } else if (rc == AC_ERR_PARSE) {
    // We also allow a different keyword after it, e.g LIMIT
    // This means if it a different keyword the error will be more ambiguous
    *constant = HYBRID_DEFAULT_RRF_CONSTANT;
    *window = HYBRID_DEFAULT_WINDOW;
    return true;
  } else if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments", " for RRF: %s", AC_Strerror(rc));
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


static bool parseRRFClause(ArgsCursor *ac, HybridRRFContext *rrfCtx, QueryError *status) {
  // RRF 4 CONSTANT 6 WINDOW 20 ...
  //     ^
  // RRF LIMIT

  // Variables to hold parsed values
  double constantValue = HYBRID_DEFAULT_RRF_CONSTANT;
  int windowValue = HYBRID_DEFAULT_WINDOW;
  bool hasExplicitWindow = false;

  if (!parseRRFArgs(ac, &constantValue, &windowValue, &hasExplicitWindow, status)) {
    return REDISMODULE_CTX_FLAGS_REPLICA_IS_CONNECTING;
  }
  
  // Store the parsed values
  rrfCtx->constant = constantValue;
  rrfCtx->window = windowValue;
  rrfCtx->hasExplicitWindow = hasExplicitWindow;
  return true;
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
  bool parsed = false;
  if (parsedScoringType == HYBRID_SCORING_LINEAR) {
    combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    combineCtx->linearCtx.numWeights = numWeights;
    parsed = parseLinearClause(ac, &combineCtx->linearCtx, status);
  } else if (parsedScoringType == HYBRID_SCORING_RRF) {
    parsed = parseRRFClause(ac, &combineCtx->rrfCtx, status);
  }
  if (!parsed) {
    return;
  }

  ACArgSpec remainingSpec = {.name = "YIELD_SCORE_AS", .type = AC_ARGTYPE_STRING, .target = &ctx->searchopts->scoreAlias};
  while (!AC_IsAtEnd(ac)) { 
    int rv = AC_ParseArgSpec(ac, &remainingSpec, NULL);
    if (rv == AC_OK) {
      continue;
    }
    else if (rv == AC_ERR_ENOENT) {
      // Could be a keyword like LOAD or something like that, can't fail here.
      break;
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ESYNTAX, "Bad arguments after COMBINE", ": %s", AC_Strerror(rv));
      return;
    }
  }
}
