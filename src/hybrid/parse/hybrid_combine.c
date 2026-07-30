/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <string.h>
#include <limits.h>
#include <stdbool.h>
#include <strings.h>

#include "hybrid_callbacks.h"
#include "hybrid/hybrid_scoring.h"
#include "query_error_ffi.h"
#include "util/arg_parser.h"
#include "hybrid/parse/hybrid_optional_args.h"
#include "query_error.h"
#include "query_flags.h"
#include "rmalloc.h"
#include "rmutil/args.h"
#include "search_options.h"

static inline bool getVarArgsForClause(ArgsCursor* ac, ArgsCursor* target, const char *clause, QueryError* status) {
  unsigned int count = 0;
  int rc = AC_GetUnsigned(ac, &count, 0);
  if (rc == AC_ERR_NOARG) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing %s argument count", clause);
    return false;
  } else if (rc != AC_OK) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid %s argument count, error: %s", clause, AC_Strerror(rc));
    return false;
  } else if (count % 2 != 0) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "%s expects pairs of key value arguments, argument count must be an even number", clause);
    return false;
  }

  rc = AC_GetSlice(ac, target, count);
  if (rc == AC_ERR_NOARG) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Not enough arguments", " in %s, specified %u but provided only %u", clause, count, AC_NumRemaining(ac));
    return false;
  } else if (rc != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Bad arguments", " in %s: %s", clause, AC_Strerror(rc));
    return false;
  }
  return true;
}

static void parseLinearClause(ArgsCursor *ac, HybridLinearContext *linearCtx,
                              RSSearchOptions *searchOpts, QueryError *status) {
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
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to create LINEAR argument parser");
    return;
  }

  // Define the required arguments
  ArgParser_AddDouble(parser, "ALPHA", "Alpha weight value", &alphaValue);
  ArgParser_AddDouble(parser, "BETA", "Beta weight value", &betaValue);
  // Legacy (counted) form of YIELD_SCORE_AS: accepted inside the method argument
  // count for backward compatibility. The positional form (after the method
  // block) is handled in handleCombine.
  ArgParser_AddString(parser, "YIELD_SCORE_AS", "Alias for the combined score", &searchOpts->scoreAlias);

  int windowValue = HYBRID_DEFAULT_WINDOW;
  ArgParser_AddIntV(parser, "WINDOW", "LINEAR window size (must be positive)",
                    &windowValue, ARG_OPT_OPTIONAL,
                    ARG_OPT_DEFAULT_INT, HYBRID_DEFAULT_WINDOW,
                    ARG_OPT_RANGE, 1LL, LLONG_MAX,
                    ARG_OPT_END);

  // Parse the arguments
  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return;
  }

  bool hasAlpha = ArgParser_WasParsed(parser, "ALPHA");
  bool hasBeta = ArgParser_WasParsed(parser, "BETA");
  if (hasAlpha ^ hasBeta) { // all or none of ALPHA and BETA must be present
    if (hasAlpha) {
      QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Missing value for BETA");
    } else {
      QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Missing value for ALPHA");
    }
    ArgParser_Free(parser);
    return;
  }

  // Store the parsed values
  linearCtx->linearWeights[0] = hasAlpha ? alphaValue : HYBRID_DEFAULT_LINEAR_ALPHA;
  linearCtx->linearWeights[1] = hasBeta ? betaValue : HYBRID_DEFAULT_LINEAR_BETA;
  linearCtx->window = windowValue;

  ArgParser_Free(parser);
}

static bool parseRRFArgs(ArgsCursor *ac, double *constant, int *window, bool *hasExplicitWindow,
                          RSSearchOptions *searchOpts, QueryError *status) {
  *hasExplicitWindow = false;
  ArgsCursor rrf = {0};
  if (!getVarArgsForClause(ac, &rrf, "RRF", status)) {
    return false;
  }

  ArgParser *parser = ArgParser_New(&rrf, "RRF");
  if (!parser) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to create RRF argument parser");
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
  // Legacy (counted) form of YIELD_SCORE_AS: accepted inside the method argument
  // count for backward compatibility. The positional form (after the method
  // block) is handled in handleCombine.
  ArgParser_AddString(parser, "YIELD_SCORE_AS", "Alias for the combined score", &searchOpts->scoreAlias);

  // Parse the arguments
  ArgParseResult result = ArgParser_Parse(parser);
  if (!result.success) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return false;
  }
  *hasExplicitWindow = ArgParser_WasParsed(parser, "WINDOW");
  ArgParser_Free(parser);
  return true;
}


// Parse the positional YIELD_SCORE_AS <alias> clause that may follow a COMBINE
// method. The alias is applied to the merged (tail) score, so it is stored in
// the tail search options rather than in either subquery.
//   COMBINE RRF|LINEAR ... YIELD_SCORE_AS <alias>
//                                         ^
static void parseCombineYieldScoreClause(ArgsCursor *ac, HybridParseContext *ctx, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing argument value for YIELD_SCORE_AS");
    return;
  }
  if (AC_GetString(ac, &ctx->searchopts->scoreAlias, NULL, 0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_BAD_VAL, "Invalid YIELD_SCORE_AS value");
    return;
  }
  // Mirror the SEARCH subquery path so the combined score is emitted as a field
  // and applyRichResultsOptimization does not skip collecting it.
  *ctx->reqFlags |= QEXEC_F_SEND_SCORES_AS_FIELD;
}

static void parseRRFClause(ArgsCursor *ac, HybridRRFContext *rrfCtx,
                            RSSearchOptions *searchOpts, QueryError *status) {
  // RRF 4 CONSTANT 6 WINDOW 20 ...
  //     ^
  // RRF LIMIT

  // Variables to hold parsed values
  double constantValue = HYBRID_DEFAULT_RRF_CONSTANT;
  int windowValue = HYBRID_DEFAULT_WINDOW;
  bool hasExplicitWindow = false;

  if (!parseRRFArgs(ac, &constantValue, &windowValue, &hasExplicitWindow, searchOpts, status)) {
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
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Unknown COMBINE method", " `%s`", method);
    return;
  }

  combineCtx->scoringType = parsedScoringType;
  ArgsCursor *ac = parser->cursor;
  // The method ArgParser writes the legacy "counted" YIELD_SCORE_AS (form A)
  // directly into searchopts->scoreAlias. Snapshot it so we can tell afterwards
  // whether form A consumed the alias, and reject a second (positional) one.
  const char *aliasBefore = ctx->searchopts->scoreAlias;
  if (parsedScoringType == HYBRID_SCORING_LINEAR) {
    combineCtx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    combineCtx->linearCtx.numWeights = numWeights;
    parseLinearClause(ac, &combineCtx->linearCtx, ctx->searchopts, status);
  } else if (parsedScoringType == HYBRID_SCORING_RRF) {
    parseRRFClause(ac, &combineCtx->rrfCtx, ctx->searchopts, status);
  }
  if (QueryError_HasError(status)) {
    return;
  }
  bool yieldParsedInBlock = (ctx->searchopts->scoreAlias != aliasBefore);

  if (AC_AdvanceIfMatch(ac, "YIELD_SCORE_AS")) {
    // Positional form B. Reject if the alias was already given in the counted
    // form A (specified more than once).
    if (yieldParsedInBlock) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                          "YIELD_SCORE_AS specified more than once");
      return;
    }
    parseCombineYieldScoreClause(ac, ctx, status);
    if (QueryError_HasError(status)) {
      return;
    }
  } else if (yieldParsedInBlock) {
    // Form A: the alias was written to searchopts->scoreAlias by the method
    // ArgParser. Mirror the SEARCH subquery path so the combined score is
    // emitted as a field and applyRichResultsOptimization does not skip it.
    *ctx->reqFlags |= QEXEC_F_SEND_SCORES_AS_FIELD;
  }
}
