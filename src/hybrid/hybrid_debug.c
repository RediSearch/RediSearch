/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_debug.h"

#include <stdbool.h>
#include <stddef.h>

#include "debug_commands.h"
#include "config.h"
#include "hybrid_exec.h"
#include "hybrid_request.h"
#include "result_processor.h"
#include "rmutil/args.h"
#include "pipeline/pipeline.h"
#include "query_error_ffi.h"
#include "rmutil/rm_assert.h"

HybridDebugParams parseHybridDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status) {
  HybridDebugParams debug_params = {0};

  unsigned long long debug_params_count = 0;
  if (parseDebugParamsCount(argv, argc, status, &debug_params_count) != 0) {
      return debug_params;
  }

  debug_params.debug_params_count = debug_params_count;
  int debug_argv_count = debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  debug_params.debug_argv = argv + (argc - debug_argv_count);

  return debug_params;
}

int parseHybridDebugParams(HybridDebugParams *params, QueryError *status) {
  RedisModuleString **debug_argv = params->debug_argv;
  unsigned long long debug_params_count = params->debug_params_count;

    // Parse component-specific timeout parameters only
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, debug_argv, debug_params_count);

  ArgsCursor searchTimeoutArgs = {0};
  ArgsCursor vsimTimeoutArgs = {0};
  ArgsCursor tailTimeoutArgs = {0};

  ACArgSpec debugArgsSpec[] = {
      // Component-specific timeouts
      {.name = "TIMEOUT_AFTER_N_SEARCH",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &searchTimeoutArgs,
       .slicelen = 1},

      {.name = "TIMEOUT_AFTER_N_VSIM",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &vsimTimeoutArgs,
       .slicelen = 1},

      {.name = "TIMEOUT_AFTER_N_TAIL",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &tailTimeoutArgs,
       .slicelen = 1},

      {NULL}
  };

  ACArgSpec *errSpec = NULL;
  int rv = AC_ParseArgSpec(&ac, debugArgsSpec, &errSpec);
  if (rv != AC_OK) {
    if (rv == AC_ERR_ENOENT) {
      // Argument not recognized
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Unrecognized argument", ": %s",
                             AC_GetStringNC(&ac, NULL));
    } else if (errSpec) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Error parsing arguments for", " %s: %s", errSpec->name, AC_Strerror(rv));
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Error parsing arguments", ": %s",
                             AC_Strerror(rv));
    }
    return REDISMODULE_ERR;
  }

  // Parse component-specific timeouts
  if (AC_IsInitialized(&searchTimeoutArgs)) {
    if (AC_GetUnsignedLongLong(&searchTimeoutArgs, &params->search_timeout_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid TIMEOUT_AFTER_N_SEARCH count");
      return REDISMODULE_ERR;
    }
    params->search_timeout_set = 1;
  }

  if (AC_IsInitialized(&vsimTimeoutArgs)) {
    if (AC_GetUnsignedLongLong(&vsimTimeoutArgs, &params->vsim_timeout_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid TIMEOUT_AFTER_N_VSIM count");
      return REDISMODULE_ERR;
    }
    params->vsim_timeout_set = 1;
  }

  if (AC_IsInitialized(&tailTimeoutArgs)) {
    if (AC_GetUnsignedLongLong(&tailTimeoutArgs, &params->tail_timeout_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid TIMEOUT_AFTER_N_TAIL count");
      return REDISMODULE_ERR;
    }
    params->tail_timeout_set = 1;
  }

  // Validate that at least one component timeout parameter was provided
  if (!params->search_timeout_set && !params->vsim_timeout_set && !params->tail_timeout_set) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "At least one component timeout parameter (TIMEOUT_AFTER_N_SEARCH, TIMEOUT_AFTER_N_VSIM, or TIMEOUT_AFTER_N_TAIL) must be specified");
    return REDISMODULE_ERR;
  }

  if (RSGlobalConfig.timeoutConfigParams.timeoutPolicy == TimeoutPolicy_ReturnStrict) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "FT.HYBRID debug timeout is not supported with ON_TIMEOUT RETURN-STRICT");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

int applyHybridDebugTimeout(HybridRequest *hreq, const HybridDebugParams *params) {
  // Apply component-specific timeouts to search, vector, and tail pipelines
  // A timeout value of 0 means no timeout for that component

  RS_ASSERT(hreq->nrequests >= 2);
  AREQ *search_req = hreq->requests[0];
  AREQ *vector_req = hreq->requests[1];

  // Apply timeout to search subquery
  if (params->search_timeout_count > 0) {
    PipelineAddTimeoutAfterCount(AREQ_QueryProcessingCtx(search_req), AREQ_SearchCtx(search_req), params->search_timeout_count);
  }

  // Apply timeout to vector subquery
  if (params->vsim_timeout_count > 0) {
    PipelineAddTimeoutAfterCount(AREQ_QueryProcessingCtx(vector_req), AREQ_SearchCtx(vector_req), params->vsim_timeout_count);
  }

  // Apply timeout to the tail pipeline, but only when it was built: internal shard
  // commands build only the depletion pipeline, leaving the tail empty (endProc NULL).
  if (params->tail_timeout_count > 0 && hreq->tailPipeline && hreq->tailPipeline->qctx.endProc) {
    PipelineAddTimeoutAfterCount(&hreq->tailPipeline->qctx, hreq->sctx, params->tail_timeout_count);
  }

  return REDISMODULE_OK;
}

int DEBUG_hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               bool internal) {
  if (argc < 7) {  // Minimum: FT.HYBRID idx SEARCH query VSIM field vector
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = QueryError_Default();
  HybridDebugParams debugParams = parseHybridDebugParamsCount(argv, argc, &status);
  if (QueryError_HasError(&status)) {
    return QueryError_ReplyAndClear(ctx, &status);
  }
  if (parseHybridDebugParams(&debugParams, &status) != REDISMODULE_OK) {
    return QueryError_ReplyAndClear(ctx, &status);
  }

  int hybridArgc = argc - debugParams.debug_params_count - 2;
  return hybridCommandHandler(ctx, argv, hybridArgc, internal, EXEC_NO_FLAGS, &debugParams);
}
