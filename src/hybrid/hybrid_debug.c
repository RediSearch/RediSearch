/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_debug.h"
#include "hybrid_exec.h"
#include "parse_hybrid.h"
#include "result_processor.h"
#include "rmutil/args.h"
#include "rmalloc.h"

HybridDebugParams parseHybridDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status) {
  HybridDebugParams debug_params = {0};

  // Verify DEBUG_PARAMS_COUNT exists in its expected position (second to last argument)
  if (argc < 2) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "DEBUG_PARAMS_COUNT arg is missing");
    return debug_params;
  }

  size_t n;
  const char *arg = RedisModule_StringPtrLen(argv[argc - 2], &n);
  if (!(strncasecmp(arg, "DEBUG_PARAMS_COUNT", n) == 0)) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "DEBUG_PARAMS_COUNT arg is missing or not in the expected position");
    return debug_params;
  }

  unsigned long long debug_params_count;
  // The count of debug params is the last argument in argv
  if (RedisModule_StringToULongLong(argv[argc - 1], &debug_params_count) != REDISMODULE_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid DEBUG_PARAMS_COUNT count");
    return debug_params;
  }

  debug_params.debug_params_count = debug_params_count;
  int debug_argv_count = debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  debug_params.debug_argv = argv + (argc - debug_argv_count);

  return debug_params;
}

int parseHybridDebugParams(HybridRequest_Debug *debug_req, QueryError *status) {
  HybridDebugParams *params = &debug_req->debug_params;
  RedisModuleString **debug_argv = params->debug_argv;
  unsigned long long debug_params_count = params->debug_params_count;

    // Parse component-specific timeout parameters only
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, debug_argv, debug_params_count);

  ArgsCursor searchTimeoutArgs = {0};
  ArgsCursor vsimTimeoutArgs = {0};

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

      {NULL}
  };

  ACArgSpec *errSpec = NULL;
  int rv = AC_ParseArgSpec(&ac, debugArgsSpec, &errSpec);
  if (rv != AC_OK) {
    if (rv == AC_ERR_ENOENT) {
      // Argument not recognized
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Unrecognized argument", ": %s",
                             AC_GetStringNC(&ac, NULL));
    } else if (errSpec) {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Error parsing arguments for", " %s: %s", errSpec->name, AC_Strerror(rv));
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Error parsing arguments", ": %s",
                             AC_Strerror(rv));
    }
    return REDISMODULE_ERR;
  }

  // Parse component-specific timeouts
  if (AC_IsInitialized(&searchTimeoutArgs)) {
    if (AC_GetUnsignedLongLong(&searchTimeoutArgs, &params->search_timeout_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid TIMEOUT_AFTER_N_SEARCH count");
      return REDISMODULE_ERR;
    }
    params->search_timeout_set = 1;
  }

  if (AC_IsInitialized(&vsimTimeoutArgs)) {
    if (AC_GetUnsignedLongLong(&vsimTimeoutArgs, &params->vsim_timeout_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid TIMEOUT_AFTER_N_VSIM count");
      return REDISMODULE_ERR;
    }
    params->vsim_timeout_set = 1;
  }

  // Validate that at least one component timeout parameter was provided
  if (!params->search_timeout_set && !params->vsim_timeout_set) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "At least one component timeout parameter (TIMEOUT_AFTER_N_SEARCH or TIMEOUT_AFTER_N_VSIM) must be specified");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void extractHybridTimeoutValues(const HybridDebugParams *params,
                                unsigned long long *search_timeout,
                                unsigned long long *vsim_timeout) {
  // Extract search timeout if set, otherwise 0 (no timeout)
  *search_timeout = params->search_timeout_set ? params->search_timeout_count : 0;

  // Extract vector timeout if set, otherwise 0 (no timeout)
  *vsim_timeout = params->vsim_timeout_set ? params->vsim_timeout_count : 0;
}

int applyHybridDebugToBuiltPipelines(HybridRequest_Debug *debug_req, QueryError *status) {
  HybridDebugParams *params = &debug_req->debug_params;

  // Extract timeout values from debug parameters
  unsigned long long search_timeout, vsim_timeout;
  extractHybridTimeoutValues(params, &search_timeout, &vsim_timeout);

  // Apply component-specific timeouts
  if (applyHybridTimeout(debug_req->hreq, search_timeout, vsim_timeout) != REDISMODULE_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Failed to apply timeout to built hybrid pipelines");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

int applyHybridTimeout(HybridRequest *hreq, unsigned long long search_timeout,
                       unsigned long long vsim_timeout) {
  // Apply component-specific timeouts to search and vector pipelines
  // A timeout value of 0 means no timeout for that component

  // Apply timeout to search subquery (requests[0])
  if (search_timeout > 0 && hreq->nrequests >= 1 && hreq->requests[0]) {
    PipelineAddTimeoutAfterCount(hreq->requests[0], search_timeout);
  }

  // Apply timeout to vector subquery (requests[1])
  if (vsim_timeout > 0 && hreq->nrequests >= 2 && hreq->requests[1]) {
    PipelineAddTimeoutAfterCount(hreq->requests[1], vsim_timeout);
  }

  return REDISMODULE_OK;
}

HybridRequest_Debug* HybridRequest_Debug_New(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryError *status) {
  // Parse debug parameters first
  HybridDebugParams debug_params = parseHybridDebugParamsCount(argv, argc, status);
  if (debug_params.debug_params_count == 0) {
    return NULL;
  }

  // Calculate the number of arguments for the actual hybrid command (excluding debug params)
  int debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>`
  int hybrid_argc = argc - debug_argv_count;

  // Get index name for creating search context
  if (hybrid_argc < 2) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Missing index name");
    return NULL;
  }

  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetWithUserDataFmt(status, QUERY_ENOINDEX, "No such index", " %s", indexname);
    return NULL;
  }

  // Parse the hybrid command (without debug parameters)
  HybridRequest *hreq = parseHybridCommand(ctx, argv, hybrid_argc, sctx, indexname, status);
  if (!hreq) {
    return NULL;
  }

  // Create the debug request wrapper
  HybridRequest_Debug *debug_req = rm_calloc(1, sizeof(HybridRequest_Debug));
  debug_req->hreq = hreq;
  debug_req->debug_params = debug_params;

  return debug_req;
}

void HybridRequest_Debug_Free(HybridRequest_Debug *debug_req) {
  if (!debug_req) {
    return;
  }

  if (debug_req->hreq) {
    HybridRequest_Free(debug_req->hreq);
  }

  rm_free(debug_req);
}

int DEBUG_hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 7) {  // Minimum: FT.HYBRID idx SEARCH query VSIM field vector
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = {0};

  // Create debug hybrid request
  HybridRequest_Debug *debug_req = HybridRequest_Debug_New(ctx, argv, argc, &status);
  if (!debug_req) {
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Parse debug parameters (but don't apply timeout yet)
  if (parseHybridDebugParams(debug_req, &status) != REDISMODULE_OK) {
    HybridRequest_Debug_Free(debug_req);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Build the hybrid pipeline FIRST
  HybridRequest *hreq = debug_req->hreq;
  if (HybridRequest_BuildPipeline(hreq, hreq->hybridParams) != REDISMODULE_OK) {
    HybridRequest_Debug_Free(debug_req);
    QueryError_SetError(&status, QUERY_EGENERIC, "Failed to build hybrid pipeline");
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Now apply debug parameters to the built pipelines
  if (applyHybridDebugToBuiltPipelines(debug_req, &status) != REDISMODULE_OK) {
    HybridRequest_Debug_Free(debug_req);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Execute and send results
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    HybridRequest_Debug_Free(debug_req);
    QueryError_SetWithUserDataFmt(&status, QUERY_ENOINDEX, "No such index", " %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Execute the hybrid request
  HREQ_Execute(hreq, ctx, sctx);

  // Note: hreq is freed by HREQ_Execute, but we need to free the debug wrapper
  debug_req->hreq = NULL;
  HybridRequest_Debug_Free(debug_req);

  return REDISMODULE_OK;
}
