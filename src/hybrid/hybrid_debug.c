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
#include "hybrid_request.h"
#include "parse_hybrid.h"
#include "result_processor.h"
#include "rmutil/args.h"
#include "rmalloc.h"

// Debug parameters structure for hybrid queries
typedef struct {
  RedisModuleString **debug_argv;
  unsigned long long debug_params_count;

  // Component-specific timeouts only
  unsigned long long search_timeout_count;
  unsigned long long vsim_timeout_count;
  unsigned long long tail_timeout_count;
  int search_timeout_set;
  int vsim_timeout_set;
  int tail_timeout_set;
} HybridDebugParams;

// Wrapper structure for hybrid request with debug capabilities
typedef struct {
  HybridRequest *hreq;                     // Base hybrid request
  HybridDebugParams debug_params;          // Debug parameters
} HybridRequest_Debug;

static HybridDebugParams parseHybridDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status) {
  HybridDebugParams debug_params = {0};

  // Verify DEBUG_PARAMS_COUNT exists in its expected position (second to last argument)
  if (argc < 2) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "DEBUG_PARAMS_COUNT arg is missing");
    return debug_params;
  }

  size_t n;
  const char *arg = RedisModule_StringPtrLen(argv[argc - 2], &n);
  if (!(strncasecmp(arg, "DEBUG_PARAMS_COUNT", n) == 0)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "DEBUG_PARAMS_COUNT arg is missing or not in the expected position");
    return debug_params;
  }

  unsigned long long debug_params_count;
  // The count of debug params is the last argument in argv
  if (RedisModule_StringToULongLong(argv[argc - 1], &debug_params_count) != REDISMODULE_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid DEBUG_PARAMS_COUNT count");
    return debug_params;
  }

  debug_params.debug_params_count = debug_params_count;
  int debug_argv_count = debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>` strings
  debug_params.debug_argv = argv + (argc - debug_argv_count);

  return debug_params;
}

static int parseHybridDebugParams(HybridRequest_Debug *debug_req, QueryError *status) {
  HybridDebugParams *params = &debug_req->debug_params;
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

  return REDISMODULE_OK;
}

static int applyHybridTimeout(HybridRequest *hreq, const HybridDebugParams *params) {
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

  // Apply timeout to tail pipeline
  if (params->tail_timeout_count > 0 && hreq->tailPipeline) {
    PipelineAddTimeoutAfterCount(&hreq->tailPipeline->qctx, hreq->sctx, params->tail_timeout_count);
  }

  return REDISMODULE_OK;
}

static int applyHybridDebugToBuiltPipelines(HybridRequest_Debug *debug_req, QueryError *status) {
  // Apply component-specific timeouts
  if (applyHybridTimeout(debug_req->hreq, &debug_req->debug_params) != REDISMODULE_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to apply timeout to built hybrid pipelines");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

static HybridRequest_Debug* HybridRequest_Debug_New(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                                     RedisSearchCtx *sctx, const char *indexname, QueryError *status) {
  // Parse debug parameters first
  HybridDebugParams debug_params = parseHybridDebugParamsCount(argv, argc, status);
  if (debug_params.debug_params_count == 0) {
    return NULL;
  }

  // Calculate the number of arguments for the actual hybrid command (excluding debug params)
  int debug_argv_count = debug_params.debug_params_count + 2;  // account for `DEBUG_PARAMS_COUNT` `<count>`
  int hybrid_argc = argc - debug_argv_count;

  HybridRequest *hreq = MakeDefaultHybridRequest(sctx);
  ArgsCursor ac = {0};
  HybridRequest_InitArgsCursor(hreq, &ac, argv, hybrid_argc);

  HybridPipelineParams hybridParams = {0};  // Stack allocation
  ParseHybridCommandCtx cmd = {0};
  cmd.search = hreq->requests[SEARCH_INDEX];
  cmd.vector = hreq->requests[VECTOR_INDEX];
  cmd.cursorConfig = &hreq->cursorConfig;
  cmd.hybridParams = &hybridParams;
  cmd.tailPlan = &hreq->tailPipeline->ap;
  cmd.reqConfig = &hreq->reqConfig;
  cmd.coordDispatchTime = &hreq->profileClocks.coordDispatchTime;

  int rc = parseHybridCommand(ctx, &ac, sctx, &cmd, status, false, EXEC_NO_FLAGS);
  if (rc != REDISMODULE_OK) {
    if (hybridParams.scoringCtx) {
      HybridScoringContext_Free(hybridParams.scoringCtx);
    }
    HybridRequest_DecrRef(hreq);
    return NULL;
  }

  SearchCtx_UpdateTime(hreq->sctx, hreq->reqConfig.queryTimeoutMS);
  for (int i = 0; i < hreq->nrequests; i++) {
    AREQ *subquery = hreq->requests[i];
    SearchCtx_UpdateTime(AREQ_SearchCtx(subquery), hreq->reqConfig.queryTimeoutMS);
  }

  // Set request flags from hybridParams
  hreq->reqflags = hybridParams.aggregationParams.common.reqflags;
  if (HybridRequest_BuildPipeline(hreq, &hybridParams, false) != REDISMODULE_OK) {
    if (hybridParams.scoringCtx) {
      HybridScoringContext_Free(hybridParams.scoringCtx);
    }
    HybridRequest_DecrRef(hreq);
    QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Failed to build hybrid pipeline");
    return NULL;
  }

  HybridRequest_Debug *debug_req = rm_calloc(1, sizeof(HybridRequest_Debug));
  debug_req->hreq = hreq;
  debug_req->debug_params = debug_params;

  return debug_req;
}

static void HybridRequest_Debug_Free(HybridRequest_Debug *debug_req) {
  if (!debug_req) {
    return;
  }

  if (debug_req->hreq) {
    HybridRequest_DecrRef(debug_req->hreq);
  }

  rm_free(debug_req);
}

int DEBUG_hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 7) {  // Minimum: FT.HYBRID idx SEARCH query VSIM field vector
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = QueryError_Default();

  // Get index name and create search context (same pattern as regular hybridCommandHandler)
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, "Index not found", ": %s", indexname);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Create debug hybrid request using the same sctx
  HybridRequest_Debug *debug_req = HybridRequest_Debug_New(ctx, argv, argc, sctx, indexname, &status);
  if (!debug_req) {
    // parseHybridCommand takes ownership of sctx but doesn't free it on error - we need to clean it up
    SearchCtx_Free(sctx);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Parse debug parameters
  if (parseHybridDebugParams(debug_req, &status) != REDISMODULE_OK) {
    HybridRequest_Debug_Free(debug_req);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  HybridRequest *hreq = debug_req->hreq;

  // Now apply debug parameters to the built pipelines
  if (applyHybridDebugToBuiltPipelines(debug_req, &status) != REDISMODULE_OK) {
    HybridRequest_Debug_Free(debug_req);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  HybridRequest_Execute(hreq, ctx, hreq->sctx);

  HybridRequest_Debug_Free(debug_req);
  return REDISMODULE_OK;
}
