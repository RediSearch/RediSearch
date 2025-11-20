/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "aggregate_debug.h"
#include "module.h"

/*  Using INTERNAL_ONLY with TIMEOUT_AFTER_N where N == 0 may result in an infinite loop in the
   coordinator. Since shard replies are always empty, the coordinator might get stuck indefinitely
   waiting for results or a timeout. If the query timeout is set to 0 (disabled), neither of these
   conditions is met. To prevent this, if results_count == 0 and the query timeout is disabled, we
   enforce a forced timeout, ideally large enough to break the infinite loop without impacting the
   requested flow */
#define COORDINATOR_FORCED_TIMEOUT 1000

AREQ_Debug *AREQ_Debug_New(RedisModuleString **argv, int argc, QueryError *status) {

  AREQ_Debug_params debug_params = parseDebugParamsCount(argv, argc, status);
  if (debug_params.debug_params_count == 0) {
    return NULL;
  }

  AREQ_Debug *debug_req = rm_realloc(AREQ_New(), sizeof(*debug_req));
  debug_req->debug_params = debug_params;

  AREQ *r = &debug_req->r;
  AREQ_AddRequestFlags(r, QEXEC_F_DEBUG);

  return debug_req;
}


// Return True if we are in a cluster environment running the coordinator
static bool isClusterCoord(AREQ_Debug *debug_req) {
  if ((GetNumShards_UnSafe() > 1) && !IsInternal(&debug_req->r)) {
    return true;
  }

  return false;
}

int parseAndCompileDebug(AREQ_Debug *debug_req, QueryError *status) {
  RedisModuleString **debug_argv = debug_req->debug_params.debug_argv;
  unsigned long long debug_params_count = debug_req->debug_params.debug_params_count;

  // Parse the debug params
  // For example debug_params = TIMEOUT_AFTER_N 2 [INTERNAL_ONLY]
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, debug_argv, debug_params_count);
  ArgsCursor timeoutArgs = {0};
  int crash = 0;
  int internal_only = 0;
  ArgsCursor pauseBeforeArgs = {0};
  ArgsCursor pauseAfterArgs = {0};
  ACArgSpec debugArgsSpec[] = {
      // Getting TIMEOUT_AFTER_N as an array to use AC_IsInitialized API.
      {.name = "TIMEOUT_AFTER_N",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &timeoutArgs,
       .slicelen = 1},
      // crash at the start of the query
      {.name = "CRASH", .type = AC_ARGTYPE_BOOLFLAG, .target = &crash},
      // optional arg for TIMEOUT_AFTER_N
      {.name = "INTERNAL_ONLY", .type = AC_ARGTYPE_BOOLFLAG, .target = &internal_only},
      // pause after specific RP after N results
      {.name = "PAUSE_AFTER_RP_N",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &pauseAfterArgs,
       .slicelen = 2},
      // pause after specific RP before N results
      {.name = "PAUSE_BEFORE_RP_N",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &pauseBeforeArgs,
       .slicelen = 2},
      {NULL}};

  ACArgSpec *errSpec = NULL;
  int rv = AC_ParseArgSpec(&ac, debugArgsSpec, &errSpec);
  if (rv != AC_OK) {
    if (rv == AC_ERR_ENOENT) {
      // Argument not recognized
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Unrecognized argument", ": %s",
                             AC_GetStringNC(&ac, NULL));
    } else if (errSpec) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Error parsing arguments for index", " %s: %s", errSpec->name, AC_Strerror(rv));
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Error parsing arguments", ": %s",
                             AC_Strerror(rv));
    }
    return REDISMODULE_ERR;
  }

  // Handle crash
  if (crash) {
    // Verify internal_only is not used with CRASH
    if (internal_only) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "INTERNAL_ONLY is not supported with CRASH");
      return REDISMODULE_ERR;
    }

    PipelineAddCrash(&debug_req->r);
  }


  // Handle timeout
  if (AC_IsInitialized(&timeoutArgs)) {
    unsigned long long results_count = -1;
    if (AC_GetUnsignedLongLong(&timeoutArgs, &results_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid TIMEOUT_AFTER_N count");
      return REDISMODULE_ERR;
    }

    // Check if timeout should be applied only in the shard query pipeline
    if (internal_only && isClusterCoord(debug_req)) {
      if (debug_req->r.reqConfig.queryTimeoutMS == 0 && results_count == 0) {
          RedisModule_Log(RSDummyContext, "debug",
                          "Forcing coordinator timeout for TIMEOUT_AFTER_N 0 and query timeout 0 "
                          "to avoid infinite loop");
          debug_req->r.reqConfig.queryTimeoutMS = COORDINATOR_FORCED_TIMEOUT;
          SearchCtx_UpdateTime(debug_req->r.sctx, debug_req->r.reqConfig.queryTimeoutMS);
      }
    } else {  // INTERNAL_ONLY was not provided, or we are not in a cluster coordinator
      // Add timeout to the pipeline
      // Note, this will add a result processor as the downstream of the last result processor
      // (rpidnext for SA, or RPNext for cluster)
      // Take this into account when adding more debug types that are modifying the rp pipeline.
      PipelineAddTimeoutAfterCount(AREQ_QueryProcessingCtx(&debug_req->r), AREQ_SearchCtx(&debug_req->r), results_count);
    }
    return REDISMODULE_OK;
  }

  // Handle pause before/after RP after N (contains the same logic)
  // Args order: RP_TYPE, N
  if (AC_IsInitialized(&pauseAfterArgs) || AC_IsInitialized(&pauseBeforeArgs)) {

    // In FT.AGGREGATE - Check if INTERNAL_ONLY is set
    // If it is set - if we are in a cluster coordinator - do nothing
    // If it is not set - if we are not cluster coordinator - do nothing
    // This can be checked by comparing isClusterCoord(debug_req) and internal_only
    if ((debug_req->r.reqflags & QEXEC_F_IS_AGGREGATE) &&
          isClusterCoord(debug_req) == internal_only) {
      return REDISMODULE_OK;
    }

    bool before = AC_IsInitialized(&pauseBeforeArgs);
    ArgsCursor *pauseArgs = before ? &pauseBeforeArgs : &pauseAfterArgs;
    const char *invalidStr = before ? "PAUSE_BEFORE_RP_N" : "PAUSE_AFTER_RP_N";
    const char *rp_type_str = NULL;
    if (!isClusterCoord(debug_req) && !(debug_req->r.reqflags & QEXEC_F_RUN_IN_BACKGROUND)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Query %s is only supported with WORKERS", invalidStr);
      return REDISMODULE_ERR;
    }

    if (AC_GetString(pauseArgs, &rp_type_str, NULL, 0) != AC_OK) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid %s RP type", invalidStr);
      return REDISMODULE_ERR;
    }
    unsigned long long results_count = -1;
    ResultProcessorType rp_type = StringToRPType(rp_type_str);
    // Verify the RP type is valid, not a debug RP type
    if (rp_type == RP_MAX) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "%s is an invalid %s RP type", rp_type_str, invalidStr);
      return REDISMODULE_ERR;
    }

    if (AC_GetUnsignedLongLong(pauseArgs, &results_count, AC_F_GE0) != AC_OK) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid %s count", invalidStr);
      return REDISMODULE_ERR;
    }

    if (!PipelineAddPauseRPcount(AREQ_QueryProcessingCtx(&debug_req->r), results_count, before, rp_type, status)) {
      // The query error is handled by each error case
      return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
  }

  // Verify internal_only is not used without TIMEOUT_AFTER_N or PAUSE_AFTER_RP_N/PAUSE_BEFORE_RP_N
  if (internal_only) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "INTERNAL_ONLY is not supported without TIMEOUT_AFTER_N or PAUSE_AFTER_RP_N/PAUSE_BEFORE_RP_N");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

AREQ_Debug_params parseDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status) {
  AREQ_Debug_params debug_params = {0};
  // Verify DEBUG_PARAMS_COUNT exists in its expected position
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
