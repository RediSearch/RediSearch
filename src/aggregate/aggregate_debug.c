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

AREQ_Debug *AREQ_Debug_New(RedisModuleString **argv, int argc, QueryError *status) {

  AREQ_Debug_params debug_params = parseDebugParamsCount(argv, argc, status);
  if (debug_params.debug_params_count == 0) {
    return NULL;
  }

  AREQ_Debug *debug_req = rm_realloc(AREQ_New(), sizeof(*debug_req));
  debug_req->debug_params = debug_params;

  AREQ *r = &debug_req->r;
  r->reqflags |= QEXEC_F_DEBUG;

  return debug_req;
}

int parseAndCompileDebug(AREQ_Debug *debug_req, QueryError *status) {
  RedisModuleString **debug_argv = debug_req->debug_params.debug_argv;
  unsigned long long debug_params_count = debug_req->debug_params.debug_params_count;

  // Parse the debug params
  // For example debug_params = TIMEOUT_AFTER_N 2
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, debug_argv, debug_params_count);
  ArgsCursor timeoutArgs = {0};
  ArgsCursor pauseBeforeArgs = {0};
  ArgsCursor pauseAfterArgs = {0};
  ACArgSpec debugArgsSpec[] = {
      // Getting TIMEOUT_AFTER_N as an array to use AC_IsInitialized API.
      {.name = "TIMEOUT_AFTER_N",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &timeoutArgs,
       .slicelen = 1},
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
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Unrecognized argument: %s",
                             AC_GetStringNC(&ac, NULL));
    } else if (errSpec) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "%s: %s", errSpec->name, AC_Strerror(rv));
    } else {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Error parsing arguments: %s",
                             AC_Strerror(rv));
    }
    return REDISMODULE_ERR;
  }


  // Handle timeout
  if (AC_IsInitialized(&timeoutArgs)) {
    unsigned long long results_count = -1;
    if (AC_GetUnsignedLongLong(&timeoutArgs, &results_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid TIMEOUT_AFTER_N count");
      return REDISMODULE_ERR;
    }

    // Add timeout to the pipeline
    // Note, this will add a result processor as the downstream of the last result processor
    // (rpidnext for SA)
    // Take this into account when adding more debug types that are modifying the rp pipeline.
    PipelineAddTimeoutAfterCount(&debug_req->r, results_count);
    return REDISMODULE_OK;
  }

  // Handle pause before/after RP after N (contains the same logic)
  // Args order: RP_TYPE, N
  if (AC_IsInitialized(&pauseAfterArgs) || AC_IsInitialized(&pauseBeforeArgs)) {

    bool before = AC_IsInitialized(&pauseBeforeArgs);
    ArgsCursor *pauseArgs = before ? &pauseBeforeArgs : &pauseAfterArgs;
    const char *invalidStr = before ? "PAUSE_BEFORE_RP_N" : "PAUSE_AFTER_RP_N";
    const char *rp_type_str = NULL;
    if (!(debug_req->r.reqflags & QEXEC_F_RUN_IN_BACKGROUND)) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Query %s is only supported with WORKERS", invalidStr);
      return REDISMODULE_ERR;
    }

    if (AC_GetString(pauseArgs, &rp_type_str, NULL, 0) != AC_OK) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid %s RP type", invalidStr);
      return REDISMODULE_ERR;
    }
    unsigned long long results_count = -1;
    ResultProcessorType rp_type = StringToRPType(rp_type_str);
    // Verify the RP type is valid, not a debug RP type
    if (rp_type == RP_MAX) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "%s is an invalid %s RP type", rp_type_str, invalidStr);
      return REDISMODULE_ERR;
    }

    if (AC_GetUnsignedLongLong(pauseArgs, &results_count, AC_F_GE0) != AC_OK) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid %s count", invalidStr);
      return REDISMODULE_ERR;
    }

    if (!PipelineAddPauseRPcount(&debug_req->r, results_count, before, rp_type, status)) {
      // The query error is handled by each error case
      return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
  }
}

AREQ_Debug_params parseDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status) {
  AREQ_Debug_params debug_params = {0};
  // Verify DEBUG_PARAMS_COUNT exists in its expected position
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
