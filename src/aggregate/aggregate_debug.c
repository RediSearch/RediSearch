/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "aggregate_debug.h"

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
  ACArgSpec debugArgsSpec[] = {
      // Getting TIMEOUT_AFTER_N as an array to use AC_IsInitialized API.
      {.name = "TIMEOUT_AFTER_N",
       .type = AC_ARGTYPE_SUBARGS_N,
       .target = &timeoutArgs,
       .slicelen = 1},
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

  if (AC_IsInitialized(&timeoutArgs)) {
    unsigned long long results_count = -1;
    if (AC_GetUnsignedLongLong(&timeoutArgs, &results_count, AC_F_GE0) != AC_OK) {
      QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid TIMEOUT_AFTER_N count");
      return REDISMODULE_ERR;
    }

    // Add timeout to the pipeline
    // Note, this will add a result processor as the downstream of the last result processor
    // (rpidnext for SA, or RPNext for cluster)
    // Take this into account when adding more debug types that are modifying the rp pipeline.
    PipelineAddTimeoutAfterCount(&debug_req->r, results_count);
  }

  return REDISMODULE_OK;
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
