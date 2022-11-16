/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __TEST_UTIL_H__
#define __TEST_UTIL_H__

#include "util.h"
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#define RMUtil_Test(f)                                       \
  if (argc < 2 || RMUtil_ArgExists(#f, argv, argc, 1)) {     \
    int rc = f(ctx);                                         \
    if (rc != REDISMODULE_OK) {                              \
      RedisModule_ReplyWithError(ctx, "Test " #f " FAILED"); \
      return REDISMODULE_ERR;                                \
    }                                                        \
  }

#define RMUtil_Assert(expr)                            \
  if (!(expr)) {                                       \
    fprintf(stderr, "Assertion '%s' Failed\n", #expr); \
    return REDISMODULE_ERR;                            \
  }

#define RMUtil_AssertReplyEquals(rep, cstr)                                     \
  RMUtil_Assert(RMUtil_StringEquals(RedisModule_CreateStringFromCallReply(rep), \
                                    RedisModule_CreateString(ctx, cstr, strlen(cstr))))
#

/**
 * Create an arg list to pass to a redis command handler manually, based on the format in fmt.
 * The accepted format specifiers are:
 *   c - for null terminated c strings
 *   s - for RedisModuleString* objects
 *   l - for longs
 *
 *  Example:  RMUtil_MakeArgs(ctx, &argc, "clc", "hello", 1337, "world");
 *
 *  Returns an array of RedisModuleString pointers. The size of the array is store in argcp
 */
RedisModuleString **RMUtil_MakeArgs(RedisModuleCtx *ctx, int *argcp, const char *fmt, ...) {

  va_list ap;
  va_start(ap, fmt);
  RedisModuleString **argv = rm_calloc(strlen(fmt), sizeof(RedisModuleString *));
  int argc = 0;
  const char *p = fmt;
  while (*p) {
    if (*p == 'c') {
      char *cstr = va_arg(ap, char *);
      argv[argc++] = RedisModule_CreateString(ctx, cstr, strlen(cstr));
    } else if (*p == 's') {
      argv[argc++] = va_arg(ap, void *);
      ;
    } else if (*p == 'l') {
      long ll = va_arg(ap, long long);
      argv[argc++] = RedisModule_CreateStringFromLongLong(ctx, ll);
    } else {
      goto fmterr;
    }
    p++;
  }
  *argcp = argc;

  return argv;
fmterr:
  free(argv);
  return NULL;
}

#endif
