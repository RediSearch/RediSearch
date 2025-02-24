/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __REDISEARCH_ASSERT__
#define __REDISEARCH_ASSERT__

#include "redismodule.h"
#include "module.h"

// Not to be called directly, used by the macros below
#define _RS_LOG_ASSERT_FMT(condition, fmt, ...)                                      \
    if (__builtin_expect(!(condition), 0)) {                                         \
        RedisModule_Log(RSDummyContext, "warning", (fmt), __VA_ARGS__);              \
        RedisModule_Assert(condition); /* Crashes server and create a crash report*/ \
    }

#ifdef NDEBUG
#define RS_LOG_ASSERT_FMT(condition, fmt, ...) // NOP
#else
#define RS_LOG_ASSERT_FMT(condition, fmt, ...) _RS_LOG_ASSERT_FMT(condition, fmt, __VA_ARGS__)
#endif

#define RS_LOG_ASSERT(condition, str) RS_LOG_ASSERT_FMT(condition, str "%s", "")
#define RS_ASSERT(condition) RS_LOG_ASSERT_FMT(condition, "Assertion failed: %s", #condition)
#define RS_ABORT(str) RS_LOG_ASSERT_FMT(0, "Aborting: %s", str)

// Assertions that we want to keep in production artifacts.
#define RS_LOG_ASSERT_FMT_ALWAYS(condition, fmt, ...) _RS_LOG_ASSERT_FMT(condition, fmt, __VA_ARGS__)
#define RS_LOG_ASSERT_ALWAYS(condition, str)  RS_LOG_ASSERT_FMT_ALWAYS(condition, str "%s", "")
#define RS_ASSERT_ALWAYS(condition) RS_LOG_ASSERT_FMT_ALWAYS(condition, "Assertion failed: %s", #condition)
#define RS_ABORT_ALWAYS(str) RS_LOG_ASSERT_FMT_ALWAYS(0, "Aborting: %s", str)

#define RS_CHECK_FUNC(funcName, ...)                                          \
    if (funcName) {                                                           \
        funcName(__VA_ARGS__);                                                \
    } 

#endif  //__REDISEARCH_ASSERT__
