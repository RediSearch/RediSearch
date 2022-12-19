/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __REDISEARCH_ASSERT__
#define __REDISEARCH_ASSERT__

#include "redismodule.h"
#include "module.h"

#ifdef NDEBUG

#define RS_LOG_ASSERT(ctx, condition, fmt, ...)    (__ASSERT_VOID_CAST (0))
#define RS_LOG_ASSERT_STR(ctx, condition, str)     (__ASSERT_VOID_CAST (0))

#else

#define RS_LOG_ASSERT_FMT(condition, fmt, ...)                                          \
    if (__builtin_expect(!(condition), 0)) {                                            \
        RedisModule_Log(RSDummyContext, "warning", (fmt), __VA_ARGS__);                 \
        RedisModule_Assert(condition); /* Crashes server and create a crash report*/    \
    } 

#define RS_LOG_ASSERT(condition, str)  RS_LOG_ASSERT_FMT(condition, str "%s", "")

#endif  //NDEBUG

#define RS_CHECK_FUNC(funcName, ...)                                          \
    if (funcName) {                                                           \
        funcName(__VA_ARGS__);                                                \
    } 

#endif  //__REDISEARCH_ASSERT__