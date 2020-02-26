#pragma once
#include "redismodule.h"

#ifdef NDEBUG

#define RS_LOG_ASSERT(ctx, condition, fmt, ...)    (__ASSERT_VOID_CAST (0))
#define RS_LOG_ASSERT_STR(ctx, condition, str)     (__ASSERT_VOID_CAST (0))
#else
// `ctx` can be NULL
#define RS_LOG_ASSERT_FMT(ctx, condition, fmt, ...)                             \
    if (!(condition)) {                                                         \
        if (ctx) {                                                              \
            RedisModule_Log(ctx, "warning", "File %s, Function %s, Line %d - "  \
                fmt, __FILE__, __func__,  __func__, __LINE__, __VA_ARGS__);     \
            RedisModule_Call(ctx, "DEBUG", "s", "SEGFAULT");                    \
        } else {                                                                  \
            *((char *)NULL) = 0;                                                \
        }                                                                       \
    } 

// `ctx` can be NULL
#define RS_LOG_ASSERT(ctx, condition, str)  RS_LOG_ASSERT_FMT(ctx, condition, str "%s", "")

#endif  //NDEBUG
