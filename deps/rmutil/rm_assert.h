#pragma once

#include <redismodule.h>
#include <assert.h>
#include "module.h"
#ifdef NDEBUG

#define RS_LOG_ASSERT(ctx, condition, fmt, ...)    (__ASSERT_VOID_CAST (0))
#define RS_LOG_ASSERT_STR(ctx, condition, str)     (__ASSERT_VOID_CAST (0))

#else

#define RS_LOG_ASSERT_FMT(condition, fmt, ...)                                          \
    if (!(condition)) {                                                                 \
        RedisModuleCtx* assertCtx = RSDummyContext;                                     \
        RedisModule_Log(assertCtx, "warning", "(%s) failed on %s:%s, Line %d - " fmt,   \
                #condition, __FILE__, __func__, __LINE__, __VA_ARGS__);                 \
        *((char *)NULL) = 0; /* Crashes server and create a crash report*/              \
    }

#define RS_LOG_ASSERT(condition, str)  RS_LOG_ASSERT_FMT(condition, str "%s", "")

#endif  //NDEBUG
