#ifndef __REDISEARCH_ASSERT__
#define __REDISEARCH_ASSERT__

#include "redismodule.h"

#ifdef NDEBUG

#define RS_LOG_ASSERT(ctx, condition, fmt, ...)    (__ASSERT_VOID_CAST (0))
#define RS_LOG_ASSERT_STR(ctx, condition, str)     (__ASSERT_VOID_CAST (0))

#else

#define RS_LOG_ASSERT_FMT(condition, fmt, ...)                                      \
    if (!(condition)) {                                                             \
        RedisModuleCtx* assertCtx = RedisModule_GetThreadSafeContext(NULL);         \
        RedisModule_Log(assertCtx, "warning", "File %s, Function %s, Line %d - "    \
                fmt, __FILE__, __func__, __LINE__, __VA_ARGS__);                    \
        *((char *)NULL) = 0; /* Crashes server crash report*/                       \
    } 

#define RS_LOG_ASSERT(condition, str)  RS_LOG_ASSERT_FMT(condition, str "%s", "")

#endif  //NDEBUG

#endif  //__REDISEARCH_ASSERT__