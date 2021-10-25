#pragma once

#include <time.h>
#include <redismodule.h>

#ifdef __cplusplus
extern "C" {
#endif

struct RedisModuleCtx;
extern struct RedisModuleCtx *RSDummyContext;

static inline double curTimeNs() {
  struct timespec tv;
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return (tv.tv_nsec / 1000000.0 + (tv.tv_sec * 1000));
}

#define RS_LOG_TIME(str)                                      \
do {                                                          \
  RedisModule_Log(RSDummyContext, "warning",                  \
                  str ": time %f", curTimeNs());              \
} while (0)


#define RS_LOG_TIME1(str)                                 \
do {                                                      \
  const char id;                                          \
  char ip;                                                \
  char master_id;                                         \
  int port;                                               \
  int flag;                                               \
  RedisModule_GetClusterNodeInfo(RSDummyContext,          \
                                 &id,                     \
                                 &ip,                     \
                                 &master_id,              \
                                 &port,                   \
                                 &flag);                  \
                                                          \
  RedisModule_Log(RSDummyContext, "warning", "%s", str);  \
  RedisModule_Log(RSDummyContext, "warning", "time %f, ip %d, port %d", curTimeNs(), ip, port);   \
} while (0)


        //RedisModule_Log(assertCtx, "warning", (fmt), __VA_ARGS__);       


#ifdef __cplusplus
}
#endif
