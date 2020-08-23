#ifndef REDISMOCK_H
#define REDISMOCK_H
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*RMCKModuleLoadFunction)(RedisModuleCtx *, RedisModuleString **, int);

void RMCK_Bootstrap(RMCKModuleLoadFunction fn, const char **s, size_t n);

void RMCK_Notify(const char *action, int events, const char *key);

// Destroy all globals
void RMCK_Shutdown(void);

#ifdef __cplusplus
}
#endif
#endif
