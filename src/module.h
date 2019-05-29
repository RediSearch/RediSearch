#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif
int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/** Cleans up all globals in the module */
void RediSearch_CleanupModule(void);

#ifdef __cplusplus
}
#endif
#endif
