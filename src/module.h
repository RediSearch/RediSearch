#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif
int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int IsEnterprise();

/** Cleans up all globals in the module */
void RediSearch_CleanupModule(void);

/** Indicates that RediSearch_Init was called */
extern int RS_Initialized;
/** Module-level dummy context for certain dummy RM_XXX operations */
extern RedisModuleCtx *RSDummyContext;
/** Indicates that RediSearch_Init was called */
extern int RS_Initialized;

#ifdef __cplusplus
}
#endif
#endif
