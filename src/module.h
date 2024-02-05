/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"
#include "rmutil/rm_assert.h"

#ifdef __cplusplus
extern "C" {
#endif
int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int IsMaster();
int IsEnterprise();

void GetFormattedRedisVersion(char *buf, size_t len);
void GetFormattedRedisEnterpriseVersion(char *buf, size_t len);

/** Cleans up all globals in the module */
void RediSearch_CleanupModule(void);

// Local spellcheck command
int SpellCheckCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/** Indicates that RediSearch_Init was called */
extern int RS_Initialized;
/** Module-level dummy context for certain dummy RM_XXX operations */
extern RedisModuleCtx *RSDummyContext;
/** Indicates that RediSearch_Init was called */
extern int RS_Initialized;

#define RS_AutoMemory(ctx)                      \
do {                                            \
  RS_LOG_ASSERT(ctx != RSDummyContext, "");     \
  RedisModule_AutoMemory(ctx);                  \
} while (0)

#ifdef __cplusplus
}
#endif
#endif
