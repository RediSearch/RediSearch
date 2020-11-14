
#pragma once

#include "redismodule.h"

///////////////////////////////////////////////////////////////////////////////////////////////

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Cleans up all globals in the module
void RediSearch_CleanupModule();

// Indicates that RediSearch_Init was called
extern int RS_Initialized;
// Module-level dummy context for certain dummy RM_XXX operations
extern RedisModuleCtx *RSDummyContext;
// Indicates that RediSearch_Init was called
extern int RS_Initialized;

///////////////////////////////////////////////////////////////////////////////////////////////
