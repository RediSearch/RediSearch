/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once
#include <stdbool.h>
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  GIL_Locked,
  Internal_Locked,
} SharedExclusiveLockType;

void SharedExclusiveLock_Init();
void SharedExclusiveLock_Destroy();
void SharedExclusiveLock_SetOwned();
void SharedExclusiveLock_UnsetOwned();
SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx);
void SharedExclusiveLock_Release(RedisModuleCtx *ctx, SharedExclusiveLockType type);

#ifdef __cplusplus
}
#endif
