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
  Unlocked = 0,
  Owned,
  Borrowed,
} SharedExclusiveLockType;

/**
 * Initialize the shared exclusive lock system.
 * Must be called before using any other lock functions.
 */
void SharedExclusiveLock_Init();

/**
 * Destroy the shared exclusive lock system.
 * Cleans up all mutexes and condition variables.
 */
void SharedExclusiveLock_Destroy();

/**
 * Mark the GIL as owned by the main thread.
 * Signals waiting threads that instead of acquiring the GIL, they should acquire the internal lock.
 *
 * @warning Should only be called by the main thread.
 */
void SharedExclusiveLock_LendGIL();

/**
 * Mark the GIL as not owned by the main thread.
 * Waits for any internal lock holders to release before returning.
 * Makes sure by waiting that while any thread holds the internal lock as an alternative to the GIL, no other thread will try to acquire the GIL.
 *
 * @warning Should only be called by the main thread.
 */
void SharedExclusiveLock_TakeBackGIL();

/**
 * Acquire either the GIL or internal lock, makes sure that only one thread can return from this function at a time.
 * Main thread may need to call this to ensure exclusive access to RedisModule_Yield or RedisModule_Call.
 * @param ctx Redis module context for GIL operations
 * @param acquireInternalLock Should be true if the caller knows it owns the GIL, and its aim is to ensure exclusive access to RedisModule_Yield or RedisModule_Call with respect to threads
 * relying on the internal lock.
 * @return Type of lock acquired (Owned or Borrowed)
 */
SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx);

/**
 * Release the previously acquired lock.
 * @param ctx Redis module context
 * @param type Type of lock to release (expected to come from SharedExclusiveLock_Acquire)
 */
void SharedExclusiveLock_Release(RedisModuleCtx *ctx, SharedExclusiveLockType type);

#ifdef __cplusplus
}
#endif
