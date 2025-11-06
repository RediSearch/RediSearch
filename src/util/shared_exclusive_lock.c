/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "shared_exclusive_lock.h"
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include "rmutil/rm_assert.h"

#define TIMEOUT_NANOSECONDS 5000 // 5 us in nanoseconds

typedef enum {
  GILState_Unknown, // Not known, may be used by main thread (in an uncontrolled way, or by some other thread, or unlocked)
  GILState_Owned, // The GIL is owned by the main thread, but is not "shared" via GILAlternativeLock by other threads
  GILState_Shared, // The GIL is owned by the main thread, and is "shared" via GILAlternativeLock by another thread
} GILState;

pthread_mutex_t GILAlternativeLock = PTHREAD_MUTEX_INITIALIZER; // Lock used as an alternative to the GIL, this is used when the GIL is owned by the main thread.
pthread_mutex_t InternalLock = PTHREAD_MUTEX_INITIALIZER; // Lock to handle communication mechanism internally
pthread_cond_t TryLockCondition = PTHREAD_COND_INITIALIZER; // Condition to signal threads waiting to try acquire the GIL or the alternative lock that they may try again.
pthread_cond_t GILSafeCondition = PTHREAD_COND_INITIALIZER; // Condition to signal the main thread that it can safely release the GIL. (In this case return from UnsetOwned)
GILState gilState = GILState_Unknown;

void SharedExclusiveLock_Init() {
  gilState = GILState_Unknown;
  pthread_mutex_init(&GILAlternativeLock, NULL);
  pthread_mutex_init(&InternalLock, NULL);
  pthread_cond_init(&TryLockCondition, NULL);
  pthread_cond_init(&GILSafeCondition, NULL);
}

void SharedExclusiveLock_Destroy() {
  pthread_mutex_destroy(&GILAlternativeLock);
  pthread_cond_destroy(&TryLockCondition);
  pthread_mutex_destroy(&InternalLock);
  pthread_cond_destroy(&GILSafeCondition);
}

void SharedExclusiveLock_SetOwned() {
  pthread_mutex_lock(&InternalLock);
  gilState = GILState_Owned;
  pthread_mutex_unlock(&InternalLock);
  // Signal any waiting threads that they may try to acquire the GIL or the alternative lock.
  pthread_cond_broadcast(&TryLockCondition);
}

void SharedExclusiveLock_UnsetOwned() {
  // Here we make sure that any thread that may assume the GIL is protected by the main thread releases the lock before returning.
  pthread_mutex_lock(&InternalLock);
  while (gilState > GILState_Owned) {
    pthread_cond_wait(&GILSafeCondition, &InternalLock);
  }
  // TODO: Without the 2 booleans. This could exhaust the main thread, more workers could be taking the Internal Lock
  gilState = GILState_Unknown;
  pthread_mutex_unlock(&InternalLock);
}

SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx) {
  pthread_mutex_lock(&InternalLock);
  while (true) {
    int rc;
    if (gilState >= GILState_Owned) {
      pthread_mutex_lock(&GILAlternativeLock);
      gilState = GILState_Shared;
      pthread_mutex_unlock(&InternalLock);
      return Internal_Locked;
    } else {
      rc = RedisModule_ThreadSafeContextTryLock(ctx);
    }
    if (rc == REDISMODULE_OK) {
      RS_LOG_ASSERT(gilState == GILState_Unknown, "gilState should be GILState_Unknown");
      pthread_mutex_unlock(&InternalLock);
      return GIL_Locked;
    }

    struct timespec timeout;
    clock_gettime(CLOCK_REALTIME, &timeout);
    timeout.tv_nsec += TIMEOUT_NANOSECONDS;
    // Handle nanosecond overflow to comply with POSIX timespec requirements
    if (timeout.tv_nsec >= 1000000000) {
      timeout.tv_sec += timeout.tv_nsec / 1000000000;
      timeout.tv_nsec %= 1000000000;
    }
    pthread_cond_timedwait(&TryLockCondition, &InternalLock, &timeout);
  }
}

void SharedExclusiveLock_Release(RedisModuleCtx *ctx, SharedExclusiveLockType type) {
  if (type == Internal_Locked) {
    pthread_mutex_unlock(&GILAlternativeLock);
    pthread_mutex_lock(&InternalLock);
    gilState = GILState_Owned;
    // If main thread is waiting to release the GIL, signal it that it can proceed.
    pthread_cond_signal(&GILSafeCondition);
    pthread_mutex_unlock(&InternalLock);
    // Signal any waiting threads that they may try to acquire the GIL or the alternative lock.
    pthread_cond_broadcast(&TryLockCondition);
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}
