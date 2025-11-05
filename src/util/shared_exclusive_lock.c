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

#define TIMEOUT_NANOSECONDS 5000 // 5 us in nanoseconds

pthread_mutex_t GILAlternativeLock = PTHREAD_MUTEX_INITIALIZER; // Lock used as an alternative to the GIL, this is used when the GIL is owned by the main thread.
pthread_mutex_t InternalLock = PTHREAD_MUTEX_INITIALIZER; // Lock to handle communication mechanism internally
pthread_cond_t GILCondition = PTHREAD_COND_INITIALIZER;
pthread_cond_t AuxLockCondition = PTHREAD_COND_INITIALIZER;
atomic_bool GILOwned;
bool GILAlternativeLockHeld = false;


void SharedExclusiveLock_Init() {
  atomic_init(&GILOwned, false);
  GILAlternativeLockHeld = false;
  pthread_mutex_init(&GILAlternativeLock, NULL);
  pthread_mutex_init(&InternalLock, NULL);
  pthread_cond_init(&GILCondition, NULL);
  pthread_cond_init(&AuxLockCondition, NULL);
}

void SharedExclusiveLock_Destroy() {
  pthread_mutex_destroy(&GILAlternativeLock);
  pthread_cond_destroy(&GILCondition);
  pthread_mutex_destroy(&InternalLock);
  pthread_cond_destroy(&AuxLockCondition);
}

void SharedExclusiveLock_SetOwned() {
  atomic_store_explicit(&GILOwned, true, memory_order_release);
  pthread_cond_broadcast(&GILCondition);
}

void SharedExclusiveLock_UnsetOwned() {
  atomic_store_explicit(&GILOwned, false, memory_order_release);
  // Here we make sure that any thread that may assume the GIL is protected by the main thread releases the lock before returning.
  pthread_mutex_lock(&InternalLock);
  while (GILAlternativeLockHeld) {
    pthread_cond_wait(&AuxLockCondition, &InternalLock);
  }
  pthread_mutex_unlock(&InternalLock);
}

SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx) {
  pthread_mutex_lock(&InternalLock);
  while (true) {
    int rc;
    if (atomic_load_explicit(&GILOwned, memory_order_acquire)) {
      // The GIL is owned in a safe manner by the module in the main thread, so we hold the internal lock and return.
      pthread_mutex_unlock(&InternalLock);
      pthread_mutex_lock(&GILAlternativeLock);
      GILAlternativeLockHeld = true;
      return Internal_Locked; // internal handle is the non-GIL lock
    } else {
      rc = RedisModule_ThreadSafeContextTryLock(ctx);
    }
    if (rc == REDISMODULE_OK) {
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
    pthread_cond_timedwait(&GILCondition, &InternalLock, &timeout);
  }
}

void SharedExclusiveLock_Release(RedisModuleCtx *ctx, SharedExclusiveLockType type) {
  if (type == Internal_Locked) {// Attempt to wake up some waiting thread
    pthread_mutex_lock(&InternalLock);
    GILAlternativeLockHeld = false;
    pthread_cond_signal(&AuxLockCondition);
    pthread_mutex_unlock(&InternalLock);
    pthread_mutex_unlock(&GILAlternativeLock);
    pthread_cond_broadcast(&GILCondition);
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}
