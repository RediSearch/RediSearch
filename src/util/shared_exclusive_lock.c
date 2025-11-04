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

pthread_mutex_t InternalLock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t GILCondition = PTHREAD_COND_INITIALIZER;
atomic_bool GILOwned;


void SharedExclusiveLock_Init() {
  GILOwned = ATOMIC_VAR_INIT(false);
  pthread_mutex_init(&InternalLock, NULL);
  pthread_cond_init(&GILCondition, NULL);
}

void SharedExclusiveLock_Destroy() {
  pthread_mutex_destroy(&InternalLock);
  pthread_cond_destroy(&GILCondition);
}

void SharedExclusiveLock_SetOwned(bool value) {
  atomic_store_explicit(&GILOwned, value, memory_order_release);
  // Signal waiting threads when GIL ownership changes
  pthread_cond_broadcast(&GILCondition);
}

SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx) {
  pthread_mutex_lock(&InternalLock);
  while (true) {
    int rc;
    if (atomic_load_explicit(&GILOwned, memory_order_acquire)) {
      // The GIL is owned in a safe manner by the module in the main thread, so we hold the internal lock and return.
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
  if (type == Internal_Locked) {
    pthread_mutex_unlock(&InternalLock);
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}
