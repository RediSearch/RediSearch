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
#include <unistd.h>

pthread_mutex_t InternalLock = PTHREAD_MUTEX_INITIALIZER;
atomic_bool GILOwned;

void SharedExclusiveLock_Init() {
  GILOwned = ATOMIC_VAR_INIT(false);
  pthread_mutex_init(&InternalLock, NULL);
}

void SharedExclusiveLock_Destroy() {
  pthread_mutex_destroy(&InternalLock);
}

void SharedExclusiveLock_SetOwned(bool value) {
  atomic_store_explicit(&GILOwned, value, memory_order_release);
}

SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx) {
  pthread_mutex_lock(&InternalLock);
  while (true) {
    int rc;
    if (!atomic_load_explicit(&GILOwned, memory_order_acquire)) {
      return Internal_Locked;
    } else {
      rc = RedisModule_ThreadSafeContextTryLock(ctx);
    }
    if (rc == REDISMODULE_OK) {
      pthread_mutex_unlock(&InternalLock);
      return Internal_Locked;
    }
    usleep(5);
  }
}

void SharedExclusiveLock_Release(RedisModuleCtx *ctx, SharedExclusiveLockType type) {
  if (type == Internal_Locked) {
    pthread_mutex_unlock(&InternalLock);
  } else {
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
}
