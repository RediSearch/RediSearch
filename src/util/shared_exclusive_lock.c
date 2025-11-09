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


// Lock used as an alternative to the GIL, this is an additional lock which is always acquired
// when calling SharedExclusiveLock_Acquire, and is held until SharedExclusiveLock_Release is called.
// When the GIL is lent by the main thread, threads will be satisfied by acquiring this lock only.
pthread_mutex_t GILAlternativeLock = PTHREAD_MUTEX_INITIALIZER;
// Lock to synchronize internal state.
// Expects low contention (at most one thread and the main thread will race on it).
pthread_mutex_t InternalLock = PTHREAD_MUTEX_INITIALIZER;
// Condition for threads waiting to acquire the GIL while it is not available.
// It is also used with a timeout because we cannot guarantee that the main thread will always signal it.
// GILAlternativeLock must be held when waiting on this condition.
pthread_cond_t GILAvailable = PTHREAD_COND_INITIALIZER;
// Condition for the main thread to wait on while trying to take back the GIL.
pthread_cond_t GILIsBorrowed = PTHREAD_COND_INITIALIZER;
// Flags to indicate whether the GIL is lent by the main thread
bool GIL_lent = false;
// Flag to indicate whether the GIL is currently borrowed by any thread
bool GIL_borrowed = false;

// Locks Order:
// 1. GIL
// 2. GILAlternativeLock
// 3. InternalLock
// A thread may hold any combination of these locks, but must always acquire them in this order.
// One exception is that we acquire the GIL last in SharedExclusiveLock_Acquire, but there is
// no risk of deadlock because we do it with a "try" mechanism.

#define TIMEOUT_NANOSECONDS 5000 // 5 us in nanoseconds
#define NANOSEC_PER_SECOND 1000000000L // 10^9

static inline void set_timeout(struct timespec *timeout) {
  clock_gettime(CLOCK_MONOTONIC_RAW, timeout);
  timeout->tv_nsec += TIMEOUT_NANOSECONDS;
  if (timeout->tv_nsec < NANOSEC_PER_SECOND) {
  } else {
    // Assumes TIMEOUT_NANOSECONDS will not exceed NANOSEC_PER_SECOND,
    // so we are only off by one second maximum.
    timeout->tv_nsec -= NANOSEC_PER_SECOND;
    timeout->tv_sec += 1;
  }
}

void SharedExclusiveLock_Init() {
  GIL_lent = false;
  GIL_borrowed = false;
  pthread_mutex_init(&GILAlternativeLock, NULL);
  pthread_mutex_init(&InternalLock, NULL);

  // Initialize GILAvailable with CLOCK_MONOTONIC_RAW
  pthread_condattr_t cond_attr;
  pthread_condattr_init(&cond_attr);
  pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC_RAW);
  pthread_cond_init(&GILAvailable, &cond_attr);
  pthread_condattr_destroy(&cond_attr);

  pthread_cond_init(&GILIsBorrowed, NULL);
}

void SharedExclusiveLock_Destroy() {
  pthread_mutex_destroy(&GILAlternativeLock);
  pthread_cond_destroy(&GILAvailable);
  pthread_mutex_destroy(&InternalLock);
  pthread_cond_destroy(&GILIsBorrowed);
}

// Assumptions:
// 1. The caller holds the GIL.
// 2. The caller won't release the GIL before calling TakeBackGIL.
// Note: The caller may call SharedExclusiveLock_Acquire while holding the GIL, if it lends it.
void SharedExclusiveLock_LendGIL() {
  pthread_mutex_lock(&InternalLock);
  GIL_lent = true;
  pthread_mutex_unlock(&InternalLock);
  // Signal any waiting threads that they may try to borrow the GIL.
  pthread_cond_broadcast(&GILAvailable);
}

// Assumptions:
// 1. The caller holds the GIL.
// 2. The caller has previously called SharedExclusiveLock_LendGIL and will not call it again before TakeBackGIL.
// 3. If the caller has called SharedExclusiveLock_Acquire while holding the GIL, it has released it before calling TakeBackGIL.
void SharedExclusiveLock_TakeBackGIL() {
  // Here we make sure that any thread that may assume the GIL is protected by the main thread releases the lock before returning.
  pthread_mutex_lock(&InternalLock);
  GIL_lent = false; // From now on, any thread should try to acquire the GIL, and not the alternative lock.
  while (GIL_borrowed) {
    pthread_cond_wait(&GILIsBorrowed, &InternalLock);
  }
  pthread_mutex_unlock(&InternalLock);
}

// Assumptions:
// 1. No re-entrancy: A thread that has already acquired the lock will not try to acquire it again before releasing it.
SharedExclusiveLockType SharedExclusiveLock_Acquire(RedisModuleCtx *ctx) {
  // First, acquire the alternative lock. Only one thread can try to acquire either the GIL or the alternative lock at a time.
  pthread_mutex_lock(&GILAlternativeLock);
  while (true) {
    SharedExclusiveLockType lockType = Unlocked;
    // Attempt to acquire the GIL, according to the internal state.
    pthread_mutex_lock(&InternalLock);
    if (GIL_lent) {
      // GIL is lent by the main thread, mark that we borrow it.
      GIL_borrowed = true;
      lockType = Borrowed;
    } else if (RedisModule_ThreadSafeContextTryLock(ctx) == REDISMODULE_OK) {
      // GIL is not lent by the main thread, but we managed to acquire it.
      lockType = Owned;
    }
    pthread_mutex_unlock(&InternalLock);

    if (lockType != Unlocked) {
      return lockType; // We have exclusive access
    }

    // Couldn't acquire the GIL, wait (for a short time or until signaled) before trying again.
    struct timespec timeout;
    set_timeout(&timeout, TIMEOUT_NANOSECONDS);
    pthread_cond_timedwait(&GILAvailable, &GILAlternativeLock, &timeout);
  }
}

// Assumptions:
// 1. The caller has previously acquired the lock by calling SharedExclusiveLock_Acquire
// 2. The value of 'type' is the result of the previous call to SharedExclusiveLock_Acquire
void SharedExclusiveLock_Release(RedisModuleCtx *ctx, SharedExclusiveLockType type) {
  if (type == Borrowed) {
    pthread_mutex_lock(&InternalLock);
    GIL_borrowed = false;
    // If main thread is waiting to release the GIL, signal it that it can proceed.
    pthread_cond_signal(&GILIsBorrowed);
    pthread_mutex_unlock(&InternalLock);

    // Signal any waiting threads that they may try to acquire the GIL or the alternative lock.
    pthread_cond_broadcast(&GILAvailable);
  } else {
    RS_ASSERT(type == Owned);
    RedisModule_ThreadSafeContextUnlock(ctx);
  }
  pthread_mutex_unlock(&GILAlternativeLock);
}
