/*
 * lock_handler.c
 *
 *  Created on: May 20, 2019
 */

#include "lock_handler.h"
#include "rmalloc.h"
#include <pthread.h>
#include <assert.h>
#include "lock_handler.h"

/**
 * Read Write lock (RWLock) implementation.
 * To avoid possible deadlocks when acquiring the RWLock, we must first acquire the GIL.
 * We maintain a counter inside thread local indicating how many times a the thread acquired the
 * GIL. This allows single thread to actually call LockHandler_AcquireGIL multiple times.
 */

pthread_rwlock_t lockRW = PTHREAD_RWLOCK_INITIALIZER;

pthread_key_t lockKey;

typedef enum AcquiredType {
  AcquiredType_READ,   // The thread acquire the RWLock for read
  AcquiredType_WRITE,  // The thread acquire the RWLock for write
  AcquiredType_NONE,
} AcquiredType;

typedef struct LockHandlerCtx {
  size_t GILacquiredAmount;
  size_t RWacquiredAmount;
  AcquiredType RWacquiredType;
} LockHandlerCtx;

static LockHandlerCtx* LockHandler_GetSpecific() {
  LockHandlerCtx* lh = pthread_getspecific(lockKey);
  if (!lh) {
    lh = rm_malloc(sizeof(*lh));
    lh->GILacquiredAmount = 0;
    lh->RWacquiredAmount = 0;
    lh->RWacquiredType = AcquiredType_NONE;
    pthread_setspecific(lockKey, lh);
  }
  return lh;
}

int LockHandler_Initialize() {
  int err = pthread_key_create(&lockKey, NULL);
  if (err) {
    return REDISMODULE_ERR;
  }

  LockHandlerCtx* lh = rm_malloc(sizeof(*lh));
  lh->GILacquiredAmount = 1;  // init is called from the main thread, the lock is always acquired
  lh->RWacquiredAmount = 0;
  lh->RWacquiredType = AcquiredType_NONE;
  pthread_setspecific(lockKey, lh);
  return REDISMODULE_OK;
}

void LockHandler_AcquireGIL(RedisModuleCtx* rctx) {
  LockHandlerCtx* lh = LockHandler_GetSpecific();
  if (lh->GILacquiredAmount == 0) {
    if (lh->RWacquiredAmount > 0) {
      // we want to acquire GIL while holding the RWLock.
      // to prevent deadlock we will release the RWLock and re-acquire it after acquire the GIL.
      pthread_rwlock_unlock(&lockRW);
    }
    RedisModule_ThreadSafeContextLock(rctx);
    if (lh->RWacquiredAmount > 0) {
      if (lh->RWacquiredType == AcquiredType_READ) {
        int res = pthread_rwlock_rdlock(&lockRW);
        assert(res == 0);
      } else if (lh->RWacquiredType == AcquiredType_WRITE) {
        int res = pthread_rwlock_wrlock(&lockRW);
        assert(res == 0);
      } else {
        assert(0);  // RWacquiredAmount > 0 but RWacquiredType is NONE, this is can not happened
      }
    }
  }
  ++lh->GILacquiredAmount;
}

void LockHandler_ReleaseGIL(RedisModuleCtx* rctx) {
  LockHandlerCtx* lh = LockHandler_GetSpecific();
  assert(lh);
  assert(lh->GILacquiredAmount > 0);
  if (--lh->GILacquiredAmount == 0) {
    RedisModule_ThreadSafeContextUnlock(rctx);
  }
}

void LockHandler_AcquireRead(RedisModuleCtx* rctx) {
  LockHandlerCtx* lh = LockHandler_GetSpecific();
  assert(lh);
  assert(lh->RWacquiredType != AcquiredType_WRITE);

  if (lh->RWacquiredAmount == 0) {
    LockHandler_AcquireGIL(rctx);  // prevent deadlocks!!
    int res = pthread_rwlock_rdlock(&lockRW);
    assert(res == 0);
    LockHandler_ReleaseGIL(rctx);
    lh->RWacquiredType = AcquiredType_READ;
  }

  ++lh->RWacquiredAmount;
}

void LockHandler_ReleaseRead(RedisModuleCtx* rctx) {
  LockHandlerCtx* lh = LockHandler_GetSpecific();
  assert(lh);
  assert(lh->RWacquiredAmount > 0);
  assert(lh->RWacquiredType == AcquiredType_READ);
  if (--lh->RWacquiredAmount == 0) {
    pthread_rwlock_unlock(&lockRW);
    lh->RWacquiredType = AcquiredType_NONE;
  }
}

void LockHandler_AcquireWrite(RedisModuleCtx* rctx) {
  LockHandlerCtx* lh = LockHandler_GetSpecific();
  assert(lh);
  assert(lh->RWacquiredType != AcquiredType_READ);

  if (lh->RWacquiredAmount == 0) {
    LockHandler_AcquireGIL(rctx);  // prevent deadlocks!!
    int res = pthread_rwlock_wrlock(&lockRW);
    assert(res == 0);
    LockHandler_ReleaseGIL(rctx);
    lh->RWacquiredType = AcquiredType_WRITE;
  }

  ++lh->RWacquiredAmount;
}

void LockHandler_ReleaseWrite(RedisModuleCtx* rctx) {
  LockHandlerCtx* lh = LockHandler_GetSpecific();
  assert(lh);
  assert(lh->RWacquiredAmount > 0);
  assert(lh->RWacquiredType == AcquiredType_WRITE);
  if (--lh->RWacquiredAmount == 0) {
    pthread_rwlock_unlock(&lockRW);
    lh->RWacquiredType = AcquiredType_NONE;
  }
}
