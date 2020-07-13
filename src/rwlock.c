#include "rwlock.h"
#include "rmalloc.h"
#include "util/arr_rm_alloc.h"
#include <assert.h>

pthread_mutex_t rwLockMutex;
pthread_rwlock_t RWLock = PTHREAD_RWLOCK_INITIALIZER;
pthread_key_t _lockKey;

typedef enum lockType { lockType_None, lockType_Read, lockType_Write } lockType;

typedef struct rwlockThreadLocal {
  size_t locked;
  lockType type;
} rwlockThreadLocal;

rwlockThreadLocal** rwlocks;

int RediSearch_LockInit(RedisModuleCtx* ctx) {
  rwlocks = array_new(rwlockThreadLocal*, 10);
  pthread_mutex_init(&rwLockMutex, NULL);
  int err = pthread_key_create(&_lockKey, NULL);
  if (err) {
    if (ctx) {
      RedisModule_Log(ctx, "warning", "could not initialize rwlock thread local");
    }
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static rwlockThreadLocal* RediSearch_GetLockThreadData() {
  rwlockThreadLocal* rwData = pthread_getspecific(_lockKey);
  if (!rwData) {
    rwData = rm_malloc(sizeof(*rwData));
    rwData->locked = 0;
    rwData->type = lockType_None;
    pthread_setspecific(_lockKey, rwData);
    pthread_mutex_lock(&rwLockMutex);
    rwlocks = array_append(rwlocks, rwData);
    pthread_mutex_unlock(&rwLockMutex);
  }
  return rwData;
}

void RediSearch_LockRead() {
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData();
  assert(rwData->type != lockType_Write);
  if (rwData->locked == 0) {
    pthread_rwlock_rdlock(&RWLock);
    rwData->type = lockType_Read;
  }
  assert(rwData->type == lockType_Read);
  ++rwData->locked;
}

void RediSearch_LockWrite() {
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData();
  assert(rwData->type != lockType_Read);
  if (rwData->locked == 0) {
    pthread_rwlock_wrlock(&RWLock);
    rwData->type = lockType_Write;
  }
  assert(rwData->type == lockType_Write);
  ++rwData->locked;
}

void RediSearch_LockRelease() {
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData();
  assert(rwData->locked > 0);
  if ((--rwData->locked) == 0) {
    pthread_rwlock_unlock(&RWLock);
    rwData->type = lockType_None;
  }
}

void RediSearch_LockDestory() {
  pthread_mutex_lock(&rwLockMutex);
  for (size_t i = 0; i < array_len(rwlocks); ++i) {
    rm_free(rwlocks[i]);
  }
  array_free(rwlocks);
  pthread_mutex_unlock(&rwLockMutex);
}
