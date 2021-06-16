#include "rwlock.h"
#include "rmalloc.h"
#include "util/arr_rm_alloc.h"
#include <assert.h>
#include "spec.h"

void RediSearch_LockInit(IndexSpec *sp) {
  sp->rwLocksData = array_new(rwlockThreadLocal*, 10);
  pthread_mutex_init(&sp->rwLocksDataMutex, NULL);
  int err = pthread_key_create(&sp->lockKey, NULL);
  RS_LOG_ASSERT(!err, "failed creating index level lockKey");
  err = pthread_rwlock_init(&sp->rwlock, NULL);
  RS_LOG_ASSERT(!err, "failed creating index level R/W lock");
}

static rwlockThreadLocal* RediSearch_GetLockThreadData(IndexSpec *sp) {
  rwlockThreadLocal* rwData = pthread_getspecific(sp->lockKey);
  if (!rwData) {
    rwData = rm_malloc(sizeof(*rwData));
    rwData->locked = 0;
    rwData->type = lockType_None;
    pthread_setspecific(sp->lockKey, rwData);
    pthread_mutex_lock(&sp->rwLocksDataMutex);
    sp->rwLocksData = array_append(sp->rwLocksData, rwData);
    pthread_mutex_unlock(&sp->rwLocksDataMutex);
  }
  return rwData;
}

void RediSearch_LockRead(IndexSpec *sp) {
  assert(sp != NULL);
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData(sp);
  assert(rwData->type != lockType_Write);
  if (rwData->locked == 0) {
    pthread_rwlock_rdlock(&sp->rwlock);
    rwData->type = lockType_Read;
  }
  assert(rwData->type == lockType_Read);
  ++rwData->locked;
}

void RediSearch_LockWrite(IndexSpec *sp) {
  assert(sp != NULL);
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData(sp);
  assert(rwData->type != lockType_Read);
  if (rwData->locked == 0) {
    pthread_rwlock_wrlock(&sp->rwlock);
    rwData->type = lockType_Write;
  }
  assert(rwData->type == lockType_Write);
  ++rwData->locked;
}

void RediSearch_LockRelease(IndexSpec *sp) {
  assert(sp != NULL);
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData(sp);
  assert(rwData->locked > 0);
  if ((--rwData->locked) == 0) {
    pthread_rwlock_unlock(&sp->rwlock);
    rwData->type = lockType_None;
  }
}

void RediSearch_LockDestory(IndexSpec *sp) {
  pthread_rwlock_destroy(&sp->rwlock);
  pthread_mutex_lock(&sp->rwLocksDataMutex);
  for (size_t i = 0; i < array_len(sp->rwLocksData); ++i) {
    rm_free(sp->rwLocksData[i]);
  }
  array_free(sp->rwLocksData);
  pthread_mutex_unlock(&sp->rwLocksDataMutex);
}
