#include "rwlock.h"
#include "rmalloc.h"
#include "util/arr_rm_alloc.h"
#include <assert.h>

void RediSearch_LockRead(IndexSpec *sp);
void RediSearch_LockWrite(IndexSpec *sp);
void RediSearch_LockRelease(IndexSpec *sp);

void RediSearch_LockRead(IndexSpec *sp) {
  assert(sp != NULL);
  pthread_rwlock_rdlock(&sp->rwlock);
  sp->writeLocked = false;
}

void RediSearch_LockWrite(IndexSpec *sp) {
  assert(sp != NULL);
  pthread_rwlock_wrlock(&sp->rwlock);
  sp->writeLocked = true;
}

void RediSearch_LockRelease(IndexSpec *sp) {
  assert(sp != NULL);
  pthread_rwlock_unlock(&sp->rwlock);
  sp->writeLocked = false;
}

