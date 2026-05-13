/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "fork_lock.h"

#ifdef __APPLE__

// macOS does not implement POSIX unnamed semaphores - sem_init() returns
// -1 / ENOSYS. SST replication is not a production target on Darwin, so the
// fork lock is compiled out as a no-op there.
void ForkLock_Init(ForkLock *fl) { (void)fl; }
void ForkLock_Destroy(ForkLock *fl) { (void)fl; }
void ForkLock_Acquire(ForkLock *fl) { (void)fl; }
void ForkLock_Release(ForkLock *fl) { (void)fl; }

#else

#include <errno.h>

void ForkLock_Init(ForkLock *fl) {
  // pshared=0: process-local. value=1: binary semaphore, starts free.
  sem_init(&fl->sem, 0, 1);
}

void ForkLock_Destroy(ForkLock *fl) {
  sem_destroy(&fl->sem);
}

void ForkLock_Acquire(ForkLock *fl) {
  // Loop on EINTR so signal delivery doesn't fail the acquire.
  while (sem_wait(&fl->sem) == -1 && errno == EINTR) {
    // retry
  }
}

void ForkLock_Release(ForkLock *fl) {
  sem_post(&fl->sem);
}

#endif
