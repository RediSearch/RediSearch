/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief A binary semaphore used as the per-IndexSpec fork lock.
 *
 * Semaphore semantics: acquire blocks until a permit is available; release
 * may be called from a thread different from the one that acquired (so this
 * is not interchangeable with a plain pthread_mutex).
 *
 * Backed by `sem_t` on Linux. On macOS, POSIX unnamed semaphores
 * (`sem_init`) are unimplemented, so all operations compile out as no-ops to
 * keep OSS builds working on Darwin developer machines.
 */
#ifdef __APPLE__
typedef struct ForkLock {
  int _unused;  // C forbids empty structs; keep ABI-stable on Darwin.
} ForkLock;
#else
#include <semaphore.h>
typedef struct ForkLock {
  sem_t sem;
} ForkLock;
#endif

/** Initialise a ForkLock with a single permit (binary semaphore). */
void ForkLock_Init(ForkLock *fl);

/** Tear down a ForkLock. Must not be called while any thread is parked on it. */
void ForkLock_Destroy(ForkLock *fl);

/** Block until a permit is available, then take it. Retries on EINTR. */
void ForkLock_Acquire(ForkLock *fl);

/** Return a permit. Safe to call from a thread different from the acquirer. */
void ForkLock_Release(ForkLock *fl);

#ifdef __cplusplus
}
#endif
