/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

/** This unit includes:
 * 1. An implementation of pthread_barrier_t for systems that do not support the POSIX pthread_barrier_t API, such as MacOS.
 * 2. A wrapper for pthread_barrier_t that extends the API with additional functionality, such as waiting for all the threads
 *    to pass the barrier before destroying the barrier.
 *  @note Currently, barrier_t does not provide an API that allows it to be reused. If reuse is required,
 *  the barrier counter should be reset to 0 before the next use. */

#include <pthread.h>
#include <stdatomic.h>

/* ============= implementation for MacOS ============= */
#if !defined _POSIX_BARRIERS || _POSIX_BARRIERS < 0
/** implementation for macos inspired by
 * http://byronlai.com/jekyll/update/2015/12/26/barrier.html
 */

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t condition_variable;
    int threads_required;
    int threads_left;
    volatile unsigned int cycle;
    volatile size_t received;
    pthread_t creator_id;
} pthread_barrier_t;

int pthread_barrier_init(pthread_barrier_t *barrier, void *attr, int count);

int pthread_barrier_wait(pthread_barrier_t *barrier);

int pthread_barrier_destroy( pthread_barrier_t *barrier);

#endif // !defined _POSIX_BARRIERS || _POSIX_BARRIERS < 0

/* ============= General API extension ============= */
typedef struct {
    pthread_barrier_t barrier;
    int count;
    volatile atomic_size_t received;
} barrier_t;

int barrier_init(barrier_t *barrier, void *attr, int count);

int barrier_wait(barrier_t *barrier);

/** The results are undefined if pthread_barrier_destroy() is called when any thread is blocked
 * on the barrier (that is, has not returned from the pthread_barrier_wait() call).
 * This function guarantees safe destruction of the barrier */
int barrier_wait_and_destroy(barrier_t *barrier);
