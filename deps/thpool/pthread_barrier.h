/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#if !defined _POSIX_BARRIERS || _POSIX_BARRIERS < 0
/* implementation inspired by
 * http://byronlai.com/jekyll/update/2015/12/26/barrier.html
 */
#include <pthread.h>

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t condition_variable;
    int threads_required;
    int threads_left;
    volatile unsigned int cycle;
} pthread_barrier_t;

static int pthread_barrier_init(pthread_barrier_t *barrier, void *attr, int count) {
    barrier->threads_required = count;
    barrier->threads_left = count;
    barrier->cycle = 0;
    pthread_mutex_init(&barrier->mutex, NULL);
    pthread_cond_init(&barrier->condition_variable, NULL);
    return 0;
}
#define PTHREAD_BARRIER_SERIAL_THREAD -1

static int pthread_barrier_wait(pthread_barrier_t *barrier) {
    pthread_mutex_lock(&barrier->mutex);

    if (--barrier->threads_left == 0) {
        barrier->cycle++;
        barrier->threads_left = barrier->threads_required;

        pthread_cond_broadcast(&barrier->condition_variable);
        pthread_mutex_unlock(&barrier->mutex);

        return PTHREAD_BARRIER_SERIAL_THREAD;
    } else {
        unsigned int cycle = barrier->cycle;

        while (cycle == barrier->cycle)
            pthread_cond_wait(&barrier->condition_variable, &barrier->mutex);

        pthread_mutex_unlock(&barrier->mutex);
        return 0;
    }
}

int pthread_barrier_destroy( pthread_barrier_t *barrier) {
  pthread_cond_destroy(&barrier->condition_variable);
  pthread_mutex_destroy(&barrier->mutex);
  return 0;
}

#endif // !defined _POSIX_BARRIERS || _POSIX_BARRIERS < 0
