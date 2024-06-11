/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "barrier.h"
#include <unistd.h>

/* ============= General API extension ============= */

int barrier_init(barrier_t *barrier, void *attr, int count) {
    int ret = pthread_barrier_init(&barrier->barrier, attr, count);
    barrier->count = count;
    barrier->received = 0;
    return ret;
}

int barrier_wait(barrier_t *barrier) {
    int ret = pthread_barrier_wait(&barrier->barrier);
    barrier->received++;
    return ret;
}

int barrier_destroy(barrier_t *barrier) {
  return pthread_barrier_destroy(&barrier->barrier);
}

int barrier_wait_for_threads_and_destroy(barrier_t *barrier) {
    // Wait for the threads to exit the barrier_wait to safely destroy the barrier.
    while (barrier->received < barrier->count) {
      usleep(1);
   }

   return pthread_barrier_destroy(&barrier->barrier);
}

/* ============= implementation for MacOS ============= */
#if !defined _POSIX_BARRIERS || _POSIX_BARRIERS < 0

#define PTHREAD_BARRIER_SERIAL_THREAD -1

int pthread_barrier_init(pthread_barrier_t *barrier, void *attr, int count) {
    barrier->threads_required = count;
    barrier->threads_left = count;
    barrier->cycle = 0;
    barrier->creator_id = pthread_self();
    pthread_mutex_init(&barrier->mutex, NULL);
    pthread_cond_init(&barrier->condition_variable, NULL);
    return 0;
}
#define PTHREAD_BARRIER_SERIAL_THREAD -1

int pthread_barrier_wait(pthread_barrier_t *barrier) {
    pthread_mutex_lock(&barrier->mutex);
    int ret = 0;
    if (--barrier->threads_left == 0) {
        barrier->cycle++;
        barrier->threads_left = barrier->threads_required;

        pthread_cond_broadcast(&barrier->condition_variable);
        pthread_mutex_unlock(&barrier->mutex);

        ret = PTHREAD_BARRIER_SERIAL_THREAD;
    } else {
        unsigned int cycle = barrier->cycle;

        while (cycle == barrier->cycle)
            pthread_cond_wait(&barrier->condition_variable, &barrier->mutex);

        pthread_mutex_unlock(&barrier->mutex);
    }
    ++barrier->received;
    if (pthread_equal(pthread_self(), barrier->creator_id)) {
        while (barrier->received < barrier->threads_required)
            usleep(1);
        barrier->received = 0;
    }
    return ret;
}

int pthread_barrier_destroy( pthread_barrier_t *barrier) {
  pthread_cond_destroy(&barrier->condition_variable);
  pthread_mutex_destroy(&barrier->mutex);
  return 0;
}

#endif // !defined _POSIX_BARRIERS || _POSIX_BARRIERS < 0
