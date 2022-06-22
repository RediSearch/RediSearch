#pragma once

#include <pthread.h>

extern pthread_rwlock_t RWLock;

#define RWLOCK_ACQUIRE_READ() pthread_rwlock_rdlock(&RWLock)
#define RWLOCK_ACQUIRE_WRITE() pthread_rwlock_wrlock(&RWLock)
#define RWLOCK_RELEASE() pthread_rwlock_unlock(&RWLock)
