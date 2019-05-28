#ifndef SRC_RWLOCK_H_
#define SRC_RWLOCK_H_

#include <pthread.h>

extern pthread_rwlock_t RWLock;

#define RWLOCK_ACQUIRE_READ() pthread_rwlock_rdlock(&RWLock)
#define RWLOCK_ACQUIRE_WRITE() pthread_rwlock_wrlock(&RWLock)
#define RWLOCK_RELEASE() pthread_rwlock_unlock(&RWLock)

#endif /* SRC_RWLOCK_H_ */
