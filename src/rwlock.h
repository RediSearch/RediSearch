#ifndef SRC_RWLOCK_H_
#define SRC_RWLOCK_H_

#include <pthread.h>
#include "spec.h"
#include "redismodule.h"

void RediSearch_LockRead(IndexSpec* sp);
void RediSearch_LockWrite(IndexSpec* sp);
void RediSearch_LockRelease(IndexSpec* sp);

#define RWLOCK_ACQUIRE_READ(sp) RediSearch_LockRead(sp)
#define RWLOCK_ACQUIRE_WRITE(sp) RediSearch_LockWrite(sp)
#define RWLOCK_RELEASE(sp) RediSearch_LockRelease(sp)

#endif /* SRC_RWLOCK_H_ */

