#ifndef SRC_RWLOCK_H_
#define SRC_RWLOCK_H_

#include <pthread.h>
#include "redismodule.h"

typedef struct IndexSpec IndexSpec; // Forward declaration

void RediSearch_LockInit(IndexSpec *sp);
void RediSearch_LockRead(IndexSpec* sp);
void RediSearch_LockWrite(IndexSpec* sp);
void RediSearch_LockRelease(IndexSpec* sp);
void RediSearch_LockDestory(IndexSpec *sp);

#define RWLOCK_ACQUIRE_READ(sp) RediSearch_LockRead(sp)
#define RWLOCK_ACQUIRE_WRITE(sp) RediSearch_LockWrite(sp)
#define RWLOCK_RELEASE(sp) RediSearch_LockRelease(sp)

#endif /* SRC_RWLOCK_H_ */
