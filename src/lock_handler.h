#ifndef SRC_LOCK_HANDLER_H_
#define SRC_LOCK_HANDLER_H_

#include "redismodule.h"

int LockHandler_Initialize();
void LockHandler_AcquireGIL(RedisModuleCtx* rctx);
void LockHandler_ReleaseGIL(RedisModuleCtx* rctx);
void LockHandler_AcquireRead(RedisModuleCtx* rctx);
void LockHandler_ReleaseRead(RedisModuleCtx* rctx);
void LockHandler_AcquireWrite(RedisModuleCtx* rctx);
void LockHandler_ReleaseWrite(RedisModuleCtx* rctx);

#endif /* SRC_LOCK_HANDLER_H_ */
