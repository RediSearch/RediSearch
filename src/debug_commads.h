/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef SRC_DEBUG_COMMADS_H_
#define SRC_DEBUG_COMMADS_H_

#include "redismodule.h"

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* SRC_DEBUG_COMMADS_H_ */
