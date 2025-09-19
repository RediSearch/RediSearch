/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_RESP3_H_
#define RS_RESP3_H_

#include "redismodule.h"
#include "reply.h"


static inline bool is_resp3(RedisModuleCtx *ctx) {
    return RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_RESP3;
}

#endif
