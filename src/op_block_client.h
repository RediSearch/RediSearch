/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef OP_BLOCK_CLIENT_H__
#define OP_BLOCK_CLIENT_H__

#include "redismodule.h"

/* A one-shot operational job, without query registration, timeout state or
 * request ownership. The worker owns this context until Unblock, including
 * after client disconnect. Reply contexts must be freed before Unblock. */
typedef struct OpBlockClientCtx {
  RedisModuleBlockedClient *bc;
} OpBlockClientCtx;

OpBlockClientCtx *OpBlockClientCtx_New(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_cb);
void OpBlockClientCtx_Unblock(OpBlockClientCtx *op);

#endif
