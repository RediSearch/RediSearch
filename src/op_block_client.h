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

/* One-shot operational blocking without query registration or timeout state.
 * The worker owns the blocked-client handle until Unblock, including after
 * client disconnect. Reply contexts must be freed before Unblock. */
RedisModuleBlockedClient *OpBlockClient_Block(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_cb);
void OpBlockClient_Unblock(RedisModuleBlockedClient *bc);

#endif
