/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "op_block_client.h"

RedisModuleBlockedClient *OpBlockClient_Block(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_cb) {
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, reply_cb, NULL, NULL, 0);
  RedisModule_BlockedClientMeasureTimeStart(bc);
  return bc;
}

void OpBlockClient_Unblock(RedisModuleBlockedClient *bc) {
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
}
