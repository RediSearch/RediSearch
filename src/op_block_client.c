/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "op_block_client.h"
#include "rmalloc.h"

OpBlockClientCtx *OpBlockClientCtx_New(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_cb) {
  OpBlockClientCtx *op = rm_new(OpBlockClientCtx);
  op->bc = RedisModule_BlockClient(ctx, reply_cb, NULL, NULL, 0);
  RedisModule_BlockedClientMeasureTimeStart(op->bc);
  return op;
}

void OpBlockClientCtx_Unblock(OpBlockClientCtx *op) {
  RedisModule_BlockedClientMeasureTimeEnd(op->bc);
  RedisModule_UnblockClient(op->bc, NULL);
  rm_free(op);
}
