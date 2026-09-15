/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "redismodule.h"

#include "rmr/reply.h"
#include "rmr/rmr.h"

// Internal shard payload; copies the node identity before serializing schemas.
int IndexList_ReplyLocalPayload(RedisModuleCtx *ctx);

// A single shard has no peers to disagree with; all its local indexes report ok.
int IndexList_ReplySingleShard(RedisModuleCtx *ctx);

// Folds shard payloads into the public per-index consistency reply.
int IndexListClusterStateReducer(struct MRCtx *mc, int count, MRReply **replies);
