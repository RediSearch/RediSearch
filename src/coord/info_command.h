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
#include "rmr/rmr.h"
#include "rmr/reply.h"

int InfoReplyReducer(struct MRCtx *mc, int count, MRReply **replies);

/* Reducer for FT.INFO ... PARTIAL: a shard that cannot report on the index is skipped and the
 * remaining shards are aggregated. Only an all-shards failure is fatal. */
int InfoReplyReducerPartial(struct MRCtx *mc, int count, MRReply **replies);
