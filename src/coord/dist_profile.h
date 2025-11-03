/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "reply.h"
#include "rmr/reply.h"
#include "../profile/options.h"

typedef struct PrintShardProfile_ctx {
  MRReply **replies;
  int count;
  bool isSearch;
} PrintShardProfile_ctx;

// Parse profile options, returns REDISMODULE_OK if parsing succeeded, otherwise returns REDISMODULE_ERR
int ParseProfile(ArgsCursor *ac, QueryError *status, ProfileOptions *options);

void PrintShardProfile(RedisModule_Reply *reply, void *ctx);