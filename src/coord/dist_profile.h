/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <stdbool.h>             // for bool

#include "reply.h"               // for RedisModule_Reply
#include "rmr/reply.h"           // for MRReply
#include "../profile/options.h"  // for ProfileOptions
#include "query_error.h"         // for QueryError
#include "rmutil/args.h"         // for ArgsCursor

typedef struct PrintShardProfile_ctx {
  MRReply **replies;
  int count;
  bool isSearch;
} PrintShardProfile_ctx;

// Parse profile options, returns REDISMODULE_OK if parsing succeeded,
// otherwise returns REDISMODULE_ERR
int ParseProfile(ArgsCursor *ac, QueryError *status, ProfileOptions *options);

void PrintShardProfile(RedisModule_Reply *reply, void *ctx);
