/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "redismodule.h"
#include "rmr/rmr.h"
#include "rmr/reply.h"

int InfoReplyReducer(struct MRCtx *mc, int count, MRReply **replies);
