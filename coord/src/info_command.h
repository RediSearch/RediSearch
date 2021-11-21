
#pragma once

#include "redismodule.h"
#include "rmr/rmr.h"
#include "rmr/reply.h"

int InfoReplyReducer(struct MRCtx *mc, int count, MRReply **replies);
