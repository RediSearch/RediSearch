/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "cluster.h"

// forward declaration
struct RedisModuleCtx;

void UpdateTopology(struct RedisModuleCtx *ctx);
int InitRedisTopologyUpdater(struct RedisModuleCtx *ctx);
int StopRedisTopologyUpdater(RedisModuleCtx *ctx);
