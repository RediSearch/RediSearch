/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "cluster.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Parse the cluster topology from the given arguments.
 * On success, returns the parsed topology. On failure, replies with an error
 * using the provided context and returns NULL.
 *
 * The `my_shard_idx` output parameter is set to the index of the shard
 * corresponding to MYID, or UINT32_MAX if MYID does not correspond to any shard
 * in the topology.
 */
MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, uint32_t *my_shard_idx);

#ifdef __cplusplus
}
#endif
