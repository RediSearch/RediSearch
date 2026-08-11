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

int RegisterLegacyTypes(RedisModuleCtx *ctx);

/* Cleanup of orphaned legacy index keys.
 *
 * A pre-2.0 index key is consumed and discarded on load, but the loader must return a non-NULL value
 * because NULL means load failure to Redis - so the key survives holding only a sentinel. The upgrade
 * sweep in indexes_scan.c deletes these, but only for indexes it can reach through a loaded spec and
 * a matching `UPGRADE_INDEX` argument, so keys arriving any other way are orphaned with nothing to
 * collect them. The entry points below cover the two ways they arrive. */

/* Delete `key` if it is an orphaned legacy key. Call from the `restore` keyspace notification:
 * `RESTORE` is how these keys reach a running server (Enterprise import replays RESTORE rather than
 * handing an RDB to Redis) and it fires no loading event. A no-op unless the key really is one. */
void LegacyTypes_CleanupRestoredKey(RedisModuleCtx *ctx, RedisModuleString *key);

