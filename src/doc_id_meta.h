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

void DocIdMeta_Init(RedisModuleCtx *ctx);

int DocIdMeta_Set(RedisModuleKey *key, uint64_t docId);

uint64_t DocIdMeta_Get(RedisModuleKey *key);
