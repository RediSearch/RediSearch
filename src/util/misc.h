/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_MISC_H
#define RS_MISC_H

#include "redismodule.h"

/**
 * This handler crashes
 */
void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value);

//null-unsafe
int GetRedisErrorCodeLength(const char* error);

#endif
