/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef REDISMOCK_H
#define REDISMOCK_H
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*RMCKModuleLoadFunction)(RedisModuleCtx *, RedisModuleString **, int);

void RMCK_Bootstrap(RMCKModuleLoadFunction fn, const char **s, size_t n);

void RMCK_Notify(const char *action, int events, const char *key);

// Destroy all globals
void RMCK_Shutdown(void);

#ifdef __cplusplus
}
#endif
#endif
