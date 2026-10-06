/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef RS_DISK_METRICS_H
#define RS_DISK_METRICS_H

#include "redismodule.h"
#include <stdbool.h>

/* The callback uses no Redis API/GIL; its context stays live until Stop returns. */
bool DiskMetrics_Start(RedisModuleCtx* ctx, bool (*collect)(void*, bool periodic), void* collector);
void DiskMetrics_Request(void);
void DiskMetrics_Stop(RedisModuleCtx* ctx);
bool DiskMetrics_InForkChild(void);

#endif
