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

#include <stdbool.h>
#include "redismodule.h"

/* The callback uses no Redis API/GIL; its context stays live until Stop returns. */
bool DiskMetrics_Start(RedisModuleCtx* ctx, bool (*collect)(void*), void* collector);
void DiskMetrics_Stop(RedisModuleCtx* ctx);
void DiskMetrics_Pause(void);
bool DiskMetrics_Resume(void);
bool DiskMetrics_Wake(void);
bool DiskMetrics_InForkChild(void);

#endif
