/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "VecSim/vec_sim_common.h"

#ifdef __cplusplus
extern "C" {
#endif

// Called by RediSearch during module init to set the callbacks
void VecSimDisk_SetThrottleCallbacks(ThrottleCB enable, ThrottleCB disable);

// Invoke the throttle callbacks set by RediSearch.
// Called by TieredHNSWDiskIndex when flat buffer reaches/leaves capacity.
void VecSimDisk_InvokeEnableThrottle(void);
void VecSimDisk_InvokeDisableThrottle(void);

#ifdef __cplusplus
}
#endif
