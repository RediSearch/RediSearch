/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "throttle.h"
#include <cassert>

// Global callbacks - set once during module init, used by all tiered disk indexes
static ThrottleCB g_enableThrottleCB = nullptr;
static ThrottleCB g_disableThrottleCB = nullptr;

extern "C" void VecSimDisk_SetThrottleCallbacks(ThrottleCB enable, ThrottleCB disable) {
    g_enableThrottleCB = enable;
    g_disableThrottleCB = disable;
}

// Invoke the enable throttle callback set by RediSearch
void VecSimDisk_InvokeEnableThrottle() {
    assert(g_enableThrottleCB);
    g_enableThrottleCB();
}

// Invoke the disable throttle callback set by RediSearch
void VecSimDisk_InvokeDisableThrottle() {
    assert(g_disableThrottleCB);
    g_disableThrottleCB();
}
