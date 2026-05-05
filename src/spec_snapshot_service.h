/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "search_ctx.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SpecSnapshotServiceRequest SpecSnapshotServiceRequest;

// Enqueue a snapshot-lock request and wait until the service thread attempts a
// non-blocking read lock. Returns an opaque request handle on success, or NULL
// if the snapshot lock could not be acquired.
SpecSnapshotServiceRequest *SpecSnapshotService_Request(RedisSearchCtx *searchCtx, uint32_t expectedSignals);

// Signal that one participant has completed its snapshot-lock handoff.
void SpecSnapshotService_Signal(SpecSnapshotServiceRequest *request);

#ifdef __cplusplus
}
#endif
