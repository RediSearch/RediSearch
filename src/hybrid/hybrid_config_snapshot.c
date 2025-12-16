/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hybrid_config_snapshot.h"
#include "rmalloc.h"

extern RSConfig RSGlobalConfig;

HybridConfigSnapshot *HybridConfigSnapshot_Create() {
    HybridConfigSnapshot *snapshot = rm_calloc(1, sizeof(HybridConfigSnapshot));

    // Capture RSGlobalConfig values
    snapshot->requestConfig = RSGlobalConfig.requestConfigParams;
    snapshot->maxSearchResults = RSGlobalConfig.maxSearchResults;
    snapshot->cursorMaxIdle = RSGlobalConfig.cursorMaxIdle;

    return snapshot;
}

void HybridConfigSnapshot_Free(void *snapshot) {
    if (snapshot) {
        rm_free(snapshot);
    }
}

