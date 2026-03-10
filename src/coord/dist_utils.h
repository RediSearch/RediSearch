/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "../coord/rmr/rmr.h"
#include "rpnet.h"

#define CURSOR_EOF 0

#ifdef __cplusplus
extern "C" {
#endif

// Cursor callback for network responses that uses barrier passed via privateData
// privateData is expected to be a ShardResponseBarrier* (or NULL)
void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep);

// Cursor callback for network responses that takes barrier explicitly
// Use this when privateData is a different type that contains a ShardResponseBarrier*
void netCursorCallbackWithBarrier(MRIteratorCallbackCtx *ctx, MRReply *rep, ShardResponseBarrier *barrier);

#ifdef __cplusplus
}
#endif
