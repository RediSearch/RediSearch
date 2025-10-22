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

#define CURSOR_EOF 0

#ifdef __cplusplus
extern "C" {
#endif

void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep);
bool getCursorCommand(long long cursorId, MRCommand *cmd, MRIteratorCtx *ctx);

#ifdef __cplusplus
}
#endif
