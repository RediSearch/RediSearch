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

// Cursor callback for network responses: re-dispatches cursor reads, handles
// errors, and pushes replies onto the iterator channel.
void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep);

// Helper function to extract total_results from a shard reply.
// Returns true if total_results was found, false otherwise.
bool extractTotalResults(MRReply *rep, MRCommand *cmd, long long *out_total);

// Read the accumulated WITHCOUNT total (sum of each shard's total_results) from
// an FT.AGGREGATE iterator's private context. Defined in dist_aggregate.c.
long long AggregateIterator_GetTotalResults(const MRIterator *it);

#ifdef __cplusplus
}
#endif
