/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <string.h>
#include "../coord/rmr/rmr.h"
#include "rpnet.h"
#include "aggregate/internal_resp_schema.h"

#define CURSOR_EOF 0

#ifdef __cplusplus
extern "C" {
#endif

// The tag occupies a slot which is an integer in legacy RESP2 replies or a
// result map in RESP3, so user field names cannot collide with it.
static inline bool isRespSchemaReply(const MRReply *rows) {
  if (!rows || MRReply_Type(rows) != MR_REPLY_ARRAY || !MRReply_Length(rows)) return false;
  const MRReply *tag = MRReply_ArrayElement(rows, 0);
  if (!tag || (MRReply_Type(tag) != MR_REPLY_STRING && MRReply_Type(tag) != MR_REPLY_STATUS))
    return false;
  size_t len;
  const char *value = MRReply_String(tag, &len);
  return len == sizeof(INTERNAL_RESP_SCHEMA_TAG) - 1 &&
         !memcmp(value, INTERNAL_RESP_SCHEMA_TAG, len);
}

// Cursor callback for network responses: re-dispatches cursor reads, handles
// errors, and pushes replies onto the iterator channel.
void netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep);

// Helper function to extract total_results from a shard reply.
// Returns true if total_results was found, false otherwise.
bool extractTotalResults(MRReply *rep, MRCommand *cmd, long long *out_total);

#ifdef __cplusplus
}
#endif
