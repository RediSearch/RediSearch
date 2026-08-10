/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "query_request.h"

#include "query_error_ffi.h"

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind) {
  request->kind = kind;
  request->cursorInfo.id = 0;
  request->reply = (ChunkReplyState) {0};
  request->reply.err = QueryError_Default();
}

void QueryRequest_ResetReply(QueryRequest *request) {
  QueryError_ClearError(&request->reply.err);
  request->reply = (ChunkReplyState) {0};
  request->reply.err = QueryError_Default();
}

void QueryRequest_Destroy(QueryRequest *request) {
  QueryError_ClearError(&request->reply.err);
}
