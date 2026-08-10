/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef QUERY_REQUEST_H__
#define QUERY_REQUEST_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  QUERY_REQUEST_KIND_AREQ,
  QUERY_REQUEST_KIND_HYBRID,
} QueryRequestKind;

typedef struct {
  uint64_t id;
} CursorInfo;

typedef struct QueryRequest {
  QueryRequestKind kind;
  CursorInfo cursorInfo;
} QueryRequest;

void QueryRequest_Init(QueryRequest *request, QueryRequestKind kind);

#ifdef __cplusplus
}
#endif

#endif
