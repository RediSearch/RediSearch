/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

/* Internal layout of MRConn and test-visible callbacks.
 *
 * Intended for conn.c and for unit tests that need to exercise internal
 * callbacks in isolation. Do not include from coordinator production code;
 * use the opaque MRConn type from conn.h instead. */

#include "conn.h"

#ifdef __cplusplus
extern "C" {
#endif

struct MRConn {
  MREndpoint ep;
  redisAsyncContext *conn;
  void *timer;
  uv_loop_t *loop;
  int protocol; // 0 (undetermined), 2, or 3
  MRConnState state;
  unsigned authFailCount; // consecutive auth failures, for rate-limited logging
};

/* Reply callback for the pipelined HELLO sent by MRConn_SendCommand. Clears
 * conn->protocol on transport or server error so the next send re-issues
 * HELLO. Exposed for unit testing; not part of the public API. */
void MRConn_HelloCallback(redisAsyncContext *c, void *r, void *privdata);

#ifdef __cplusplus
}
#endif
