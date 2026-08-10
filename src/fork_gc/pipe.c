/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"

#include <sys/uio.h>

// Assumes the spec is locked.
void FGC_updateStats(ForkGC *gc, RedisSearchCtx *sctx,
                     size_t recordsRemoved, size_t bytesCollected,
                     size_t bytesAdded, bool ignoredLastBlock) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize += bytesAdded;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
  gc->stats.totalCollected -= bytesAdded;
  gc->stats.gcBlocksDenied += ignoredLastBlock ? 1 : 0;
}

// glue to use process pipe as writer for II GC delta info
void pipe_write_cb(void *ctx, const void *buf, size_t len) {
  ForkGC *gc = ctx;
  FGC_sendFixed(gc, buf, len);
}

// glue to use process pipe as reader for II GC delta info
int pipe_read_cb(void *ctx, void *buf, size_t len) {
  ForkGC *gc = ctx;
  return FGC_recvFixed(gc, buf, len);
}

void sendHeaderString(void* ptrCtx) {
  CTX_II_GC_Callback* ctx = ptrCtx;
  struct iovec *iov = ctx->hdrarg;
  FGC_sendBuffer(ctx->gc, iov->iov_base, iov->iov_len);
}

