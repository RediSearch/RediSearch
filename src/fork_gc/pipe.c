/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"
#include "module.h"
#include "rmutil/rm_assert.h"
#include <unistd.h>
#include <poll.h>
#include <errno.h>
#include <string.h>

void *RECV_BUFFER_EMPTY = (void *)0x0deadbeef;

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

// FGC_sendBuffer and FGC_sendTerminator are defined in Rust
// (fork_gc_ffi crate); prototypes come from fork_gc_rs.h.

// FGC_recvFixed is defined in Rust (fork_gc_ffi crate); prototype comes
// from fork_gc_rs.h.

int __attribute__((warn_unused_result))
FGC_recvBuffer(ForkGC *fgc, void **buf, size_t *len) {
  size_t temp_len;
  if (FGC_recvFixed(fgc, &temp_len, sizeof temp_len) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  if (temp_len == SIZE_MAX) {
    *len = temp_len;
    *buf = RECV_BUFFER_EMPTY;
    return REDISMODULE_OK;
  }
  if (temp_len == 0) {
    *len = temp_len;
    *buf = NULL;
    return REDISMODULE_OK;
  }

  char *buf_data = rm_malloc(temp_len + 1);
  buf_data[temp_len] = 0;
  if (FGC_recvFixed(fgc, buf_data, temp_len) != REDISMODULE_OK) {
    rm_free(buf_data);
    return REDISMODULE_ERR;
  }
  *len = temp_len;
  *buf = buf_data;
  return REDISMODULE_OK;
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

// If anything other than FGC_COLLECTED is returned, it is an error or done
FGCError recvFieldHeader(ForkGC *fgc, char **fieldName, size_t *fieldNameLen,
                         uint64_t *id) {
  if (FGC_recvBuffer(fgc, (void **)fieldName, fieldNameLen) != REDISMODULE_OK) {
    return FGC_PARENT_ERROR;
  }
  if (*fieldName == RECV_BUFFER_EMPTY) {
    *fieldName = NULL;
    return FGC_DONE;
  }

  if (FGC_recvFixed(fgc, id, sizeof(*id)) != REDISMODULE_OK) {
    rm_free(*fieldName);
    *fieldName = NULL;
    return FGC_PARENT_ERROR;
  }
  return FGC_COLLECTED;
}
