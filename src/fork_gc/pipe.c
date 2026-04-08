/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"

#include <unistd.h>                   // for read, write
#include <poll.h>                     // for poll, pollfd, POLLERR, POLLHUP
#include <errno.h>                    // for errno, EINTR
#include <string.h>                   // for strerror
#include <bits/types/struct_iovec.h>  // for iovec
#include <stdio.h>                    // for perror
#include <sys/uio.h>                  // for ssize_t

#include "rmutil/rm_assert.h"         // for RS_LOG_ASSERT
#include "redismodule.h"              // for REDISMODULE_OK, REDISMODULE_ERR
#include "rmalloc.h"                  // for rm_free, rm_malloc
#include "spec.h"                     // for IndexSpec, IndexStats

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

// Buff shouldn't be NULL.
void FGC_sendFixed(ForkGC *fgc, const void *buff, size_t len) {
  RS_LOG_ASSERT(len > 0, "buffer length cannot be 0");
  ssize_t size = write(fgc->pipe_write_fd, buff, len);
  if (size != len) {
    perror("broken pipe, exiting GC fork: write() failed");
    // just exit, do not abort(), which will trigger a watchdog on RLEC, causing adverse effects
    RedisModule_Log(fgc->ctx, "warning", "GC fork: broken pipe, exiting");
    RedisModule_ExitFromChild(1);
  }
}

void FGC_sendBuffer(ForkGC *fgc, const void *buff, size_t len) {
  FGC_SEND_VAR(fgc, len);
  if (len > 0) {
    FGC_sendFixed(fgc, buff, len);
  }
}

/**
 * Send instead of a string to indicate that no more buffers are to be received
 */
void FGC_sendTerminator(ForkGC *fgc) {
  size_t smax = SIZE_MAX;
  FGC_SEND_VAR(fgc, smax);
}

int __attribute__((warn_unused_result)) FGC_recvFixed(ForkGC *fgc, void *buf, size_t len) {
  // poll the pipe, so that we don't block while read, with timeout of 3 minutes
  int poll_rc;
  while ((poll_rc = poll(fgc->pollfd_read, 1, 180000)) == 1) {
    ssize_t nrecvd = read(fgc->pipe_read_fd, buf, len);
    if (nrecvd > 0) {
      buf += nrecvd;
      len -= nrecvd;
    } else if (nrecvd <= 0 && errno != EINTR) {
      break;
    }
    if (len == 0) {
      return REDISMODULE_OK;
    }
  }
  short revents = fgc->pollfd_read[0].revents;
  const char *what = (poll_rc == 0) ? "timeout" : "error";
  RedisModule_Log(fgc->ctx, "warning", "ForkGC - got %s while reading from pipe. errno: %s, revents: 0x%x (POLLIN=%x POLLERR=%x POLLHUP=%x POLLNVAL=%x)",
                  what, strerror(errno), revents, (revents & POLLIN), (revents & POLLERR), (revents & POLLHUP), (revents & POLLNVAL));
  return REDISMODULE_ERR;
}

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
