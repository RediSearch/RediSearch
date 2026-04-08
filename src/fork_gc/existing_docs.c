/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <bits/types/struct_iovec.h>  // for iovec
#include <stddef.h>                   // for NULL, size_t

#include "pipe.h"                     // for pipe_read_cb, pipe_write_cb
#include "fork_gc.h"                  // for ForkGC
#include "inverted_index.h"           // for II_GCScanStats, ...
#include "redismodule.h"              // for REDISMODULE_OK
#include "rmalloc.h"                  // for rm_free
#include "search_ctx.h"               // for RedisSearchCtx, ...
#include "spec.h"                     // for IndexSpec, IndexSpecRef_Promote
#include "util/references.h"          // for StrongRef_Get, StrongRef

void FGC_childCollectExistingDocs(ForkGC *gc, RedisSearchCtx *sctx) {
  IndexSpec *spec = sctx->spec;

  InvertedIndex *idx = spec->existingDocs;
  if (idx) {
    struct iovec iov = {.iov_base = (void *)"", 0};

    CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &iov };
    II_GCCallback cb = { .ctx = &cbCtx, .call = sendHeaderString };

    II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

    InvertedIndex_GcDelta_Scan(
        &wr, sctx, idx,
        &cb, NULL
    );
  }

  // we are done with existing docs inverted index
  FGC_sendTerminator(gc);
}

FGCError FGC_parentHandleExistingDocs(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;

  size_t ei_len;
  char *empty_indicator = NULL;

  if (FGC_recvBuffer(gc, (void **)&empty_indicator, &ei_len) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  if (empty_indicator == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  II_GCScanStats info = {0};
  II_GCReader rd = { .ctx = gc, .read = pipe_read_cb };
  InvertedIndexGcDelta *delta = InvertedIndex_GcDelta_Read(&rd);

  if (delta == NULL) {
    rm_free(empty_indicator);
    return FGC_CHILD_ERROR;
  }

  RedisSearchCtx sctx_;
  RedisSearchCtx *sctx = NULL;
  InvertedIndex *idx = NULL;

  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    status = FGC_SPEC_DELETED;
    goto cleanup;
  }

  sctx_ = SEARCH_CTX_STATIC(gc->ctx, sp);
  sctx = &sctx_;

  RedisSearchCtx_LockSpecWrite(sctx);

  idx = sp->existingDocs;

  InvertedIndex_ApplyGcDelta(idx, delta, &info);
  delta = NULL;

  // We don't count the records that we removed, because we also don't count
  // their addition (they are duplications so we have no such desire).

  if (InvertedIndex_NumDocs(idx) == 0) {
    // inverted index was cleaned entirely, let's free it
    info.bytes_freed += InvertedIndex_MemUsage(idx);
    InvertedIndex_Free(idx);
    sp->existingDocs = NULL;
  }
  FGC_updateStats(gc, sctx, 0, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

cleanup:
  rm_free(empty_indicator);
  if (sp) {
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
  }

  InvertedIndex_GcDelta_Free(delta);

  return status;
}
