/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Internal header for fork GC pipe I/O utilities and shared declarations.
// Not part of the public API — only included by src/fork_gc/*.c files.

#ifndef FORK_GC_PIPE_H_
#define FORK_GC_PIPE_H_

#include "fork_gc.h"
#include "fork_gc_ffi.h"
#include "search_ctx.h"
#include "inverted_index.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/uio.h>

//------------------------------------------------------------------------------
// Pipe I/O primitives
//------------------------------------------------------------------------------

#define FGC_SEND_VAR(fgc, v) FGC_sendFixed(fgc, &v, sizeof v)

// Sentinel length value sent over the pipe to signal end-of-stream.
#define NO_MORE_DATA SIZE_MAX

//------------------------------------------------------------------------------
// Pipe read/write callbacks for II GC
//------------------------------------------------------------------------------

// Glue to use process pipe as writer for II GC delta info.
void pipe_write_cb(void *ctx, const void *buf, size_t len);

// Glue to use process pipe as reader for II GC delta info.
int pipe_read_cb(void *ctx, void *buf, size_t len);

//------------------------------------------------------------------------------
// Shared helpers
//------------------------------------------------------------------------------

// Context for inverted-index GC callbacks that send data over the pipe.
typedef struct {
  ForkGC *gc;
  void *hdrarg;
} CTX_II_GC_Callback;

// Send an iovec-based header string over the pipe. Used by terms, missing_docs, existing_docs.
void sendHeaderString(void *ptrCtx);

// Update index and GC stats after applying a delta.
void FGC_updateStats(ForkGC *gc, RedisSearchCtx *sctx,
                     size_t recordsRemoved, size_t bytesCollected,
                     size_t bytesAdded, bool ignoredLastBlock);

//------------------------------------------------------------------------------
// Per-index-kind child collectors and parent handlers
//------------------------------------------------------------------------------

void FGC_childCollectTerms(ForkGC *gc, RedisSearchCtx *sctx);
FGCError FGC_parentHandleTerms(ForkGC *gc);

void FGC_childCollectNumeric(ForkGC *gc, RedisSearchCtx *sctx);
FGCError FGC_parentHandleNumeric(ForkGC *gc);

void FGC_childCollectTags(ForkGC *gc, RedisSearchCtx *sctx);
FGCError FGC_parentHandleTags(ForkGC *gc);

void FGC_childCollectMissingDocs(ForkGC *gc, RedisSearchCtx *sctx);
FGCError FGC_parentHandleMissingDocs(ForkGC *gc);

void FGC_childCollectExistingDocs(ForkGC *gc, RedisSearchCtx *sctx);
FGCError FGC_parentHandleExistingDocs(ForkGC *gc);

#endif /* FORK_GC_PIPE_H_ */
