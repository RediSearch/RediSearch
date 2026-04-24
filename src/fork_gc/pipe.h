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
#include "search_ctx.h"
#include "inverted_index.h"
#include <stddef.h>
#include <stdbool.h>
#include <sys/uio.h>

typedef enum {
  // Terms have been collected
  FGC_COLLECTED,
  // No more terms remain
  FGC_DONE,
  // Pipe error, child probably crashed
  FGC_CHILD_ERROR,
  // Error on the parent
  FGC_PARENT_ERROR,
  // The spec was deleted
  FGC_SPEC_DELETED,
} FGCError;

// Sentinel value indicating an empty/terminator buffer was received.
extern void *RECV_BUFFER_EMPTY;

//------------------------------------------------------------------------------
// Pipe I/O primitives
//------------------------------------------------------------------------------

#define FGC_SEND_VAR(fgc, v) FGC_sendFixed(fgc, &v, sizeof v)

void FGC_sendBuffer(ForkGC *fgc, const void *buff, size_t len);

// Send instead of a string to indicate that no more buffers are to be received.
void FGC_sendTerminator(ForkGC *fgc);

int __attribute__((warn_unused_result)) FGC_recvFixed(ForkGC *fgc, void *buf, size_t len);

int __attribute__((warn_unused_result)) FGC_recvBuffer(ForkGC *fgc, void **buf, size_t *len);

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

// Receive a field header (field name + unique id). Used by numeric and tags.
// Returns FGC_COLLECTED on success, FGC_DONE when no more fields, or an error.
FGCError recvFieldHeader(ForkGC *fgc, char **fieldName, size_t *fieldNameLen, uint64_t *id);

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
