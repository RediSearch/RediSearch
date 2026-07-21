/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"
#include "inverted_index_ffi.h"
#include "numeric_range_tree_ffi.h"
#include "redis_index.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/hidden.h"
#include "iterators_ffi.h"

FGCError FGC_parentHandleNumeric(ForkGC *gc) {
  size_t fieldNameLen;
  char *fieldName = NULL;
  uint64_t rtUniqueId;
  NumericRangeTree *rt = NULL;
  FGCError status = recvFieldHeader(gc, &fieldName, &fieldNameLen, &rtUniqueId);
  if (status == FGC_DONE) {
    return FGC_DONE;
  }
  if (status != FGC_COLLECTED) {
    FGC_freeBuffer(fieldName, fieldNameLen);
    return status;
  }

  // Reusable buffer for entry data across loop iterations.
  char *entryData = NULL;
  size_t entryDataCap = 0;

  // Per-node streaming apply loop: read entries one at a time from the pipe.
  while (status == FGC_COLLECTED) {
    IndexSpec *sp = NULL;
    StrongRef spec_ref = {0};
    size_t nodeLen = 0;
    uint32_t nodePosition = 0;
    uint32_t nodeGeneration = 0;
    size_t entryLen = 0;
    RedisSearchCtx _sctx;
    ApplyGcEntryResult r;

    if (FGC_recvFixed(gc, &nodeLen, sizeof nodeLen) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }
    if (nodeLen == NO_MORE_DATA) {
      break;
    }

    // Read node_position + node_generation + entry_data.
    if (FGC_recvFixed(gc, &nodePosition, sizeof nodePosition) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }
    if (FGC_recvFixed(gc, &nodeGeneration, sizeof nodeGeneration) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }
    entryLen = nodeLen - sizeof(nodePosition) - sizeof(nodeGeneration);
    if (entryLen > entryDataCap) {
      entryData = rm_realloc(entryData, entryLen);
      entryDataCap = entryLen;
    }
    if (FGC_recvFixed(gc, entryData, entryLen) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    // Acquire spec reference and lock.
    spec_ref = IndexSpecRef_Promote(gc->index);
    sp = StrongRef_Get(spec_ref);
    if (!sp) {
      status = FGC_SPEC_DELETED;
      goto loop_cleanup;
    }
    _sctx = SEARCH_CTX_STATIC(gc->ctx, sp);
    RedisSearchCtx_LockSpecWrite(&_sctx);

    // First iteration: look up the tree and validate uniqueId once.
    // The rt pointer remains valid across lock/unlock cycles because we hold
    // a StrongRef each iteration (the tree is only freed when the spec is freed).
    // Node-level staleness is handled by the generational arena inside
    // NumericRangeTree_ApplyGcEntry.
    if (!rt) {
      const FieldSpec *fs = IndexSpec_GetFieldWithLength(_sctx.spec, fieldName, fieldNameLen);
      // Cast is safe: openNumericOrGeoIndex only mutates fs when create_if_missing is true.
      rt = openNumericOrGeoIndex(_sctx.spec, (FieldSpec *)fs, DONT_CREATE_INDEX);
      if (!rt || NumericRangeTree_GetUniqueId(rt) != rtUniqueId) {
        status = FGC_PARENT_ERROR;
        goto loop_cleanup;
      }
    }

    r = NumericRangeTree_ApplyGcEntry(rt, nodePosition, nodeGeneration,
                                      (const uint8_t *)entryData, entryLen);
    switch (r.status) {
      case Ok:
        FGC_updateStats(gc, &_sctx, r.gc_result.index_gc_info.entries_removed,
                        r.gc_result.index_gc_info.bytes_freed,
                        r.gc_result.index_gc_info.bytes_allocated,
                        r.gc_result.index_gc_info.ignored_last_block);
        IndexStats_BlockCountAdd(&_sctx.spec->stats,
                                 r.gc_result.index_gc_info.block_count_delta);
        break;
      case NodeNotFound:
        gc->stats.gcNumericNodesMissed++;
        break;
      case DeserializationError:
        status = FGC_CHILD_ERROR;
        goto loop_cleanup;
    }

  loop_cleanup:
    if (sp) {
      RedisSearchCtx_UnlockSpec(&_sctx);
      IndexSpecRef_Release(spec_ref);
    }
  }
  rm_free(entryData);

  // Conditionally trim empty leaves (re-acquire lock).
  if (status == FGC_COLLECTED && rt && gc->cleanNumericEmptyNodes) {
    StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    if (!sp) {
      FGC_freeBuffer(fieldName, fieldNameLen);
      return FGC_SPEC_DELETED;
    }
    RedisSearchCtx sctx2 = SEARCH_CTX_STATIC(gc->ctx, sp);
    RedisSearchCtx_LockSpecWrite(&sctx2);
    CompactIfSparseResult r = NumericRangeTree_CompactIfSparse(rt);
    if (r.inverted_index_size_delta < 0) {
      FGC_updateStats(gc, &sctx2, 0, -r.inverted_index_size_delta, 0, 0);
    }
    IndexStats_BlockCountAdd(&sctx2.spec->stats, r.block_count_delta);
    RedisSearchCtx_UnlockSpec(&sctx2);
    IndexSpecRef_Release(spec_ref);
  }

  FGC_freeBuffer(fieldName, fieldNameLen);
  return status;
}
