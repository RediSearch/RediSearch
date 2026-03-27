/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"
#include "redis_index.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/hidden.h"
#include "iterators_rs.h"

void FGC_childCollectNumeric(ForkGC *gc, RedisSearchCtx *sctx) {
  arrayof(FieldSpec*) numericFields = getFieldsByType(sctx->spec, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);

  for (int i = 0; i < array_len(numericFields); ++i) {
    NumericRangeTree *rt = openNumericOrGeoIndex(sctx->spec, numericFields[i], DONT_CREATE_INDEX);

    // No entries were added to the numeric field, hence the tree was not initialized
    if (!rt) {
      continue;
    }

    // Send the field header (field_name + unique_id).
    const char *fieldName = HiddenString_GetUnsafe(numericFields[i]->fieldName, NULL);
    FGC_sendBuffer(gc, fieldName, strlen(fieldName));
    uint64_t uniqueId = NumericRangeTree_GetUniqueId(rt);
    FGC_sendFixed(gc, &uniqueId, sizeof uniqueId);

    // Stream one node at a time to avoid buffering all deltas in memory.
    NumericGcScanner *scanner = NumericGcScanner_New(sctx, rt);
    NumericGcNodeEntry entry;
    while (NumericGcScanner_Next(scanner, &entry)) {
      // Send: node_len + node_position + node_generation + entry_data.
      size_t nodeLen = sizeof(entry.node_position) + sizeof(entry.node_generation) + entry.data_len;
      FGC_SEND_VAR(gc, nodeLen);
      FGC_SEND_VAR(gc, entry.node_position);
      FGC_SEND_VAR(gc, entry.node_generation);
      FGC_sendFixed(gc, entry.data, entry.data_len);
    }
    NumericGcScanner_Free(scanner);

    FGC_sendTerminator(gc);
  }

  array_free(numericFields);
  // we are done with numeric fields
  FGC_sendTerminator(gc);
}

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
    rm_free(fieldName);
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
    // Check if we received the sentinel terminator value
    if (nodeLen == SIZE_MAX) {
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
      rm_free(fieldName);
      return FGC_SPEC_DELETED;
    }
    RedisSearchCtx sctx2 = SEARCH_CTX_STATIC(gc->ctx, sp);
    RedisSearchCtx_LockSpecWrite(&sctx2);
    CompactIfSparseResult r = NumericRangeTree_CompactIfSparse(rt);
    if (r.inverted_index_size_delta < 0) {
      FGC_updateStats(gc, &sctx2, 0, -r.inverted_index_size_delta, 0, 0);
    }
    RedisSearchCtx_UnlockSpec(&sctx2);
    IndexSpecRef_Release(spec_ref);
  }

  rm_free(fieldName);
  return status;
}
