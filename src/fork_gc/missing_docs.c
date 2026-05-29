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
#include "rmutil/rm_assert.h"
#include "obfuscation/hidden.h"

void FGC_childCollectMissingDocs(ForkGC *gc, RedisSearchCtx *sctx) {
  IndexSpec *spec = sctx->spec;

  dictIterator* iter = dictGetIterator(spec->missingFieldDict);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    const HiddenString *hiddenFieldName = dictGetKey(entry);
    InvertedIndex *idx = dictGetVal(entry);
    if(idx) {
      size_t length;
      const char* fieldName = HiddenString_GetUnsafe(hiddenFieldName, &length);
      struct iovec iov = {.iov_base = (void *)fieldName, length};

      CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &iov };
      II_GCCallback cb = { .ctx = &cbCtx, .call = sendHeaderString };

      II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

      InvertedIndex_GcDelta_Scan(
          &wr, sctx, idx,
          &cb, NULL
      );
    }
  }
  dictReleaseIterator(iter);

  // we are done with missing field docs inverted indexes
  FGC_sendTerminator(gc);
}

FGCError FGC_parentHandleMissingDocs(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;
  size_t fieldNameLen = 0;
  char *rawFieldName = NULL;
  II_GCScanStats info = {0};
  II_GCReader rd;
  InvertedIndexGcDelta *delta = NULL;
  HiddenString *fieldName = NULL;
  StrongRef spec_ref;
  IndexSpec *sp = NULL;
  RedisSearchCtx sctx_;
  RedisSearchCtx *sctx;
  InvertedIndex *idx = NULL;

  if (FGC_recvBuffer(gc, (void **)&rawFieldName, &fieldNameLen) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  if (fieldNameLen == SIZE_MAX) {
    return FGC_DONE;
  }

  rd = (II_GCReader){ .ctx = gc, .read = pipe_read_cb };
  delta = InvertedIndex_GcDelta_Read(&rd);

  if (delta == NULL) {
    FGC_freeBuffer(rawFieldName, fieldNameLen);
    return FGC_CHILD_ERROR;
  }

  fieldName = NewHiddenString(rawFieldName, fieldNameLen, false);
  spec_ref = IndexSpecRef_Promote(gc->index);
  sp = StrongRef_Get(spec_ref);
  if (!sp) {
    status = FGC_SPEC_DELETED;
    goto cleanup;
  }

  sctx_ = SEARCH_CTX_STATIC(gc->ctx, sp);
  sctx = &sctx_;

  RedisSearchCtx_LockSpecWrite(sctx);
  idx = dictFetchValue(sctx->spec->missingFieldDict, fieldName);

  if (idx == NULL) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  InvertedIndex_ApplyGCDelta(idx, delta, &info);
  delta = NULL;
  IndexStats_BlockCountAdd(&sctx->spec->stats, info.block_count_delta);

  if (InvertedIndex_NumDocs(idx) == 0) {
    // Sample memory and block count before the destructor callback (InvIndFreeCb) frees
    // the index without spec context.
    info.bytes_freed += InvertedIndex_MemUsage(idx);
    IndexStats_BlockCountAdd(&sctx->spec->stats, -(int64_t)InvertedIndex_NumBlocks(idx));
    dictDelete(sctx->spec->missingFieldDict, fieldName);
  }
  FGC_updateStats(gc, sctx, info.entries_removed, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

cleanup:

  if (sp) {
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
  }
  HiddenString_Free(fieldName, false);
  FGC_freeBuffer(rawFieldName, fieldNameLen);
  InvertedIndex_GcDelta_Free(delta);

  return status;
}
