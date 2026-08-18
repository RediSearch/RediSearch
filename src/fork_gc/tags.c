/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdint.h>
#include <string.h>

#include "pipe.h"
#include "triemap_ffi.h"
#include "inverted_index_ffi.h"
#include "redisearch_rs/headers/tag_index_ffi.h"
#include "redis_index.h"
#include "tag_index.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/hidden.h"
#include "field_spec.h"
#include "fork_gc.h"
#include "fork_gc_ffi.h"
#include "inverted_index.h"
#include "redismodule.h"
#include "search_ctx.h"
#include "spec.h"
#include "util/arr/arr.h"
#include "util/references.h"

typedef struct {
  const char *field;
  /* Frames the stream: a NULL marks the end of this field's values. The parent
   * never dereferences it — it compares `valueUniqueId` instead, because the
   * tag's posting list can be freed and a new one allocated at the same address
   * between this scan and the parent's apply. */
  const void *valueMarker;
  IndexUniqueId valueUniqueId;
  char *tagValue;
  size_t tagLen;
  uint64_t uniqueId;
  int sentFieldName;
} tagHeader;

static void sendTagHeader(void *opaqueCtx) {
  CTX_II_GC_Callback* ctx = opaqueCtx;

  tagHeader *info = ctx->hdrarg;
  if (!info->sentFieldName) {
    info->sentFieldName = 1;
    FGC_sendBuffer(ctx->gc, info->field, strlen(info->field));
    FGC_sendFixed(ctx->gc, &info->uniqueId, sizeof info->uniqueId);
  }
  FGC_SEND_VAR(ctx->gc, info->valueMarker);
  FGC_SEND_VAR(ctx->gc, info->valueUniqueId);
  FGC_sendBuffer(ctx->gc, info->tagValue, info->tagLen);
}

void FGC_childCollectTags(ForkGC *gc, RedisSearchCtx *sctx) {
  RS_ASSERT(sctx->spec->diskSpec == NULL);
  arrayof(FieldSpec*) tagFields = getFieldsByType(sctx->spec, INDEXFLD_T_TAG);
  if (array_len(tagFields) != 0) {
    for (int i = 0; i < array_len(tagFields); ++i) {
      TagIndex *tagIdx = TagIndex_Open(tagFields[i]);
      if (!tagIdx) {
        continue;
      }

      tagHeader header = {.field = HiddenString_GetUnsafe(tagFields[i]->fieldName, NULL),
                          .uniqueId = TagIndex_GetId(tagIdx)};

      ValueIterator *iter = TagIndex_IterateValues(tagIdx);
      char *ptr;
      tm_len_t len;
      TagIndexValue *value;
      while (Rust_TagIndex_ValueIterator_Next(iter, &ptr, &len, &value)) {
        header.valueMarker = value;
        header.valueUniqueId = Rust_TagIndexValue_UniqueId(value);
        header.tagValue = ptr;
        header.tagLen = len;

        // send repaired data

        CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &header };
        II_GCCallback cb = { .ctx = &cbCtx, .call = sendTagHeader };

        II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

        Rust_TagIndexValue_GcDelta_Scan(&wr, sctx, value, &cb);
      }
      Rust_TagIndex_ValueIterator_Free(iter);

      // we are done with the current field
      if (header.sentFieldName) {
        void *pdummy = NULL;
        FGC_SEND_VAR(gc, pdummy);
      }
    }
  }

  array_free(tagFields);
  // we are done with tag fields
  FGC_sendTerminator(gc);
}

FGCError FGC_parentHandleTags(ForkGC *gc) {
  size_t fieldNameLen;
  char *fieldName = NULL;
  uint64_t tagUniqueId;
  const void *valueMarker = NULL;
  FGCError status = recvFieldHeader(gc, &fieldName, &fieldNameLen, &tagUniqueId);

  while (status == FGC_COLLECTED) {
    InvertedIndexGcDelta *delta = NULL;
    TagIndex *tagIdx = NULL;
    char *tagVal = NULL;
    size_t tagValLen = 0;
    IndexUniqueId valueUniqueId = 0;
    StrongRef spec_ref;
    IndexSpec *sp = NULL;
    RedisSearchCtx _sctx;
    RedisSearchCtx *sctx = NULL;
    II_GCReader rd;
    const FieldSpec *fs = NULL;
    TagGcResult r = {0};

    if (FGC_recvFixed(gc, &valueMarker, sizeof valueMarker) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      break;
    }

    // No more tags values in tag field
    if (valueMarker == NULL) {
      RS_LOG_ASSERT(status == FGC_COLLECTED, "GC status is COLLECTED");
      break;
    }

    if (FGC_recvFixed(gc, &valueUniqueId, sizeof valueUniqueId) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      break;
    }

    spec_ref = IndexSpecRef_Promote(gc->index);
    sp = StrongRef_Get(spec_ref);
    if (!sp) {
      status = FGC_SPEC_DELETED;
      break;
    }
    _sctx = SEARCH_CTX_STATIC(gc->ctx, sp);
    sctx = &_sctx;

    if (FGC_recvBuffer(gc, (void **)&tagVal, &tagValLen) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    rd = (II_GCReader){ .ctx = gc, .read = pipe_read_cb };
    delta = InvertedIndex_GcDelta_Read(&rd);

    if (delta == NULL) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    RedisSearchCtx_LockSpecWrite(sctx);

    fs = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, fieldNameLen);
    RS_LOG_ASSERT_FMT(fs, "tag field '%.*s' not found in index during GC", (int)fieldNameLen, fieldName);
    tagIdx = TagIndex_Open(fs);
    RS_LOG_ASSERT_FMT(tagIdx, "tag field '%.*s' was not opened", (int)fieldNameLen, fieldName);

    if (TagIndex_GetId(tagIdx) != tagUniqueId) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    // Applies the delta, and reports it was not applied when the tag's posting
    // list is no longer the one that was scanned — removed, or replaced by a
    // new one that may well sit at the same address. When the list ends up
    // empty the tag is dropped from the values trie and the suffix index too.
    // Ownership of `delta` transfers on every path.
    r = Rust_TagIndex_GC(tagIdx, (const uint8_t *)tagVal, tagValLen, valueUniqueId, delta);
    delta = NULL;
    if (!r.applied) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    IndexStats_BlockCountAdd(&sctx->spec->stats, r.info.block_count_delta);
    FGC_updateStats(gc, sctx, r.info.entries_removed, r.info.bytes_freed, r.info.bytes_allocated,
                    r.info.ignored_last_block);

  loop_cleanup:
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
    InvertedIndex_GcDelta_Free(delta);

    if (tagVal) {
      FGC_freeBuffer(tagVal, tagValLen);
    }
  }

  FGC_freeBuffer(fieldName, fieldNameLen);
  return status;
}
