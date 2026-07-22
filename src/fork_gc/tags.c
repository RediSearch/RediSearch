/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"
#include "triemap_ffi.h"
#include "inverted_index_ffi.h"
#include "redis_index.h"
#include "tag_index.h"
#include "redisearch_rs/headers/tag_index.h"
#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/hidden.h"

typedef struct {
  const char *field;
  const void *curPtr;
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
  FGC_SEND_VAR(ctx->gc, info->curPtr);
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

      ValueIterator *iter = TagIndex2_IterateValues(tagIdx);
      char *ptr;
      tm_len_t len;
      TagIndexValue *value;
      while (TagIndex2_ValueIterator_Next(iter, &ptr, &len, &value)) {
        header.curPtr = value;
        header.tagValue = ptr;
        header.tagLen = len;

        // send repaired data

        CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &header };
        II_GCCallback cb = { .ctx = &cbCtx, .call = sendTagHeader };

        II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

        TagIndexValue_NumDocs_GcDelta_Scan(&wr, sctx, value, &cb);
      }

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
  InvertedIndex *value = NULL;
  FGCError status = recvFieldHeader(gc, &fieldName, &fieldNameLen, &tagUniqueId);

  while (status == FGC_COLLECTED) {
    InvertedIndexGcDelta *delta = NULL;
    TagIndex *tagIdx = NULL;
    char *tagVal = NULL;
    size_t tagValLen = 0;
    StrongRef spec_ref;
    IndexSpec *sp = NULL;
    RedisSearchCtx _sctx;
    RedisSearchCtx *sctx = NULL;
    II_GCReader rd;
    const FieldSpec *fs = NULL;
    TagGcResult r = {0};

    if (FGC_recvFixed(gc, &value, sizeof value) != REDISMODULE_OK) {
      status = FGC_CHILD_ERROR;
      break;
    }

    // No more tags values in tag field
    if (value == NULL) {
      RS_LOG_ASSERT(status == FGC_COLLECTED, "GC status is COLLECTED");
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

    // Apply the delta to the tag's inverted index in Rust. TagIndex2_GC also
    // verifies the scanned index is still the tag's current one (else reports
    // it was not applied), and — when the posting list becomes empty — drops
    // the value from the values trie and the suffix trie. Ownership of `delta`
    // is transferred to the call (consumed on every path).
    r = TagIndex2_GC(tagIdx, (const uint8_t *)tagVal, tagValLen, value, delta);
    delta = NULL;
    if (!r.applied) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    IndexStats_BlockCountAdd(&sctx->spec->stats, r.info.block_count_delta);
    FGC_updateStats(gc, sctx, r.info.entries_removed, r.info.bytes_freed,
                    r.info.bytes_allocated, r.info.ignored_last_block);

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
