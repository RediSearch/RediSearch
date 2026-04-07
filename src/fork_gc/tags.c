/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdint.h>              // for uint64_t
#include <string.h>              // for NULL, size_t, strlen

#include "pipe.h"                // for CTX_II_GC_Callback, FGC_sendBuffer
#include "redis_index.h"         // for DONT_CREATE_INDEX
#include "tag_index.h"           // for TagIndex, TagIndex_Open, ...
#include "suffix.h"              // for deleteSuffixTrieMap
#include "rmutil/rm_assert.h"    // for RS_LOG_ASSERT_FMT, RS_ASSERT, ...
#include "obfuscation/hidden.h"  // for HiddenString_GetUnsafe
#include "field_spec.h"          // for FieldSpec, INDEXFLD_T_TAG
#include "fork_gc.h"             // for ForkGC
#include "inverted_index.h"      // for II_GCScanStats, InvertedIndex, ...
#include "redismodule.h"         // for REDISMODULE_OK
#include "rmalloc.h"             // for rm_free
#include "search_ctx.h"          // for RedisSearchCtx, ...
#include "spec.h"                // for IndexSpecRef_Promote, ...
#include "triemap.h"             // for TrieMapIterator_Next, TrieMap_Delete
#include "util/arr/arr.h"        // for array_len, array_free, arrayof
#include "util/references.h"     // for StrongRef_Get, StrongRef

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
                          .uniqueId = tagIdx->uniqueId};

      TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values);
      char *ptr;
      tm_len_t len;
      InvertedIndex *value;
      while (TrieMapIterator_Next(iter, &ptr, &len, (void **)&value)) {
        header.curPtr = value;
        header.tagValue = ptr;
        header.tagLen = len;

        // send repaired data

        CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &header };
        II_GCCallback cb = { .ctx = &cbCtx, .call = sendTagHeader };

        II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

        InvertedIndex_GcDelta_Scan(
            &wr, sctx, value,
            &cb, NULL
        );
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
    II_GCScanStats info = {0};
    TagIndex *tagIdx = NULL;
    char *tagVal = NULL;
    size_t tagValLen = 0;
    StrongRef spec_ref;
    IndexSpec *sp = NULL;
    RedisSearchCtx _sctx;
    RedisSearchCtx *sctx = NULL;
    II_GCReader rd;
    const FieldSpec *fs = NULL;
    size_t dummy_size = 0;
    InvertedIndex *idx = NULL;

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

    if (tagIdx->uniqueId != tagUniqueId) {
      status = FGC_CHILD_ERROR;
      goto loop_cleanup;
    }

    idx = TagIndex_OpenIndex(tagIdx, tagVal, tagValLen, DONT_CREATE_INDEX, &dummy_size);
    if (idx == TRIEMAP_NOTFOUND || idx != value) {
      status = FGC_PARENT_ERROR;
      goto loop_cleanup;
    }

    InvertedIndex_ApplyGcDelta(idx, delta, &info);
    delta = NULL;

    // if tag value is empty, let's remove it.
    if (InvertedIndex_NumDocs(idx) == 0) {
      // get memory before deleting the inverted index
      info.bytes_freed += InvertedIndex_MemUsage(idx);
      TrieMap_Delete(tagIdx->values, tagVal, tagValLen, (void (*)(void *))InvertedIndex_Free);

      if (tagIdx->suffix) {
        deleteSuffixTrieMap(tagIdx->suffix, tagVal, tagValLen);
      }
    }

    FGC_updateStats(gc, sctx, info.entries_removed, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

  loop_cleanup:
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
    InvertedIndex_GcDelta_Free(delta);

    if (tagVal) {
      rm_free(tagVal);
    }
  }

  rm_free(fieldName);
  return status;
}
