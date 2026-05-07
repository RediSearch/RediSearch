/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"
#include "triemap.h"
#include "redis_index.h"
#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/obfuscation_api.h"
#include "obfuscation/hidden.h"

void FGC_childCollectTerms(ForkGC *gc, RedisSearchCtx *sctx) {
  TrieIterator *iter = Trie_Iterate(sctx->spec->terms, "", 0, 0, 1);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, NULL, &dist)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    InvertedIndex *idx = Redis_OpenInvertedIndex(sctx->spec, term, termLen, DONT_CREATE_INDEX, NULL);
    if (idx) {
      struct iovec iov = {.iov_base = (void *)term, termLen};

      CTX_II_GC_Callback cbCtx = { .gc = gc, .hdrarg = &iov };
      II_GCCallback cb = { .ctx = &cbCtx, .call = sendHeaderString };

      II_GCWriter wr = { .ctx = gc, .write = pipe_write_cb };

      InvertedIndex_GcDelta_Scan(
          &wr, sctx, idx,
          &cb, NULL
      );
    }
    rm_free(term);
  }
  TrieIterator_Free(iter);

  // we are done with terms
  FGC_sendTerminator(gc);
}

FGCError FGC_parentHandleTerms(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;
  size_t len = 0;
  char *term = NULL;
  II_GCScanStats info = {0};
  II_GCReader rd;
  InvertedIndexGcDelta *delta = NULL;
  StrongRef spec_ref;
  IndexSpec *sp = NULL;
  RedisSearchCtx sctx_;
  RedisSearchCtx *sctx = NULL;
  InvertedIndex *idx = NULL;

  if (FGC_recvBuffer(gc, (void **)&term, &len) != REDISMODULE_OK) {
    return FGC_CHILD_ERROR;
  }

  if (term == RECV_BUFFER_EMPTY) {
    return FGC_DONE;
  }

  rd = (II_GCReader){ .ctx = gc, .read = pipe_read_cb };

  delta = InvertedIndex_GcDelta_Read(&rd);

  if (delta == NULL) {
    rm_free(term);
    return FGC_CHILD_ERROR;
  }

  spec_ref = IndexSpecRef_Promote(gc->index);
  sp = StrongRef_Get(spec_ref);
  if (!sp) {
    status = FGC_SPEC_DELETED;
    goto cleanup;
  }

  sctx_ = SEARCH_CTX_STATIC(gc->ctx, sp);
  sctx = &sctx_;

  RedisSearchCtx_LockSpecWrite(sctx);

  idx = Redis_OpenInvertedIndex(sctx->spec, term, len, DONT_CREATE_INDEX, NULL);

  if (idx == NULL) {
    status = FGC_PARENT_ERROR;
    goto cleanup;
  }

  InvertedIndex_ApplyGcDelta(idx, delta, &info);
  delta = NULL;

  if (InvertedIndex_NumDocs(idx) == 0) {

    // inverted index was cleaned entirely lets free it
    if (sctx->spec->keysDict) {
      CharBuf termKey = {.buf = term, .len = len};
      // get memory before deleting the inverted index
      size_t inv_idx_size = InvertedIndex_MemUsage(idx);
      if (dictDelete(sctx->spec->keysDict, &termKey) == DICT_OK) {
        info.bytes_freed += inv_idx_size;
      }
    }

    if (!Trie_Delete(sctx->spec->terms, term, len)) {
      const char* name = IndexSpec_FormatName(sctx->spec, RSGlobalConfig.hideUserDataFromLog);
      RedisModule_Log(sctx->redisCtx, "warning", "RedisSearch fork GC: deleting a term '%s' from"
                      " trie in index '%s' failed", RSGlobalConfig.hideUserDataFromLog ? Obfuscate_Text(term) : term, name);
    }
    sctx->spec->stats.scoring.numTerms--;
    sctx->spec->stats.termsSize -= len;
    if (sctx->spec->suffix) {
      deleteSuffixTrie(sctx->spec->suffix, term, len);
    }
  }

  FGC_updateStats(gc, sctx, info.entries_removed, info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

cleanup:

  if (sp) {
    RedisSearchCtx_UnlockSpec(sctx);
    IndexSpecRef_Release(spec_ref);
  }
  rm_free(term);

  InvertedIndex_GcDelta_Free(delta);
  return status;
}
