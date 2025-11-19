/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "redis_index.h"
#include "doc_table.h"
#include "redismodule.h"
#include "inverted_index.h"
#include "iterators/inverted_index_iterator.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/logging.h"
#include "util/misc.h"
#include "tag_index.h"
#include "rmalloc.h"
#include <stdio.h>

RedisModuleType *InvertedIndexType;

static inline void updateTime(SearchTime *searchTime, int32_t durationNS) {
  if (RS_IsMock) return;

  // 0 disables the timeout
  if (durationNS == 0) {
    durationNS = INT32_MAX;
  }


  struct timespec duration = { .tv_sec = durationNS / 1000,
                               .tv_nsec = ((durationNS % 1000) * 1000000) };
#ifdef CLOCK_REALTIME_COARSE
  clock_gettime(CLOCK_REALTIME_COARSE, &searchTime->current);
#else
  // In some mac systems CLOCK_REALTIME_COARSE is not defined, we fallback to CLOCK_REALTIME
  clock_gettime(CLOCK_REALTIME, &searchTime->current);
#endif

  // The timeout mechanism is based on the monotonic clock, so we need another clock_gettime call
  timespec monotoicNow = { .tv_sec = 0,
                           .tv_nsec = 0 };
  clock_gettime(CLOCK_MONOTONIC_RAW, &monotoicNow);
  rs_timeradd(&monotoicNow, &duration, &searchTime->timeout);
}

/**
 * Format redis key for a term.
 */
RedisModuleString *fmtRedisTermKey(const RedisSearchCtx *ctx, const char *term, size_t len) {
  char buf_s[1024] = {"ft:"};
  size_t offset = 3;
  size_t nameLen = 0;
  const char* name = HiddenString_GetUnsafe(ctx->spec->specName, &nameLen);
  char *buf, *bufDyn = NULL;
  if (nameLen + len + 10 > sizeof(buf_s)) {
    buf = bufDyn = rm_calloc(1, nameLen + len + 10);
    strcpy(buf, "ft:");
  } else {
    buf = buf_s;
  }

  memcpy(buf + offset, name, nameLen);
  offset += nameLen;
  buf[offset++] = '/';
  memcpy(buf + offset, term, len);
  offset += len;
  RedisModuleString *ret = RedisModule_CreateString(ctx->redisCtx, buf, offset);
  rm_free(bufDyn);
  return ret;
}

RedisModuleString *fmtRedisSkipIndexKey(const RedisSearchCtx *ctx, const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SKIPINDEX_KEY_FORMAT, HiddenString_GetUnsafe(ctx->spec->specName, NULL),
                                        (int)len, term);
}

RedisModuleString *fmtRedisScoreIndexKey(const RedisSearchCtx *ctx, const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SCOREINDEX_KEY_FORMAT, HiddenString_GetUnsafe(ctx->spec->specName, NULL),
                                        (int)len, term);
}

void RedisSearchCtx_LockSpecRead(RedisSearchCtx *ctx) {
  RS_ASSERT(ctx->flags == RS_CTX_UNSET);
  pthread_rwlock_rdlock(&ctx->spec->rwlock);
  // pause rehashing while we're using the dict for reads only
  // Assert that the pause value before we pause is valid.
  RS_ASSERT_ALWAYS(dictPauseRehashing(ctx->spec->keysDict));
  ctx->flags = RS_CTX_READONLY;
}

void RedisSearchCtx_LockSpecWrite(RedisSearchCtx *ctx) {
  RS_ASSERT(ctx->flags == RS_CTX_UNSET);
  pthread_rwlock_wrlock(&ctx->spec->rwlock);
  ctx->flags = RS_CTX_READWRITE;
}

// DOES NOT INCREMENT REF COUNT
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL) {
  IndexLoadOptions loadOpts = {.nameC = indexName};
  StrongRef ref = IndexSpec_LoadUnsafeEx(&loadOpts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return NULL;
  }

  RedisSearchCtx *sctx = rm_new(RedisSearchCtx);
  *sctx = SEARCH_CTX_STATIC(ctx, sp);
  return sctx;
}

RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL) {
  return NewSearchCtxC(ctx, RedisModule_StringPtrLen(indexName, NULL), resetTTL);
}

void RedisSearchCtx_UnlockSpec(RedisSearchCtx *sctx) {
  RS_ASSERT(sctx);
  if (sctx->flags == RS_CTX_UNSET) {
    return;
  }
  if (sctx->flags == RS_CTX_READONLY) {
    // We paused rehashing when we locked the spec for read. Now we can resume it.
    // Assert that it was actually previously paused
    RS_ASSERT_ALWAYS(dictResumeRehashing(sctx->spec->keysDict));
  }
  pthread_rwlock_unlock(&sctx->spec->rwlock);
  sctx->flags = RS_CTX_UNSET;
}

void SearchCtx_UpdateTime(RedisSearchCtx *sctx, int32_t durationNS) {
  updateTime(&sctx->time, durationNS);
}

void SearchCtx_CleanUp(RedisSearchCtx * sctx) {
  if (sctx->key_) {
    RedisModule_CloseKey(sctx->key_);
    sctx->key_ = NULL;
  }
  RedisSearchCtx_UnlockSpec(sctx);
}

void SearchCtx_Free(RedisSearchCtx *sctx) {
  SearchCtx_CleanUp(sctx);
  rm_free(sctx);
}

static InvertedIndex *openIndexKeysDict(const RedisSearchCtx *ctx, RedisModuleString *termKey,
                                        int write, bool *outIsNew) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, termKey);
  if (kdv) {
    if (outIsNew) {
      *outIsNew = false;
    }
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }

  if (outIsNew) {
    *outIsNew = true;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))InvertedIndex_Free;
  size_t index_size;
  kdv->p = NewInvertedIndex(ctx->spec->flags, &index_size);
  ctx->spec->stats.invertedSize += index_size;
  dictAdd(ctx->spec->keysDict, termKey, kdv);
  return kdv->p;
}

InvertedIndex *Redis_OpenInvertedIndex(const RedisSearchCtx *ctx, const char *term, size_t len, int write, bool *outIsNew) {
  RedisModuleString *termKey = fmtRedisTermKey(ctx, term, len);
  InvertedIndex *idx = openIndexKeysDict(ctx, termKey, write, outIsNew);
  RedisModule_FreeString(ctx->redisCtx, termKey);
  return idx;
}

QueryIterator *Redis_OpenReader(const RedisSearchCtx *ctx, RSQueryTerm *term, DocTable *dt,
                                t_fieldMask fieldMask, double weight) {

  RedisModuleString *termKey = fmtRedisTermKey(ctx, term->str, term->len);
  InvertedIndex *idx = NULL;
  RedisModuleKey *k = NULL;

  idx = openIndexKeysDict(ctx, termKey, 0, NULL);
  if (!idx) {
    goto err;
  }

  if (!InvertedIndex_NumDocs(idx) ||
     (Index_StoreFieldMask(ctx->spec) && !(InvertedIndex_FieldMask(idx) & fieldMask))) {
    // empty index! or index does not have results from requested field.
    // pass
    goto err;
  }

  FieldMaskOrIndex fieldMaskOrIndex = {.mask_tag = FieldMaskOrIndex_Mask, .mask = fieldMask};
  QueryIterator *it = NewInvIndIterator_TermQuery(idx, ctx, fieldMaskOrIndex, term, weight);
  RedisModule_FreeString(ctx->redisCtx, termKey);
  return it;

err:
  if (k) {
    RedisModule_CloseKey(k);
  }
  if (termKey) {
    RedisModule_FreeString(ctx->redisCtx, termKey);
  }
  if (term) {
    Term_Free(term);
  }
  return NULL;
}

int Redis_DropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque) {
  // extract the term from the key
  RedisSearchCtx *sctx = opaque;
  RedisModuleString *pf = fmtRedisTermKey(sctx, "", 0);
  size_t pflen, len;
  RedisModule_StringPtrLen(pf, &pflen);
  RedisModule_FreeString(sctx->redisCtx, pf);

  char *k = (char *)RedisModule_StringPtrLen(kn, &len);
  k += pflen;
  // char *term = rm_strndup(k, len - pflen);

  RedisModuleString *sck = fmtRedisScoreIndexKey(sctx, k, len - pflen);
  RedisModuleString *sik = fmtRedisSkipIndexKey(sctx, k, len - pflen);

  RedisModuleCallReply *rep = RedisModule_Call(ctx, "DEL", "sss", kn, sck, sik);
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  RedisModule_FreeString(ctx, sck);
  RedisModule_FreeString(ctx, sik);
  // free(term);

  return REDISMODULE_OK;
}

int Redis_DeleteKey(RedisModuleCtx *ctx, RedisModuleString *s) {
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "DEL", "s", s);
  RS_ASSERT(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER);
  long long res = RedisModule_CallReplyInteger(rep);
  RedisModule_FreeCallReply(rep);
  return res;
}

int Redis_DeleteKeyC(RedisModuleCtx *ctx, char *cstr) {
  // Send command and args to replicas and AOF
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "DEL", "c!", cstr);
  RS_ASSERT(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER);
  long long res = RedisModule_CallReplyInteger(rep);
  RedisModule_FreeCallReply(rep);
  return res;
}
