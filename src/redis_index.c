/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redis_index.h"
#include "doc_table.h"
#include "redismodule.h"
#include "inverted_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/logging.h"
#include "util/misc.h"
#include "tag_index.h"
#include "rmalloc.h"
#include <stdio.h>

RedisModuleType *InvertedIndexType;

void *InvertedIndex_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > INVERTED_INDEX_ENCVER) {
    return NULL;
  }

  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(RedisModule_LoadUnsigned(rdb), 0, &index_memsize);

  // If the data was encoded with a version that did not include the store numeric / store freqs
  // options - we force adding StoreFreqs.
  if (encver <= INVERTED_INDEX_NOFREQFLAG_VER) {
    idx->flags |= Index_StoreFreqs;
  }
  idx->lastId = RedisModule_LoadUnsigned(rdb);
  idx->numDocs = RedisModule_LoadUnsigned(rdb);
  idx->size = RedisModule_LoadUnsigned(rdb);
  idx->blocks = rm_calloc(idx->size, sizeof(IndexBlock));

  size_t actualSize = 0;
  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock *blk = &idx->blocks[actualSize];
    blk->firstId = RedisModule_LoadUnsigned(rdb);
    blk->lastId = RedisModule_LoadUnsigned(rdb);
    blk->numEntries = RedisModule_LoadUnsigned(rdb);
    if (blk->numEntries > 0) {
      ++actualSize;
    }

    blk->buf.data = RedisModule_LoadStringBuffer(rdb, &blk->buf.offset);
    blk->buf.cap = blk->buf.offset;
    // if we read a buffer of 0 bytes we still read 1 byte from the RDB that needs to be freed
    if (!blk->buf.cap && blk->buf.data) {
      RedisModule_Free(blk->buf.data);
      blk->buf.data = NULL;
    } else {
      char *buf = rm_malloc(blk->buf.offset);
      memcpy(buf, blk->buf.data, blk->buf.offset);
      RedisModule_Free(blk->buf.data);
      blk->buf.data = buf;
    }
  }
  idx->size = actualSize;
  if (idx->size == 0) {
    size_t sz;
    InvertedIndex_AddBlock(idx, 0, &sz);
  } else {
    idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  }
  return idx;
}
void InvertedIndex_RdbSave(RedisModuleIO *rdb, void *value) {

  InvertedIndex *idx = value;
  RedisModule_SaveUnsigned(rdb, idx->flags);
  RedisModule_SaveUnsigned(rdb, idx->lastId);
  RedisModule_SaveUnsigned(rdb, idx->numDocs);
  uint32_t readSize = 0;
  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock *blk = &idx->blocks[i];
    if (blk->numEntries == 0) {
      continue;
    }
    ++readSize;
  }
  RedisModule_SaveUnsigned(rdb, readSize);

  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock *blk = &idx->blocks[i];
    if (blk->numEntries == 0) {
      continue;
    }
    RedisModule_SaveUnsigned(rdb, blk->firstId);
    RedisModule_SaveUnsigned(rdb, blk->lastId);
    RedisModule_SaveUnsigned(rdb, blk->numEntries);
    if (IndexBlock_DataLen(blk)) {
      RedisModule_SaveStringBuffer(rdb, IndexBlock_DataBuf(blk), IndexBlock_DataLen(blk));
    } else {
      RedisModule_SaveStringBuffer(rdb, "", 0);
    }
  }
}
void InvertedIndex_Digest(RedisModuleDigest *digest, void *value) {
}

unsigned long InvertedIndex_MemUsage(const void *value) {
  const InvertedIndex *idx = value;
  unsigned long ret = sizeof_InvertedIndex(idx->flags);
  for (size_t i = 0; i < idx->size; i++) {
    ret += sizeof(IndexBlock);
    ret += IndexBlock_DataCap(&idx->blocks[i]);
  }
  return ret;
}

int InvertedIndex_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = InvertedIndex_RdbLoad,
                               .rdb_save = InvertedIndex_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .mem_usage = InvertedIndex_MemUsage,
                               .free = InvertedIndex_Free};

  InvertedIndexType = RedisModule_CreateDataType(ctx, "ft_invidx", INVERTED_INDEX_ENCVER, &tm);
  if (InvertedIndexType == NULL) {
    RedisModule_Log(ctx, "warning", "Could not create inverted index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

/**
 * Format redis key for a term.
 * TODO: Add index name to it
 */
RedisModuleString *fmtRedisTermKey(RedisSearchCtx *ctx, const char *term, size_t len) {
  char buf_s[1024] = {"ft:"};
  size_t offset = 3;
  size_t nameLen = ctx->spec->nameLen;

  char *buf, *bufDyn = NULL;
  if (nameLen + len + 10 > sizeof(buf_s)) {
    buf = bufDyn = rm_calloc(1, nameLen + len + 10);
    strcpy(buf, "ft:");
  } else {
    buf = buf_s;
  }

  memcpy(buf + offset, ctx->spec->name, nameLen);
  offset += nameLen;
  buf[offset++] = '/';
  memcpy(buf + offset, term, len);
  offset += len;
  RedisModuleString *ret = RedisModule_CreateString(ctx->redisCtx, buf, offset);
  rm_free(bufDyn);
  return ret;
}

RedisModuleString *fmtRedisSkipIndexKey(RedisSearchCtx *ctx, const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SKIPINDEX_KEY_FORMAT, ctx->spec->name,
                                        (int)len, term);
}

RedisModuleString *fmtRedisScoreIndexKey(RedisSearchCtx *ctx, const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SCOREINDEX_KEY_FORMAT, ctx->spec->name,
                                        (int)len, term);
}

void RedisSearchCtx_LockSpecRead(RedisSearchCtx *ctx) {
  RedisModule_Assert(ctx->flags == RS_CTX_UNSET);
  pthread_rwlock_rdlock(&ctx->spec->rwlock);
  // pause rehashing while we're using the dict for reads only
  // Assert that the pause value before we pause is valid.
  RedisModule_Assert(dictPauseRehashing(ctx->spec->keysDict));
  ctx->flags = RS_CTX_READONLY;
}

void RedisSearchCtx_LockSpecWrite(RedisSearchCtx *ctx) {
  RedisModule_Assert(ctx->flags == RS_CTX_UNSET);
  pthread_rwlock_wrlock(&ctx->spec->rwlock);
  ctx->flags = RS_CTX_READWRITE;
}

// DOES NOT INCREMENT REF COUNT
RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL) {
  IndexLoadOptions loadOpts = {.name = {.cstring = indexName}};
  StrongRef ref = IndexSpec_LoadUnsafeEx(ctx, &loadOpts);
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
  assert(sctx);
  if (sctx->flags == RS_CTX_UNSET) {
    return;
  }
  if (sctx->flags == RS_CTX_READONLY) {
    // We paused rehashing when we locked the spec for read. Now we can resume it.
    // Assert that it was actually previously paused
    RedisModule_Assert(dictResumeRehashing(sctx->spec->keysDict));
  }
  pthread_rwlock_unlock(&sctx->spec->rwlock);
  sctx->flags = RS_CTX_UNSET;
}

void SearchCtx_UpdateTimeout(RedisSearchCtx *sctx, struct timespec timeoutTime) {
  sctx->timeout = timeoutTime;
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

static InvertedIndex *openIndexKeysDict(RedisSearchCtx *ctx, RedisModuleString *termKey,
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
  kdv->dtor = InvertedIndex_Free;
  size_t index_size;
  kdv->p = NewInvertedIndex(ctx->spec->flags, 1, &index_size);
  ctx->spec->stats.invertedSize += index_size;
  dictAdd(ctx->spec->keysDict, termKey, kdv);
  return kdv->p;
}

InvertedIndex *Redis_OpenInvertedIndexEx(RedisSearchCtx *ctx, const char *term, size_t len,
                                         int write, bool *outIsNew, RedisModuleKey **keyp) {
  RedisModuleString *termKey = fmtRedisTermKey(ctx, term, len);
  InvertedIndex *idx = NULL;

  if (!ctx->spec->keysDict) {
    RedisModuleKey *k = RedisModule_OpenKey(ctx->redisCtx, termKey,
                                            REDISMODULE_READ | (write ? REDISMODULE_WRITE : 0));

    // check that the key is empty
    if (k == NULL) {
      if (outIsNew) {
        *outIsNew = false;
      }
      goto end;
    }

    int kType = RedisModule_KeyType(k);

    if (kType == REDISMODULE_KEYTYPE_EMPTY) {
      if (write) {
        if (outIsNew) {
          *outIsNew = true;
        }
        size_t index_size;
        idx = NewInvertedIndex(ctx->spec->flags, 1, &index_size);
        ctx->spec->stats.invertedSize += index_size;
        RedisModule_ModuleTypeSetValue(k, InvertedIndexType, idx);
      }
    } else if (kType == REDISMODULE_KEYTYPE_MODULE &&
               RedisModule_ModuleTypeGetType(k) == InvertedIndexType) {
      idx = RedisModule_ModuleTypeGetValue(k);
    }
    if (idx == NULL) {
      RedisModule_CloseKey(k);
    } else {
      if (keyp) {
        *keyp = k;
      }
    }
  } else {
    idx = openIndexKeysDict(ctx, termKey, write, outIsNew);
  }
end:
  RedisModule_FreeString(ctx->redisCtx, termKey);
  return idx;
}

IndexReader *Redis_OpenReader(RedisSearchCtx *ctx, RSQueryTerm *term, DocTable *dt,
                              int singleWordMode, t_fieldMask fieldMask, ConcurrentSearchCtx *csx,
                              double weight) {

  RedisModuleString *termKey = fmtRedisTermKey(ctx, term->str, term->len);
  InvertedIndex *idx = NULL;
  RedisModuleKey *k = NULL;
  if (!ctx->spec->keysDict) {
    k = RedisModule_OpenKey(ctx->redisCtx, termKey, REDISMODULE_READ);

    // we do not allow empty indexes when loading an existing index
    if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
        RedisModule_ModuleTypeGetType(k) != InvertedIndexType) {
      goto err;
    }

    idx = RedisModule_ModuleTypeGetValue(k);
  } else {
    idx = openIndexKeysDict(ctx, termKey, 0, NULL);
    if (!idx) {
      goto err;
    }
  }

  if (!idx->numDocs ||
     (Index_StoreFieldMask(ctx->spec) && !(idx->fieldMask & fieldMask))) {
    // empty index! or index does not have results from requested field.
    // pass
    goto err;
  }

  IndexReader *ret = NewTermIndexReader(idx, ctx->spec, fieldMask, term, weight);
  if (csx) {
    ConcurrentSearch_AddKey(csx, TermReader_OnReopen, ret, NULL);
  }
  RedisModule_FreeString(ctx->redisCtx, termKey);
  return ret;

err:
  if (k) {
    RedisModule_CloseKey(k);
  }
  if (termKey) {
    RedisModule_FreeString(ctx->redisCtx, termKey);
  }
  return NULL;
}

int Redis_ScanKeys(RedisModuleCtx *ctx, const char *prefix, ScanFunc f, void *opaque) {
  long long ptr = 0;

  int num = 0;
  do {
    RedisModuleString *sptr = RedisModule_CreateStringFromLongLong(ctx, ptr);
    RedisModuleCallReply *r =
        RedisModule_Call(ctx, "SCAN", "scccc", sptr, "MATCH", prefix, "COUNT", "100");
    RedisModule_FreeString(ctx, sptr);
    if (r == NULL || RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR) {
      return num;
    }

    if (RedisModule_CallReplyLength(r) < 1) {
      break;
    }

    sptr = RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(r, 0));
    RedisModule_StringToLongLong(sptr, &ptr);
    RedisModule_FreeString(ctx, sptr);
    // printf("ptr: %s %lld\n",
    // RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(r, 0),
    // NULL), ptr);
    if (RedisModule_CallReplyLength(r) == 2) {
      RedisModuleCallReply *keys = RedisModule_CallReplyArrayElement(r, 1);
      size_t nks = RedisModule_CallReplyLength(keys);

      for (size_t i = 0; i < nks; i++) {
        RedisModuleString *kn =
            RedisModule_CreateStringFromCallReply(RedisModule_CallReplyArrayElement(keys, i));
        if (f(ctx, kn, opaque) != REDISMODULE_OK) goto end;

        // RedisModule_FreeString(ctx, kn);
        if (++num % 10000 == 0) {
          LG_DEBUG("Scanned %d keys", num);
        }
      }

      // RedisModule_FreeCallReply(keys);
    }

    RedisModule_FreeCallReply(r);

  } while (ptr);
end:
  return num;
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
  RedisModule_Assert(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER);
  long long res = RedisModule_CallReplyInteger(rep);
  RedisModule_FreeCallReply(rep);
  return res;
}

int Redis_DeleteKeyC(RedisModuleCtx *ctx, char *cstr) {
  RedisModuleCallReply *rep;
  if (!isCrdt) {
    rep = RedisModule_Call(ctx, "DEL", "c!", cstr);
  } else {
    rep = RedisModule_Call(ctx, "DEL", "c", cstr);
  }
  RedisModule_Assert(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_INTEGER);
  long long res = RedisModule_CallReplyInteger(rep);
  RedisModule_FreeCallReply(rep);
  return res;
}
