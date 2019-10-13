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
  InvertedIndex *idx = NewInvertedIndex(RedisModule_LoadUnsigned(rdb), 0);

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
    blk->numDocs = RedisModule_LoadUnsigned(rdb);
    if (blk->numDocs > 0) {
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
    InvertedIndex_AddBlock(idx, 0);
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
    if (blk->numDocs == 0) {
      continue;
    }
    ++readSize;
  }
  RedisModule_SaveUnsigned(rdb, readSize);

  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock *blk = &idx->blocks[i];
    if (blk->numDocs == 0) {
      continue;
    }
    RedisModule_SaveUnsigned(rdb, blk->firstId);
    RedisModule_SaveUnsigned(rdb, blk->lastId);
    RedisModule_SaveUnsigned(rdb, blk->numDocs);
    if (IndexBlock_DataLen(blk)) {
      RedisModule_SaveStringBuffer(rdb, IndexBlock_DataBuf(blk), IndexBlock_DataLen(blk));
    } else {
      RedisModule_SaveStringBuffer(rdb, "", 0);
    }
  }
}

unsigned long InvertedIndex_MemUsage(const void *value) {
  const InvertedIndex *idx = value;
  unsigned long ret = sizeof(InvertedIndex);
  for (size_t i = 0; i < idx->size; i++) {
    ret += sizeof(IndexBlock);
    ret += IndexBlock_DataLen(&idx->blocks[i]);
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
    RedisModule_Log(ctx, "error", "Could not create inverted index type");
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
  size_t nameLen = strlen(ctx->spec->name);

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
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SKIPINDEX_KEY_FORMAT, ctx->spec->name, len,
                                        term);
}

RedisModuleString *fmtRedisScoreIndexKey(RedisSearchCtx *ctx, const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SCOREINDEX_KEY_FORMAT, ctx->spec->name, len,
                                        term);
}

RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL) {
  IndexLoadOptions loadOpts = {.name = {.cstring = indexName}};
  IndexSpec *sp = IndexSpec_LoadEx(ctx, &loadOpts);

  if (!sp) {
    return NULL;
  }

  RedisSearchCtx *sctx = rm_malloc(sizeof(*sctx));
  *sctx = (RedisSearchCtx){.spec = sp,  // newline
                           .redisCtx = ctx,
                           .key_ = loadOpts.keyp,
                           .refcount = 1};
  return sctx;
}

RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL) {

  return NewSearchCtxC(ctx, RedisModule_StringPtrLen(indexName, NULL), resetTTL);
}

RedisSearchCtx *SearchCtx_Refresh(RedisSearchCtx *sctx, RedisModuleString *keyName) {
  // First we close the relevant keys we're touching
  RedisModuleCtx *redisCtx = sctx->redisCtx;
  SearchCtx_Free(sctx);
  // now release the global lock
  RedisModule_ThreadSafeContextUnlock(redisCtx);
  // try to acquire it again...
  RedisModule_ThreadSafeContextLock(redisCtx);
  // reopen the context - it might have gone away!
  return NewSearchCtx(redisCtx, keyName, true);
}

void SearchCtx_Free(RedisSearchCtx *sctx) {
  if (sctx->key_) {
    RedisModule_CloseKey(sctx->key_);
    sctx->key_ = NULL;
  }
  rm_free(sctx);
}

static InvertedIndex *openIndexKeysDict(RedisSearchCtx *ctx, RedisModuleString *termKey,
                                        int write) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, termKey);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }

  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = InvertedIndex_Free;
  kdv->p = NewInvertedIndex(ctx->spec->flags, 1);
  dictAdd(ctx->spec->keysDict, termKey, kdv);
  return kdv->p;
}

InvertedIndex *Redis_OpenInvertedIndexEx(RedisSearchCtx *ctx, const char *term, size_t len,
                                         int write, RedisModuleKey **keyp) {
  RedisModuleString *termKey = fmtRedisTermKey(ctx, term, len);
  InvertedIndex *idx = NULL;

  if (!ctx->spec->keysDict) {
    RedisModuleKey *k = RedisModule_OpenKey(ctx->redisCtx, termKey,
                                            REDISMODULE_READ | (write ? REDISMODULE_WRITE : 0));

    // check that the key is empty
    if (k == NULL) {
      goto end;
    }

    int kType = RedisModule_KeyType(k);

    if (kType == REDISMODULE_KEYTYPE_EMPTY) {
      if (write) {
        idx = NewInvertedIndex(ctx->spec->flags, 1);
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
    idx = openIndexKeysDict(ctx, termKey, write);
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
    idx = openIndexKeysDict(ctx, termKey, 0);
    if (!idx) {
      goto err;
    }
  }

  if (!idx->numDocs) {
    // empty index! pass
    goto err;
  }

  IndexReader *ret = NewTermIndexReader(idx, ctx->spec, fieldMask, term, weight);
  if (csx) {
    ConcurrentSearch_AddKey(csx, k, REDISMODULE_READ, termKey, IndexReader_OnReopen, ret, NULL);
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

int Redis_DropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque) {
  // extract the term from the key
  RedisSearchCtx *sctx = opaque;
  RedisModuleString *pf = fmtRedisTermKey(sctx, "", 0);
  size_t pflen, len;
  RedisModule_StringPtrLen(pf, &pflen);

  char *k = (char *)RedisModule_StringPtrLen(kn, &len);
  k += pflen;
  // char *term = rm_strndup(k, len - pflen);

  RedisModuleString *sck = fmtRedisScoreIndexKey(sctx, k, len - pflen);
  RedisModuleString *sik = fmtRedisSkipIndexKey(sctx, k, len - pflen);

  RedisModule_Call(ctx, "DEL", "sss", kn, sck, sik);

  RedisModule_FreeString(ctx, sck);
  RedisModule_FreeString(ctx, sik);
  // free(term);

  return REDISMODULE_OK;
}

static int Redis_DeleteKey(RedisModuleCtx *ctx, RedisModuleString *s) {
  RedisModuleKey *k = RedisModule_OpenKey(ctx, s, REDISMODULE_WRITE);
  if (k != NULL) {
    RedisModule_DeleteKey(k);
    RedisModule_CloseKey(k);
    return 1;
  }
  return 0;
}

int Redis_DropIndex(RedisSearchCtx *ctx, int deleteDocuments, int deleteSpecKey) {

  if (deleteDocuments) {

    DocTable *dt = &ctx->spec->docs;
    DOCTABLE_FOREACH(dt, Redis_DeleteKey(ctx->redisCtx, DMD_CreateKeyString(dmd, ctx->redisCtx)));
  }

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  TrieIterator *it = Trie_Iterate(ctx->spec->terms, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModuleString *keyName = fmtRedisTermKey(ctx, res, strlen(res));
    Redis_DropScanHandler(ctx->redisCtx, keyName, ctx);
    RedisModule_FreeString(ctx->redisCtx, keyName);
    rm_free(res);
  }
  DFAFilter_Free(it->ctx);
  rm_free(it->ctx);
  TrieIterator_Free(it);

  // Delete the numeric, tag, and geo indexes which reside on separate keys
  for (size_t i = 0; i < ctx->spec->numFields; i++) {
    const FieldSpec *fs = ctx->spec->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
      Redis_DeleteKey(ctx->redisCtx, IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_NUMERIC));
    }
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      Redis_DeleteKey(ctx->redisCtx, IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_TAG));
    }
    if (FIELD_IS(fs, INDEXFLD_T_GEO)) {
      Redis_DeleteKey(ctx->redisCtx, IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_GEO));
    }
  }

  // Delete the index spec
  int deleted = 1;
  if (deleteSpecKey) {
    deleted = Redis_DeleteKey(
        ctx->redisCtx,
        RedisModule_CreateStringPrintf(ctx->redisCtx, INDEX_SPEC_KEY_FMT, ctx->spec->name));
  }
  return deleted ? REDISMODULE_OK : REDISMODULE_ERR;
}
