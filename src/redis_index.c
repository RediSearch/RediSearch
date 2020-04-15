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
#include "module.h"
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
void InvertedIndex_Digest(RedisModuleDigest *digest, void *value) {
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

RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName, bool resetTTL) {
  IndexLoadOptions loadOpts = {.name = {.cstring = indexName}};
  IndexSpec *sp = IndexSpec_LoadEx(ctx, &loadOpts);

  if (!sp) {
    return NULL;
  }

  RedisSearchCtx *sctx = rm_malloc(sizeof(*sctx));
  *sctx = (RedisSearchCtx){.spec = sp,  // newline
                           .redisCtx = ctx,
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

RedisSearchCtx *NewSearchCtxDefault(RedisModuleCtx *ctx) {
  RedisSearchCtx *sctx = rm_malloc(sizeof(*sctx));
  *sctx = (RedisSearchCtx){.redisCtx = ctx};
  return sctx;
}

void SearchCtx_Free(RedisSearchCtx *sctx) {
  if (sctx->key_) {
    RedisModule_CloseKey(sctx->key_);
    sctx->key_ = NULL;
  }
  if (!sctx->isStatic) {
    rm_free(sctx);
  }
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
