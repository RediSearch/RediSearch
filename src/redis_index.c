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
    buf = bufDyn = calloc(1, nameLen + len + 10);
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
  free(bufDyn);
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
  rm_free(sctx);
}
/*
 * Select a random term from the index that matches the index prefix and inveted key format.
 * It tries RANDOMKEY 10 times and returns NULL if it can't find anything.
 */
const char *Redis_SelectRandomTermByIndex(RedisSearchCtx *ctx, size_t *tlen) {

  RedisModuleString *pf = fmtRedisTermKey(ctx, "", 0);
  size_t pflen;
  const char *prefix = RedisModule_StringPtrLen(pf, &pflen);

  for (int i = 0; i < 10; i++) {
    RedisModuleCallReply *rep = RedisModule_Call(ctx->redisCtx, "RANDOMKEY", "");
    if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_STRING) {
      break;
    }

    // get the key and see if it matches the prefix
    size_t len;
    const char *kstr = RedisModule_CallReplyStringPtr(rep, &len);
    if (!strncmp(kstr, prefix, pflen)) {
      *tlen = len - pflen;
      return kstr + pflen;
    }
  }
  *tlen = 0;
  return NULL;
}

const char *Redis_SelectRandomTerm(RedisSearchCtx *ctx, size_t *tlen) {

  for (int i = 0; i < 5; i++) {
    RedisModuleCallReply *rep = RedisModule_Call(ctx->redisCtx, "RANDOMKEY", "");
    if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_STRING) {
      break;
    }

    // get the key and see if it matches the prefix
    size_t len;
    RedisModuleString *krstr = RedisModule_CreateStringFromCallReply(rep);
    char *kstr = (char *)RedisModule_StringPtrLen(krstr, &len);
    if (!strncmp(kstr, TERM_KEY_PREFIX, strlen(TERM_KEY_PREFIX))) {
      // check to see that the key is indeed an inverted index record
      RedisModuleKey *k = RedisModule_OpenKey(ctx->redisCtx, krstr, REDISMODULE_READ);
      if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                        RedisModule_ModuleTypeGetType(k) != InvertedIndexType)) {
        continue;
      }
      RedisModule_CloseKey(k);
      size_t offset = strlen(TERM_KEY_PREFIX);
      char *idx = kstr + offset;
      while (offset < len && kstr[offset] != '/') {
        offset++;
      }
      if (offset < len) {
        kstr[offset++] = '\0';
      }
      char *term = kstr + offset;
      *tlen = len - offset;
      // printf("Found index %s and term %sm len %zd\n", idx, term, *tlen);
      IndexSpec *sp = IndexSpec_Load(ctx->redisCtx, idx, 1);
      // printf("Spec: %p\n", sp);

      if (sp == NULL) {
        continue;
      }
      ctx->spec = sp;
      return term;
    }
  }

  return NULL;
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

  kdv = calloc(1, sizeof(*kdv));
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
      RedisModule_FreeString(ctx->redisCtx, termKey);
      return NULL;
    }

    idx = RedisModule_ModuleTypeGetValue(k);
  } else {
    idx = openIndexKeysDict(ctx, termKey, 0);
    if (!idx) {
      RedisModule_FreeString(ctx->redisCtx, termKey);
      return NULL;
    }
  }

  if (!idx->numDocs) {
    // empty index! pass
    RedisModule_FreeString(ctx->redisCtx, termKey);
    return NULL;
  }

  IndexReader *ret = NewTermIndexReader(idx, ctx->spec, fieldMask, term, weight);
  if (csx) {
    ConcurrentSearch_AddKey(csx, k, REDISMODULE_READ, termKey, IndexReader_OnReopen, ret, NULL);
  }
  RedisModule_FreeString(ctx->redisCtx, termKey);
  return ret;
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

  char *k = (char *)RedisModule_StringPtrLen(kn, &len);
  k += pflen;
  // char *term = strndup(k, len - pflen);

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
    free(res);
  }
  DFAFilter_Free(it->ctx);
  free(it->ctx);
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
