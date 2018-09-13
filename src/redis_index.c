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

  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock *blk = &idx->blocks[i];
    blk->firstId = RedisModule_LoadUnsigned(rdb);
    blk->lastId = RedisModule_LoadUnsigned(rdb);
    blk->numDocs = RedisModule_LoadUnsigned(rdb);

    size_t cap;
    char *data = RedisModule_LoadStringBuffer(rdb, &cap);

    blk->data = Buffer_Wrap(cap > 0 ? data : NULL, cap);
    blk->data->offset = cap;
    // if we read a buffer of 0 bytes we still read 1 byte from the RDB that needs to be freed
    if (!cap && data) RedisModule_Free(data);
  }
  return idx;
}
void InvertedIndex_RdbSave(RedisModuleIO *rdb, void *value) {

  InvertedIndex *idx = value;
  RedisModule_SaveUnsigned(rdb, idx->flags);
  RedisModule_SaveUnsigned(rdb, idx->lastId);
  RedisModule_SaveUnsigned(rdb, idx->numDocs);
  RedisModule_SaveUnsigned(rdb, idx->size);

  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock *blk = &idx->blocks[i];
    RedisModule_SaveUnsigned(rdb, blk->firstId);
    RedisModule_SaveUnsigned(rdb, blk->lastId);
    RedisModule_SaveUnsigned(rdb, blk->numDocs);
    RedisModule_SaveStringBuffer(rdb, blk->data->data ? blk->data->data : "", blk->data->offset);
  }
}
void InvertedIndex_Digest(RedisModuleDigest *digest, void *value) {
}

unsigned long InvertedIndex_MemUsage(const void *value) {
  const InvertedIndex *idx = value;
  unsigned long ret = sizeof(InvertedIndex);
  for (size_t i = 0; i < idx->size; i++) {
    ret += sizeof(IndexBlock);
    ret += sizeof(Buffer);
    ret += Buffer_Offset(idx->blocks[i].data);
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

RedisSearchCtx *NewSearchCtxC(RedisModuleCtx *ctx, const char *indexName) {

  RedisModuleString *keyName = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, indexName);

  RedisModuleKey *k = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
  // printf("open key %s: %p\n", RedisModule_StringPtrLen(keyName, NULL), k);
  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
    return NULL;
  }
  IndexSpec *sp = RedisModule_ModuleTypeGetValue(k);

  RedisSearchCtx *sctx = rm_malloc(sizeof(*sctx));
  *sctx = (RedisSearchCtx){
      .spec = sp,
      .redisCtx = ctx,
      .key = k,
      .keyName = keyName,
  };
  return sctx;
}

RedisSearchCtx *NewSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName) {

  return NewSearchCtxC(ctx, RedisModule_StringPtrLen(indexName, NULL));
}

RedisSearchCtx *SearchCtx_Refresh(RedisSearchCtx *sctx, RedisModuleString *keyName) {
  // First we close the relevant keys we're touching
  RedisModule_CloseKey(sctx->key);
  RedisModuleCtx *redisCtx = sctx->redisCtx;
  SearchCtx_Free(sctx);
  // now release the global lock
  RedisModule_ThreadSafeContextUnlock(redisCtx);
  // try to acquire it again...
  RedisModule_ThreadSafeContextLock(redisCtx);
  // reopen the context - it might have gone away!
  return NewSearchCtx(redisCtx, keyName);
}

RedisSearchCtx *NewSearchCtxDefault(RedisModuleCtx *ctx) {
  RedisSearchCtx *sctx = rm_malloc(sizeof(*sctx));
  *sctx = (RedisSearchCtx){.redisCtx = ctx};
  return sctx;
}

void SearchCtx_Free(RedisSearchCtx *sctx) {
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

InvertedIndex *Redis_OpenInvertedIndexEx(RedisSearchCtx *ctx, const char *term, size_t len,
                                         int write, RedisModuleKey **keyp) {
  RedisModuleString *termKey = fmtRedisTermKey(ctx, term, len);
  RedisModuleKey *k = RedisModule_OpenKey(ctx->redisCtx, termKey,
                                          REDISMODULE_READ | (write ? REDISMODULE_WRITE : 0));

  RedisModule_FreeString(ctx->redisCtx, termKey);
  InvertedIndex *idx = NULL;

  // check that the key is empty
  if (k == NULL) {
    return NULL;
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
    return NULL;
  } else {
    if (keyp) {
      *keyp = k;
    }
    return idx;
  }
}

IndexReader *Redis_OpenReader(RedisSearchCtx *ctx, RSQueryTerm *term, DocTable *dt,
                              int singleWordMode, t_fieldMask fieldMask, ConcurrentSearchCtx *csx,
                              double weight) {

  RedisModuleString *termKey = fmtRedisTermKey(ctx, term->str, term->len);
  RedisModuleKey *k = RedisModule_OpenKey(ctx->redisCtx, termKey, REDISMODULE_READ);

  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(k) != InvertedIndexType) {
    RedisModule_FreeString(ctx->redisCtx, termKey);
    return NULL;
  }

  InvertedIndex *idx = RedisModule_ModuleTypeGetValue(k);

  IndexReader *ret = NewTermIndexReader(idx, dt, fieldMask, term, weight);
  if (csx) {
    ConcurrentSearch_AddKey(csx, k, REDISMODULE_READ, termKey, IndexReader_OnReopen, ret, NULL,
                            ConcurrentKey_SharedNothing);
  }
  return ret;
}

int Redis_LoadDocument(RedisSearchCtx *ctx, RedisModuleString *key, Document *doc) {
  doc->numFields = 0;
  doc->fields = NULL;
  RedisModuleCallReply *rep = RedisModule_Call(ctx->redisCtx, "HGETALL", "s", key);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    return REDISMODULE_ERR;
  }

  size_t len = RedisModule_CallReplyLength(rep);
  // Zero means the document does not exist in redis
  if (len == 0) {
    return REDISMODULE_ERR;
  }
  doc->fields = calloc(len / 2, sizeof(DocumentField));
  doc->numFields = len / 2;
  int n = 0;
  RedisModuleCallReply *k, *v;
  for (int i = 0; i < len; i += 2, ++n) {
    k = RedisModule_CallReplyArrayElement(rep, i);
    v = RedisModule_CallReplyArrayElement(rep, i + 1);
    doc->fields[n].name = RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(k), NULL);
    doc->fields[n].text = RedisModule_CreateStringFromCallReply(v);
  }

  return REDISMODULE_OK;
}

int Redis_LoadDocumentEx(RedisSearchCtx *ctx, RedisModuleString *key, const char **fields,
                         size_t nfields, Document *doc, RedisModuleKey **rkeyp) {
  RedisModuleKey *rkeyp_s = NULL;
  if (!rkeyp) {
    rkeyp = &rkeyp_s;
  }

  *rkeyp = NULL;
  if (!fields) {
    return Redis_LoadDocument(ctx, key, doc);
  }

  // Get the key itself
  *rkeyp = RedisModule_OpenKey(ctx->redisCtx, key, REDISMODULE_READ);
  if (*rkeyp == NULL) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_KeyType(*rkeyp) != REDISMODULE_KEYTYPE_HASH) {
    RedisModule_CloseKey(*rkeyp);
    return REDISMODULE_ERR;
  }

  doc->fields = malloc(sizeof(*doc->fields) * nfields);

  for (size_t ii = 0; ii < nfields; ++ii) {
    int rv = RedisModule_HashGet(*rkeyp, REDISMODULE_HASH_CFIELDS, fields[ii],
                                 &doc->fields[ii].text, NULL);
    if (rv == REDISMODULE_OK) {
      doc->numFields++;
      doc->fields[ii].name = fields[ii];
    }
  }

  return REDISMODULE_OK;
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

int Redis_DropIndex(RedisSearchCtx *ctx, int deleteDocuments) {

  if (deleteDocuments) {

    DocTable *dt = &ctx->spec->docs;
    DocTable_ForEach(dt, Redis_DeleteKey(ctx->redisCtx, DMD_CreateKeyString(dmd, ctx->redisCtx)));
  }

  // Delete any dangling term keys
  RedisModuleString *pf = fmtRedisTermKey(ctx, "*", 1);
  const char *prefix = RedisModule_StringPtrLen(pf, NULL);
  Redis_ScanKeys(ctx->redisCtx, prefix, Redis_DropScanHandler, ctx);

  // Do the same with geo keys
  pf = RedisModule_CreateStringPrintf(ctx->redisCtx, GEOINDEX_KEY_FMT, ctx->spec->name, "*");
  prefix = RedisModule_StringPtrLen(pf, NULL);
  Redis_ScanKeys(ctx->redisCtx, prefix, Redis_DropScanHandler, ctx);

  // Delete the numeric and tag indexes which reside on separate keys
  for (size_t i = 0; i < ctx->spec->numFields; i++) {
    const FieldSpec *spec = ctx->spec->fields + i;
    if (spec->type == FIELD_NUMERIC) {
      Redis_DeleteKey(ctx->redisCtx, fmtRedisNumericIndexKey(ctx, spec->name));
    } else if (spec->type == FIELD_TAG) {
      Redis_DeleteKey(ctx->redisCtx, TagIndex_FormatName(ctx, spec->name));
    }
  }

  // Delete the index spec
  int deleted = Redis_DeleteKey(
      ctx->redisCtx,
      RedisModule_CreateStringPrintf(ctx->redisCtx, INDEX_SPEC_KEY_FMT, ctx->spec->name));
  return deleted ? REDISMODULE_OK : REDISMODULE_ERR;
}
