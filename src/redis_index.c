
#include "redis_index.h"
#include "doc_table.h"
#include "redismodule.h"
#include "inverted_index.h"
#include "tag_index.h"
#include "util/logging.h"
#include "util/misc.h"

#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "rmalloc.h"

#include <stdio.h>

///////////////////////////////////////////////////////////////////////////////////////////////

RedisModuleType *InvertedIndexType;

//---------------------------------------------------------------------------------------------

void *InvertedIndex_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver > INVERTED_INDEX_ENCVER) {
    return nullptr;
  }
  InvertedIndex *idx = new InvertedIndex(static_cast<IndexFlags>(RedisModule_LoadUnsigned(rdb)), 0);

  // If the data was encoded with a version that did not include the store numeric / store freqs
  // options - we force adding StoreFreqs.
  if (encver <= INVERTED_INDEX_NOFREQFLAG_VER) {
    idx->flags |= Index_StoreFreqs;
  }
  idx->lastId = RedisModule_LoadUnsigned(rdb);
  idx->numDocs = RedisModule_LoadUnsigned(rdb);
  idx->size = RedisModule_LoadUnsigned(rdb);
  idx->blocks = static_cast<IndexBlock*>(rm_calloc(idx->size, sizeof *idx->blocks));

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
      blk->buf.data = nullptr;
    } else {
      char *buf = static_cast<char *>(rm_malloc(blk->buf.offset));
      memcpy(buf, blk->buf.data, blk->buf.offset);
      RedisModule_Free(blk->buf.data);
      blk->buf.data = buf;
    }
  }
  idx->size = actualSize;
  if (idx->size == 0) {
    idx->AddBlock(t_docId{0});
  } else {
    idx->blocks = static_cast<IndexBlock*>(rm_realloc(idx->blocks, idx->size * sizeof *idx->blocks));
  }
  return idx;
}

//---------------------------------------------------------------------------------------------

void InvertedIndex_RdbSave(RedisModuleIO *rdb, void *value) {
  InvertedIndex *idx = static_cast<InvertedIndex*>(value);
  RedisModule_SaveUnsigned(rdb, idx->flags);
  RedisModule_SaveUnsigned(rdb, idx->lastId);
  RedisModule_SaveUnsigned(rdb, idx->numDocs);
  uint32_t readSize = 0;
  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock &blk = idx->blocks[i];
    if (blk.numDocs == 0) {
      continue;
    }
    ++readSize;
  }
  RedisModule_SaveUnsigned(rdb, readSize);

  for (uint32_t i = 0; i < idx->size; i++) {
    IndexBlock &blk = idx->blocks[i];
    if (blk.numDocs == 0) {
      continue;
    }
    RedisModule_SaveUnsigned(rdb, blk.firstId);
    RedisModule_SaveUnsigned(rdb, blk.lastId);
    RedisModule_SaveUnsigned(rdb, blk.numDocs);
    if (blk.DataLen()) {
      RedisModule_SaveStringBuffer(rdb, blk.DataBuf(), blk.DataLen());
    } else {
      RedisModule_SaveStringBuffer(rdb, "", 0);
    }
  }
}

//---------------------------------------------------------------------------------------------

void InvertedIndex_Digest(RedisModuleDigest *digest, void *value) {
}

//---------------------------------------------------------------------------------------------

unsigned long InvertedIndex_MemUsage(const void *value) {
  const InvertedIndex *idx = static_cast<const InvertedIndex *>(value);
  unsigned long ret = sizeof(InvertedIndex);
  for (size_t i = 0; i < idx->size; i++) {
    ret += sizeof(IndexBlock);
    ret += idx->blocks[i].DataLen();
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

int InvertedIndex_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = InvertedIndex_RdbLoad,
                               .rdb_save = InvertedIndex_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .mem_usage = InvertedIndex_MemUsage,
                               .free = InvertedIndex_Free};

  InvertedIndexType = RedisModule_CreateDataType(ctx, "ft_invidx", INVERTED_INDEX_ENCVER, &tm);
  if (InvertedIndexType == nullptr) {
    RedisModule_Log(ctx, "error", "Could not create inverted index type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Format redis key for a term.
// TODO: Add index name to it

RedisModuleString *RedisSearchCtx::TermKeyName(const String& term) {
  String buf = "ft:" + String{spec->name} + "/" + term;
  RedisModuleString *ret = RedisModule_CreateString(redisCtx, buf.data(), buf.length());
  return ret;
}

//---------------------------------------------------------------------------------------------

RedisModuleString *RedisSearchCtx::SkipIndexKeyName(const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(redisCtx, SKIPINDEX_KEY_FORMAT, spec->name, (int)len, term);
}

//---------------------------------------------------------------------------------------------

RedisModuleString *RedisSearchCtx::ScoreIndexKeyName(const char *term, size_t len) {
  return RedisModule_CreateStringPrintf(redisCtx, SCOREINDEX_KEY_FORMAT, spec->name, (int)len, term);
}

//---------------------------------------------------------------------------------------------

void RedisSearchCtx::ctor(RedisModuleCtx *ctx, const char *indexName, bool resetTTL) {
  redisCtx = ctx;
  IndexLoadOptions loadOpts(0, indexName);
  spec = IndexSpec::LoadEx(ctx, &loadOpts);
  key = loadOpts.keyp;
  if (!spec) {
    throw Error("Index %s does not exist", indexName);
  }
}

//---------------------------------------------------------------------------------------------

RedisSearchCtx::RedisSearchCtx(RedisModuleCtx *ctx, IndexSpecId id)
  : redisCtx {ctx}
  , key {nullptr}
  , spec {nullptr}
  , specId {id}
{ }

RedisSearchCtx::RedisSearchCtx(RedisModuleCtx *ctx, IndexSpec *spec_)
  : redisCtx {ctx}
  , key {nullptr}
  , spec {spec_}
  , specId {spec_->uniqueId}
{ }

RedisSearchCtx::RedisSearchCtx(RedisModuleCtx *ctx, const char *indexName, bool resetTTL) {
  ctor(ctx, indexName, resetTTL);
}

RedisSearchCtx::RedisSearchCtx(RedisModuleCtx *ctx, RedisModuleString *indexName, bool resetTTL) {
  ctor(ctx, RedisModule_StringPtrLen(indexName, nullptr), resetTTL);
}

RedisSearchCtx::RedisSearchCtx(const RedisSearchCtx &sctx) {
  redisCtx = sctx.redisCtx;
  key = nullptr;
  spec = sctx.spec;
  specId = sctx.specId;
}

//---------------------------------------------------------------------------------------------

void RedisSearchCtx::Refresh(RedisModuleString *keyName) {
  // First we close the relevant keys we're touching
  if (key) {
    RedisModule_CloseKey(key);
  }

  // now release the global lock
  RedisModule_ThreadSafeContextUnlock(redisCtx);
  // try to acquire it again...
  RedisModule_ThreadSafeContextLock(redisCtx);
  // reopen the context - it might have gone away!
  ctor(redisCtx, RedisModule_StringPtrLen(keyName, nullptr), true);
}

//---------------------------------------------------------------------------------------------

RedisSearchCtx::~RedisSearchCtx() {
  if (key) {
    RedisModule_CloseKey(key);
  }
}

//---------------------------------------------------------------------------------------------

// Select a random term from the index that matches the index prefix and inveted key format.
// It tries RANDOMKEY 10 times and returns nullptr if it can't find anything.

const char *Redis_SelectRandomTermByIndex(RedisSearchCtx *ctx, size_t *tlen) {
  RedisModuleString *pf = ctx->TermKeyName("");
  size_t pflen;
  const char *prefix = RedisModule_StringPtrLen(pf, &pflen);

  for (int i = 0; i < 10; i++) {
    RedisModuleCallReply *rep = RedisModule_Call(ctx->redisCtx, "RANDOMKEY", "");
    if (rep == nullptr || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_STRING) {
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
  return nullptr;
}

//---------------------------------------------------------------------------------------------

const char *Redis_SelectRandomTerm(RedisSearchCtx *ctx, size_t *tlen) {
  for (int i = 0; i < 5; i++) {
    RedisModuleCallReply *rep = RedisModule_Call(ctx->redisCtx, "RANDOMKEY", "");
    if (rep == nullptr || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_STRING) {
      break;
    }

    // get the key and see if it matches the prefix
    size_t len;
    RedisModuleString *krstr = RedisModule_CreateStringFromCallReply(rep);
    char *kstr = (char *)RedisModule_StringPtrLen(krstr, &len);
    if (!strncmp(kstr, TERM_KEY_PREFIX, strlen(TERM_KEY_PREFIX))) {
      // check to see that the key is indeed an inverted index record
      RedisModuleKey *k = static_cast<RedisModuleKey *>(RedisModule_OpenKey(ctx->redisCtx, krstr, REDISMODULE_READ));
      if (k == nullptr || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
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
      IndexSpec *sp = IndexSpec::Load(ctx->redisCtx, idx, 1);

      if (sp == nullptr) {
        continue;
      }
      ctx->spec = sp;
      return term;
    }
  }

  return nullptr;
}

//---------------------------------------------------------------------------------------------

static InvertedIndex *openIndexKeysDict(RedisSearchCtx *ctx, RedisModuleString *termKey, int write) {
  if (ctx->spec->keysDict.contains(termKey)) {
    BaseIndex *index = ctx->spec->keysDict[termKey];
    try {
      return dynamic_cast<InvertedIndex *>(index);
    } catch (std::bad_cast&) {
      throw Error("error: invalid index type...");
    }
  }

  if (!write) {
    return nullptr;
  }

  InvertedIndex *val = new InvertedIndex(ctx->spec->flags, 1);
  ctx->spec->keysDict.insert({termKey, val});
  return val;
}

//---------------------------------------------------------------------------------------------

InvertedIndex *Redis_OpenInvertedIndexEx(
  RedisSearchCtx *sctx, const char *term, size_t len, int write, RedisModuleKey **keyp
) {
  RedisModuleString *termKey = sctx->TermKeyName({term, len});
  InvertedIndex *idx = nullptr;

  if (sctx->spec->keysDict.empty()) {
    RedisModuleKey *k = static_cast<RedisModuleKey *>(RedisModule_OpenKey(
      sctx->redisCtx, termKey, REDISMODULE_READ | (write ? REDISMODULE_WRITE : 0)
    ));

    // check that the key is empty
    if (k == nullptr) {
      goto end;
    }

    int kType = RedisModule_KeyType(k);

    if (kType == REDISMODULE_KEYTYPE_EMPTY) {
      if (write) {
        idx = new InvertedIndex(sctx->spec->flags, 1);
        RedisModule_ModuleTypeSetValue(k, InvertedIndexType, idx);
      }
    } else if (kType == REDISMODULE_KEYTYPE_MODULE &&
               RedisModule_ModuleTypeGetType(k) == InvertedIndexType) {
      idx = static_cast<InvertedIndex *>(RedisModule_ModuleTypeGetValue(k));
    }
    if (idx == nullptr) {
      RedisModule_CloseKey(k);
    } else {
      if (keyp) {
        *keyp = k;
      }
    }
  } else {
    idx = openIndexKeysDict(sctx, termKey, write);
  }

end:
  RedisModule_FreeString(sctx->redisCtx, termKey);
  return idx;
}

//---------------------------------------------------------------------------------------------

IndexReader *Redis_OpenReader(RedisSearchCtx *sctx, RSQueryTerm *term, DocTable *dt, int singleWordMode,
                              t_fieldMask fieldMask, ConcurrentSearch *csx, double weight) {
  RedisModuleString *termKey = sctx->TermKeyName(term->str);
  InvertedIndex *idx = nullptr;
  RedisModuleKey *k = nullptr;
  TermIndexReader *reader = nullptr;

  if (sctx->spec->keysDict.empty()) {
    k = static_cast<RedisModuleKey *>(RedisModule_OpenKey(sctx->redisCtx, termKey, REDISMODULE_READ));

    // we do not allow empty indexes when loading an existing index
    if (k == nullptr || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
        RedisModule_ModuleTypeGetType(k) != InvertedIndexType) {
      goto err;
    }

    idx = static_cast<InvertedIndex *>(RedisModule_ModuleTypeGetValue(k));
  } else {
    idx = openIndexKeysDict(sctx, termKey, 0);
    if (!idx) {
      goto err;
    }
  }

  if (!idx->numDocs) {
    // empty index! pass
    goto err;
  }

  reader = new TermIndexReader(idx, sctx->spec, fieldMask, term, weight);
  if (csx) {
    csx->AddKey(TermIndexReaderConcKey(k, termKey, reader));
  }
  RedisModule_FreeString(sctx->redisCtx, termKey);
  return reader;

err:
  if (k) {
    RedisModule_CloseKey(k);
  }
  if (termKey) {
    RedisModule_FreeString(sctx->redisCtx, termKey);
  }
  return nullptr;
}

//---------------------------------------------------------------------------------------------

int Redis_ScanKeys(RedisModuleCtx *ctx, const char *prefix, ScanFunc f, void *opaque) {
  long long ptr = 0;

  int num = 0;
  do {
    RedisModuleString *sptr = RedisModule_CreateStringFromLongLong(ctx, ptr);
    RedisModuleCallReply *r =
        RedisModule_Call(ctx, "SCAN", "scccc", sptr, "MATCH", prefix, "COUNT", "100");
    RedisModule_FreeString(ctx, sptr);
    if (r == nullptr || RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR) {
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
    // nullptr), ptr);
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

//---------------------------------------------------------------------------------------------

int Redis_DropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn, void *opaque) {
  // extract the term from the key
  RedisSearchCtx *sctx = static_cast<RedisSearchCtx *>(opaque);
  RedisModuleString *pf = sctx->TermKeyName("");
  size_t pflen, len;
  RedisModule_StringPtrLen(pf, &pflen);

  char *k = (char *)RedisModule_StringPtrLen(kn, &len);
  k += pflen;
  // char *term = rm_strndup(k, len - pflen);

  RedisModuleString *sck = sctx->ScoreIndexKeyName(k, len - pflen);
  RedisModuleString *sik = sctx->SkipIndexKeyName(k, len - pflen);

  RedisModule_Call(ctx, "DEL", "sss", kn, sck, sik);

  RedisModule_FreeString(ctx, sck);
  RedisModule_FreeString(ctx, sik);
  // free(term);

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

static bool Redis_DeleteKey(RedisModuleCtx *ctx, RedisModuleString *s) {
  RedisModuleKey *k = static_cast<RedisModuleKey *>(RedisModule_OpenKey(ctx, s, REDISMODULE_WRITE));
  if (k != nullptr) {
    RedisModule_DeleteKey(k);
    RedisModule_CloseKey(k);
    return true;
  }
  return false;
}

//---------------------------------------------------------------------------------------------

int Redis_DropIndex(RedisSearchCtx *ctx, int deleteDocuments, int deleteSpecKey) {
  RedisModuleCtx *redisCtx = ctx->redisCtx;
  if (deleteDocuments) {
    ctx->spec->docs.foreach([&](DocumentMetadata &dmd) {
        Redis_DeleteKey(redisCtx, dmd.CreateKeyString(redisCtx));
      });
  }

  Runes runes;
  float score = 0;
  int dist = 0;
  size_t termLen;
  RSPayload payload;

  TrieIterator it = ctx->spec->terms->Iterate("", 0, 1);
  while (it.Next(runes, payload, score, &dist)) {
    char *res = runes.toUTF8(&termLen);
    RedisModuleString *keyName = ctx->TermKeyName(res);
    Redis_DropScanHandler(redisCtx, keyName, ctx);
    RedisModule_FreeString(redisCtx, keyName);
  }

  // Delete the numeric, tag, and geo indexes which reside on separate keys
  for (auto const &fs : ctx->spec->fields) {
    if (fs.IsFieldType(INDEXFLD_T_NUMERIC)) {
      Redis_DeleteKey(redisCtx, ctx->spec->GetFormattedKey(fs, INDEXFLD_T_NUMERIC));
    }
    if (fs.IsFieldType(INDEXFLD_T_TAG)) {
      Redis_DeleteKey(redisCtx, ctx->spec->GetFormattedKey(fs, INDEXFLD_T_TAG));
    }
    if (fs.IsFieldType(INDEXFLD_T_GEO)) {
      Redis_DeleteKey(redisCtx, ctx->spec->GetFormattedKey(fs, INDEXFLD_T_GEO));
    }
  }

  // Delete the index spec
  bool deleted = true;
  if (deleteSpecKey) {
    deleted = Redis_DeleteKey(redisCtx, RedisModule_CreateStringPrintf(redisCtx, INDEX_SPEC_KEY_FMT, ctx->spec->name));
  }
  return deleted ? REDISMODULE_OK : REDISMODULE_ERR;
}

///////////////////////////////////////////////////////////////////////////////////////////////
