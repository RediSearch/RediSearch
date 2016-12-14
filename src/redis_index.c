#include <stdio.h>
#include "redis_index.h"
#include "util/logging.h"
#include "doc_table.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"

/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisSearchCtx *ctx, const char *term,
                                   size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, TERM_KEY_FORMAT,
                                        ctx->spec->name, len, term);
}

RedisModuleString *fmtRedisSkipIndexKey(RedisSearchCtx *ctx, const char *term,
                                        size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SKIPINDEX_KEY_FORMAT,
                                        ctx->spec->name, len, term);
}

RedisModuleString *fmtRedisScoreIndexKey(RedisSearchCtx *ctx, const char *term,
                                         size_t len) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, SCOREINDEX_KEY_FORMAT,
                                        ctx->spec->name, len, term);
}
/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisSearchCtx *ctx, const char *term,
                              size_t len) {
  // Open the index writer
  RedisModuleString *termKey = fmtRedisTermKey(ctx, term, len);
  BufferWriter bw = NewRedisWriter(ctx->redisCtx, termKey);
  RedisModule_FreeString(ctx->redisCtx, termKey);
  // Open the skip index writer
  termKey = fmtRedisSkipIndexKey(ctx, term, len);
  Buffer *sb = NewRedisBuffer(ctx->redisCtx, termKey, BUFFER_WRITE);
  BufferWriter skw = {
      sb, redisWriterWrite, redisWriterTruncate, RedisBufferFree,
  };
  RedisModule_FreeString(ctx->redisCtx, termKey);

  if (sb->cap > sizeof(u_int32_t)) {
    u_int32_t len;

    BufferRead(sb, &len, sizeof(len));
    BufferSeek(sb, sizeof(len) + len * sizeof(SkipEntry));
  }

  termKey = fmtRedisScoreIndexKey(ctx, term, len);

  // Open the score index writer
  ScoreIndexWriter scw = NewScoreIndexWriter(
      NewRedisWriter(ctx->redisCtx, fmtRedisScoreIndexKey(ctx, term, len)));
  RedisModule_FreeString(ctx->redisCtx, termKey);
  IndexWriter *w = NewIndexWriterBuf(bw, skw, scw, INDEX_DEFAULT_FLAGS);
  return w;
}

void Redis_CloseWriter(IndexWriter *w) {
  IW_Close(w);
  RedisBufferFree(w->bw.buf);
  RedisBufferFree(w->skipIndexWriter.buf);
  RedisBufferFree(w->scoreWriter.bw.buf);
  free(w);
}

SkipIndex *LoadRedisSkipIndex(RedisSearchCtx *ctx, const char *term,
                              size_t len) {
  Buffer *b = NewRedisBuffer(ctx->redisCtx,
                             fmtRedisSkipIndexKey(ctx, term, len), BUFFER_READ);
  if (b && b->cap > sizeof(SkipEntry)) {
    SkipIndex *si = malloc(sizeof(SkipIndex));
    BufferRead(b, &si->len, sizeof(si->len));
    si->entries = (SkipEntry *)b->pos;

    RedisBufferFree(b);
    return si;
  }
  RedisBufferFree(b);
  return NULL;
}

ScoreIndex *LoadRedisScoreIndex(RedisSearchCtx *ctx, const char *term,
                                size_t len) {
  Buffer *b = NewRedisBuffer(
      ctx->redisCtx, fmtRedisScoreIndexKey(ctx, term, len), BUFFER_READ);
  if (b == NULL || b->cap <= sizeof(ScoreIndexEntry)) {
    return NULL;
  }
  return NewScoreIndex(b);
}

IndexReader *Redis_OpenReader(RedisSearchCtx *ctx, const char *term, size_t len,
                              DocTable *dt, int singleWordMode,
                              u_char fieldMask) {
  Buffer *b = NewRedisBuffer(ctx->redisCtx, fmtRedisTermKey(ctx, term, len),
                             BUFFER_READ);
  if (b == NULL) { // not found
    return NULL;
  }
  SkipIndex *si = NULL;
  ScoreIndex *sci = NULL;
  if (singleWordMode) {
    sci = LoadRedisScoreIndex(ctx, term, len);
  } else {
    si = LoadRedisSkipIndex(ctx, term, len);
  }

  return NewIndexReaderBuf(b, si, dt, singleWordMode, sci, fieldMask);
}

void Redis_CloseReader(IndexReader *r) {
  // we don't call IR_Free because it frees the underlying memory right now

  RedisBufferFree(r->buf);

  if (r->skipIdx != NULL) {
    free(r->skipIdx);
  }
  if (r->scoreIndex != NULL) {
    ScoreIndex_Free(r->scoreIndex);
  }
  free(r);
}



/**
Get a numeric incrementing doc Id for indexing, from a string docId of the
document.
We either fetch it from a map of doc key => id, or we increment the top id and
write to that map
@return docId, or 0 on error, meaning we can't index the document

TODO: Detect if the id is numeric and don't convert it
*/
t_docId Redis_GetDocId(RedisSearchCtx *ctx, RedisModuleString *docKey,
                       int *isnew) {
  *isnew = 0;
    
  if (!ctx->docKeyTableKey) {
    
    RedisModuleString *kstr = RedisModule_CreateString(
        ctx->redisCtx, REDISINDEX_DOCKEY_MAP, strlen(REDISINDEX_DOCKEY_MAP));

    ctx->docKeyTableKey = RedisModule_OpenKey(
        ctx->redisCtx, kstr, REDISMODULE_WRITE | REDISMODULE_READ);
    if (ctx->docKeyTableKey == NULL ||
        (RedisModule_KeyType(ctx->docKeyTableKey) != REDISMODULE_KEYTYPE_EMPTY &&
         RedisModule_KeyType(ctx->docKeyTableKey) != REDISMODULE_KEYTYPE_HASH)) {
      return 0;
    }
  }


  // try loading the id 
  RedisModuleString *docIdStr = NULL;
  long long docId = 0;

  if (RedisModule_HashGet(ctx->docKeyTableKey, REDISMODULE_HASH_NONE, docKey,
                          &docIdStr, NULL) == REDISMODULE_ERR) {
    return 0;
  }
  if (docIdStr != NULL) {
    if (RedisModule_StringToLongLong(docIdStr, &docId) == REDISMODULE_ERR) {
      return 0;
    }
    return (t_docId)docId;
  }

  // not found - increment the global id counter and set in the map
  RedisModuleCallReply *increp =
      RedisModule_Call(ctx->redisCtx, "INCR", "c", REDISINDEX_DOCIDCOUNTER);
  if (increp == NULL)
    return 0;

  long long ll = RedisModule_CallReplyInteger(increp);
  RedisModuleString *ls =
      RedisModule_CreateStringFromLongLong(ctx->redisCtx, ll);

  // map docId => key
  RedisModule_Call(ctx->redisCtx, "HSET", "css", REDISINDEX_DOCIDS_MAP, ls,
                   docKey);
  // map key => docId
  RedisModule_Call(ctx->redisCtx, "HSET", "css", REDISINDEX_DOCKEY_MAP, docKey,
                   ls);
  *isnew = 1;
  return (t_docId)ll;
}

RedisModuleString *Redis_GetDocKey(RedisSearchCtx *ctx, t_docId docId) {

  if (!ctx->docIdTableKey) {
    RedisModuleString *kstr = RedisModule_CreateString(
        ctx->redisCtx, REDISINDEX_DOCIDS_MAP, strlen(REDISINDEX_DOCIDS_MAP));

    ctx->docIdTableKey = RedisModule_OpenKey(ctx->redisCtx, kstr, REDISMODULE_READ);
    if (ctx->docIdTableKey == NULL ||
        (RedisModule_KeyType(ctx->docIdTableKey) != REDISMODULE_KEYTYPE_EMPTY &&
         RedisModule_KeyType(ctx->docIdTableKey) != REDISMODULE_KEYTYPE_HASH)) {
      return NULL;
    }
  }


  // try loading the id 
  RedisModuleString *docKey = NULL;
  static char buf[64];
  snprintf(buf, 64, "%d", docId);
  RedisModule_HashGet(ctx->docIdTableKey, REDISMODULE_HASH_CFIELDS, buf,
                          &docKey, NULL);
  return docKey;
  
}

/**
Open the doc table key. Return REDISMODULE_ERR if failed
*/
int InitDocTable(RedisSearchCtx *ctx, DocTable *t) {
  char buf[strlen(ctx->spec->name) + 16];
  sprintf(buf, DOCTABLE_KEY_FMT, ctx->spec->name);

  RedisModuleKey *k = RedisModule_OpenKey(
      ctx->redisCtx, RedisModule_CreateString(ctx->redisCtx, buf, strlen(buf)),
      REDISMODULE_READ | REDISMODULE_WRITE);

  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    return REDISMODULE_ERR;
  }

  t->ctx = ctx->redisCtx;
  t->key = k;
  return REDISMODULE_OK;
}

int DocTable_GetMetadata(DocTable *t, t_docId docId, DocumentMetadata *md) {
  md->score = 0;
  md->flags = 0;
  // memset(md, 0, sizeof(DocumentMetadata));
  return REDISMODULE_OK;
  char buf[32];
  snprintf(buf, 32, DOCTABLE_DOCID_KEY_FMT, docId);

  RedisModuleString *data = NULL;
  int rc =
      RedisModule_HashGet(t->key, REDISMODULE_HASH_CFIELDS, buf, &data, NULL);
  if (rc == REDISMODULE_ERR || data == NULL) {
    return REDISMODULE_ERR;
  }
  RedisModule_FreeString(t->ctx, data);

  size_t len;
  const char *p = RedisModule_StringPtrLen(data, &len);
  int ret = REDISMODULE_ERR;
  if (len == sizeof(DocumentMetadata)) {
    memcpy(md, p, sizeof(DocumentMetadata));
    ret = REDISMODULE_OK;
  }

  RedisModule_FreeString(t->ctx, data);
  return ret;
}

int DocTable_PutDocument(DocTable *t, t_docId docId, double score,
                         u_short flags) {
  char buf[32];
  int sz = snprintf(buf, 32, DOCTABLE_DOCID_KEY_FMT, docId);

  DocumentMetadata md = {score, flags};

  RedisModuleString *data =
      RedisModule_CreateString(t->ctx, (char *)&md, sizeof(DocumentMetadata));

  LG_DEBUG("Writing META %s -> %p. doc score :%f\n", buf, data, score);
  int rc =
      RedisModule_HashSet(t->key, REDISMODULE_HASH_CFIELDS, buf, data, NULL);
  RedisModule_FreeString(t->ctx, data);
  return rc;
}

void Document_Free(Document doc) { free(doc.fields); }

int Redis_LoadDocument(RedisSearchCtx *ctx, RedisModuleString *key,
                       Document *doc) {
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx->redisCtx, "HGETALL", "s", key);
  RMUTIL_ASSERT_NOERROR(ctx->redisCtx, rep);
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
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
    doc->fields[n].name = RedisModule_CreateStringFromCallReply(k);
    doc->fields[n].text = RedisModule_CreateStringFromCallReply(v);
  }

  return REDISMODULE_OK;
}

Document NewDocument(RedisModuleString *docKey, double score, int numFields,
                     const char *lang) {
  Document doc;
  doc.docKey = docKey;
  doc.score = (float)score;
  doc.numFields = numFields;
  doc.fields = calloc(doc.numFields, sizeof(DocumentField));
  doc.language = lang;

  return doc;
}

Document *Redis_LoadDocuments(RedisSearchCtx *ctx, RedisModuleString **keys,
                              int numKeys, int *nump) {
  Document *docs = calloc(numKeys, sizeof(Document));
  int n = 0;

  for (int i = 0; i < numKeys; i++) {
    if (Redis_LoadDocument(ctx, keys[i], &docs[n]) == REDISMODULE_OK) {
      docs[n].docKey = keys[i];

      n++;
    }
  }

  *nump = n;
  return docs;
}

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc) {

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, doc->docKey, REDISMODULE_WRITE);
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < doc->numFields; i++) {
    if (RedisModule_HashSet(k, REDISMODULE_HASH_NONE, doc->fields[i].name,
                            doc->fields[i].text, NULL) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }

  return REDISMODULE_OK;
}

int Redis_ScanKeys(RedisModuleCtx *ctx, const char *prefix, ScanFunc f,
                   void *opaque) {
  long long ptr = 0;

  int num = 0;
  do {
    RedisModuleString *sptr = RedisModule_CreateStringFromLongLong(ctx, ptr);
    RedisModuleCallReply *r = RedisModule_Call(ctx, "SCAN", "scccc", sptr,
                                               "MATCH", prefix, "COUNT", "100");
    RedisModule_FreeString(ctx, sptr);
    if (r == NULL || RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR) {
      return num;
    }

    if (RedisModule_CallReplyLength(r) < 1) {
      break;
    }

    sptr = RedisModule_CreateStringFromCallReply(
        RedisModule_CallReplyArrayElement(r, 0));
    RedisModule_StringToLongLong(sptr, &ptr);
    RedisModule_FreeString(ctx, sptr);
    // printf("ptr: %s %lld\n",
    // RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(r, 0),
    // NULL), ptr);
    if (RedisModule_CallReplyLength(r) == 2) {
      RedisModuleCallReply *keys = RedisModule_CallReplyArrayElement(r, 1);
      size_t nks = RedisModule_CallReplyLength(keys);

      for (size_t i = 0; i < nks; i++) {
        RedisModuleString *kn = RedisModule_CreateStringFromCallReply(
            RedisModule_CallReplyArrayElement(keys, i));
        if (f(ctx, kn, opaque) != REDISMODULE_OK)
          goto end;

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

int Redis_OptimizeScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn,
                              void *opaque) {
  // extract the term from the key
  RedisSearchCtx *sctx = opaque;
  RedisModuleString *pf = fmtRedisTermKey(sctx, "", 0);
  size_t pflen, len;
  const char *prefix = RedisModule_StringPtrLen(pf, &pflen);

  char *k = (char *)RedisModule_StringPtrLen(kn, &len);
  k += pflen;

  // Open the index writer for the term
  IndexWriter *w = Redis_OpenWriter(sctx, k, len - pflen);
  if (w) {
    // Truncate the main index buffer to its final size
    w->bw.Truncate(w->bw.buf, 0);

    // for small entries, delete the score index
    if (w->ndocs < SCOREINDEX_DELETE_THRESHOLD) {
      RedisBufferCtx *bctx = w->scoreWriter.bw.buf->ctx;
      RedisModule_DeleteKey(bctx->key);
      RedisModule_CloseKey(bctx->key);
      bctx->key = NULL;
    } else {
      // truncate the score index to its final size
      w->scoreWriter.bw.Truncate(w->scoreWriter.bw.buf, 0);
    }

    // truncate the skip index
    w->skipIndexWriter.Truncate(w->skipIndexWriter.buf, 0);

    Redis_CloseWriter(w);
  }

  RedisModule_FreeString(ctx, pf);

  return REDISMODULE_OK;
}

int Redis_DropScanHandler(RedisModuleCtx *ctx, RedisModuleString *kn,
                          void *opaque) {
  // extract the term from the key
  RedisSearchCtx *sctx = opaque;
  RedisModuleString *pf = fmtRedisTermKey(sctx, "", 0);
  size_t pflen, len;
  const char *prefix = RedisModule_StringPtrLen(pf, &pflen);

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

int Redis_DropIndex(RedisSearchCtx *ctx, int deleteDocuments) {
  size_t len;

  if (deleteDocuments) {
    RedisModuleCallReply *r =
        RedisModule_Call(ctx->redisCtx, "HKEYS", "c", REDISINDEX_DOCKEY_MAP);
    if (r == NULL || RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR) {
      return REDISMODULE_ERR;
    }

    len = RedisModule_CallReplyLength(r);
    for (size_t i = 0; i < len; i++) {
      RedisModuleKey *k = RedisModule_OpenKey(
          ctx->redisCtx, RedisModule_CreateStringFromCallReply(
                             RedisModule_CallReplyArrayElement(r, i)),
          REDISMODULE_WRITE);

      if (k != NULL) {
        RedisModule_DeleteKey(k);
      }
      RedisModule_CloseKey(k);
    }

    RedisModuleString *dmd = RedisModule_CreateStringPrintf(
        ctx->redisCtx, DOCTABLE_KEY_FMT, ctx->spec->name);
    RedisModule_Call(ctx->redisCtx, "DEL", "cccs", REDISINDEX_DOCKEY_MAP,
                     REDISINDEX_DOCIDS_MAP, REDISINDEX_DOCIDCOUNTER, dmd);
  }

  RedisModuleString *pf = fmtRedisTermKey(ctx, "*", 1);
  const char *prefix = RedisModule_StringPtrLen(pf, &len);

  // Delete the actual index sub keys
  Redis_ScanKeys(ctx->redisCtx, prefix, Redis_DropScanHandler, ctx);
  return REDISMODULE_OK;
}