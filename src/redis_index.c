#include <stdio.h>
#include "redis_index.h"
#include "util/logging.h"
#include "doc_table.h"

/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisSearchCtx *ctx, const char *term) {
  char *k = malloc(strlen(term) + 5);
  int len = sprintf(k, TERM_KEY_FORMAT, ctx->spec->name, term);
  RedisModuleString *ret = RedisModule_CreateString(ctx->redisCtx, k, len);
  free(k);
  return ret;
}


RedisModuleString *fmtRedisSkipIndexKey(RedisSearchCtx *ctx, const char *term) {
  char *k = malloc(strlen(term) + 5);
  int len = sprintf(k, SKIPINDEX_KEY_FORMAT, ctx->spec->name, term);
  RedisModuleString *ret = RedisModule_CreateString(ctx->redisCtx, k, len);
  free(k);
  return ret;
}
/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisSearchCtx *ctx, const char *term) {
  
  // Open the index writer
  BufferWriter bw = NewRedisWriter(ctx->redisCtx, fmtRedisTermKey(ctx, term));
  
  // Open the skip index writer
  BufferWriter sw = NewRedisWriter(ctx->redisCtx, fmtRedisSkipIndexKey(ctx, term));
  IndexWriter *w = NewIndexWriterBuf(bw, sw);

  return w;
}

void Redis_CloseWriter(IndexWriter *w) {
  IW_Close(w);
  RedisBufferFree(w->bw.buf);
}

SkipIndex *LoadRedisSkipIndex(RedisSearchCtx *ctx, const char *term) {
  Buffer *b = NewRedisBuffer(ctx->redisCtx, fmtRedisSkipIndexKey(ctx, term), BUFFER_READ);
  if (b && b->cap > sizeof(SkipEntry)) {
    
    SkipIndex *si = malloc(sizeof(SkipIndex));
    BufferRead(b, &si->len, sizeof(si->len));
    si->entries = (SkipEntry*)b->pos;
    
    RedisBufferFree(b);
    return si;
  }
  RedisBufferFree(b);
  return NULL;
}


IndexReader *Redis_OpenReader(RedisSearchCtx *ctx, const char *term, DocTable *dt) {
  Buffer *b = NewRedisBuffer(ctx->redisCtx, fmtRedisTermKey(ctx, term), BUFFER_READ);
  if (b == NULL) {  // not found
    return NULL;
  }
  SkipIndex *si = LoadRedisSkipIndex(ctx, term);
  printf("Loaded skip index %p\n", si);
  return NewIndexReaderBuf(b, si, dt);
}

void Redis_CloseReader(IndexReader *r) {
  // we don't call IR_Free because it frees the underlying memory right now

  RedisBufferFree(r->buf);

  if (r->skipIdx != NULL) {
    free(r->skipIdx);
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
  
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx->redisCtx, "HGET", "cs", REDISINDEX_DOCKEY_MAP, docKey);
  if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR)
    return 0;

  // not found - increment the global id counter and set in the map
  if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
    RedisModuleCallReply *increp =
        RedisModule_Call(ctx->redisCtx, "INCR", "c", REDISINDEX_DOCIDCOUNTER);
    if (rep == NULL) return 0;

    long long ll = RedisModule_CallReplyInteger(increp);
    RedisModuleString *ls = RedisModule_CreateStringFromLongLong(ctx->redisCtx, ll);
    
    // map docId => key
    RedisModule_Call(ctx->redisCtx, "HSET", "css", REDISINDEX_DOCIDS_MAP,
                     ls, docKey);
    // map key => docId                
    RedisModule_Call(ctx->redisCtx, "HSET", "css", REDISINDEX_DOCKEY_MAP,
                    docKey, ls);                     
    *isnew = 1;
    return (t_docId)ll;
  }

  // just convert the response to a number and return it
  long long id;

  if (RedisModule_StringToLongLong(RedisModule_CreateStringFromCallReply(rep),
                                   &id) == REDISMODULE_OK) {
    return (t_docId)id;
  }

  return 0;
}


RedisModuleString *Redis_GetDocKey(RedisSearchCtx *ctx, t_docId docId) {
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx->redisCtx, "HGET", "cs", REDISINDEX_DOCIDS_MAP,
                       RedisModule_CreateStringFromLongLong(ctx->redisCtx, docId));

  if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL ||
      RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR)
    return NULL;

  return RedisModule_CreateStringFromCallReply(rep);
}

/**
Open the doc table key. Return REDISMODULE_ERR if failed
*/
int InitDocTable(RedisSearchCtx *ctx, DocTable *t) {
  
  char buf[strlen(ctx->spec->name)+16];
  sprintf(buf, DOCTABLE_KEY_FMT, ctx->spec->name);
  
  RedisModuleKey *k = RedisModule_OpenKey(
      ctx->redisCtx, 
      RedisModule_CreateString(ctx->redisCtx, buf, strlen(buf)),
      REDISMODULE_READ|REDISMODULE_WRITE);

  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    return REDISMODULE_ERR;
  }

  t->ctx = ctx->redisCtx;
  t->key = k;
  return REDISMODULE_OK;
}

int DocTable_GetMetadata(DocTable *t, t_docId docId, DocumentMetadata *md) {

    char buf[32];
    snprintf(buf, 32, DOCTABLE_DOCID_KEY_FMT, docId);
    
    RedisModuleString *data = NULL;
    int rc = RedisModule_HashGet(t->key, REDISMODULE_HASH_CFIELDS, buf, &data, NULL);
    if (rc == REDISMODULE_ERR || data == NULL) {
      return REDISMODULE_ERR;
    }
    
    size_t len;
    const char *p = RedisModule_StringPtrLen(data, &len);
    int ret = REDISMODULE_ERR;
    if (len == sizeof(DocumentMetadata)) {
      memcpy(md, p, sizeof(DocumentMetadata));
      ret = REDISMODULE_OK;
    }
    LG_DEBUG("READ META: score %f\n", md->score);
    
    RedisModule_FreeString(t->ctx, data);
    return ret;
      
}

int DocTable_PutDocument(DocTable *t, t_docId docId, double score, u_short flags) {
    char buf[32];
    int sz = snprintf(buf, 32, DOCTABLE_DOCID_KEY_FMT, docId);
    
    DocumentMetadata md = {score, flags};
    
    RedisModuleString *data = RedisModule_CreateString(t->ctx, (char *)&md, sizeof(DocumentMetadata));
    
    LG_DEBUG("Writing META %s -> %p. doc score :%f\n", buf, data, score);
    int rc = RedisModule_HashSet(t->key, REDISMODULE_HASH_CFIELDS, 
                buf, 
                data, NULL);
    RedisModule_FreeString(t->ctx, data);
    return rc;
    
}