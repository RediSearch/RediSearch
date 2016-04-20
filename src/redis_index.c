#include "redis_index.h"
#include <stdio.h>

/**
* Format redis key for a term.
* TODO: Add index name to it
*/
RedisModuleString *fmtRedisTermKey(RedisModuleCtx *ctx, const char *term) {
    char *k = malloc(strlen(term) + 5);
    int len = sprintf(k, TERM_KEY_FORMAT, term);
    RedisModuleString *ret = RedisModule_CreateString(ctx, k, len);
    free(k);
    return ret;
}

/**
* Open a redis index writer on a redis key
*/
IndexWriter *Redis_OpenWriter(RedisModuleCtx *ctx, const char *term) {
    
    BufferWriter br = NewRedisWriter(ctx, fmtRedisTermKey(ctx, term));
    IndexWriter *w = NewIndexWriterBuf(br);
    
    return w;
    
}


void Redis_CloseWriter(IndexWriter *w) {
    IW_Close(w);
    RedisBufferFree(w->bw.buf);
}

SkipIndex *LoadRedisSkipIndex(RedisModuleCtx *ctx, const char *term) {
    return NULL;   
}


IndexReader *Redis_OpenReader(RedisModuleCtx *ctx, const char *term) {
    
    Buffer *b = NewRedisBuffer(ctx, fmtRedisTermKey(ctx, term), BUFFER_READ);
    SkipIndex *si = LoadRedisSkipIndex(ctx, term);
    return NewIndexReaderBuf(b, si);
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
Get a numeric incrementing doc Id for indexing, from a string docId of the document.
We either fetch it from a map of doc key => id, or we increment the top id and write to that map
@return docId, or 0 on error, meaning we can't index the document

TODO: Detect if the id is numeric and don't convert it
*/
t_docId Redis_GetDocId(RedisModuleCtx *ctx, RedisModuleString *docKey, int *isnew) {
    
    *isnew = 0;
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "HGET", "cs", REDISINDEX_DOCIDS_MAP, docKey);
    if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR) return 0;
    
    // not found - increment the global id counter and set in the map
    if (RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_NULL) {
        
        RedisModuleCallReply *increp = RedisModule_Call(ctx, "INCR", "c", REDISINDEX_DOCIDCOUNTER);
        if (rep == NULL) return 0;
        
        long long ll = RedisModule_CallReplyInteger(increp);
        //RedisModule_Call(ctx, "HSET", "csl", REDISINDEX_DOCIDS_MAP, docKey, ll);
        *isnew = 1;
        return (t_docId)ll;
    }  
    
    // just convert the response to a number and return it
    long long id;
    
    if (RedisModule_StringToLongLong(RedisModule_CreateStringFromCallReply(rep), &id) == REDISMODULE_OK) {
        return (t_docId)id;
    }
    
    return 0;
   
}