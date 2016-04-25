#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <time.h>
#include "index.h"
#include "varint.h"
#include "redismodule.h"
#include "forward_index.h"
#include "tokenize.h"
#include "redis_index.h"
#include "util/logging.h"
#include "util/pqueue.h"
#include "query.h"


typedef struct {
    const char *name;
    const char *text;
} DocumentField;

typedef struct {
    RedisModuleString *docKey;
    DocumentField *fields;
    int numFields;
    float score; 
} Document;

typedef struct {
    t_docId *docIds;
    size_t totalResults;
    t_offset limit;
} Result;

int AddDocument(RedisModuleCtx *ctx, Document doc) {
    
    int isnew;
    t_docId docId = Redis_GetDocId(ctx, doc.docKey, &isnew);
    if (docId == 0 || isnew == 0) {
        LG_ERROR("Not a new doc");
        return REDISMODULE_ERR;
    }
    
    ForwardIndex *idx = NewForwardIndex(docId,  doc.score);
    
    
    int totalTokens = 0;
    for (int i = 0; i < doc.numFields; i++) {
        LG_DEBUG("Tokenizing %s: %s\n", doc.fields[i].name, doc.fields[i].text );
        totalTokens += tokenize(doc.fields[i].text, 1, 1, idx, forwardIndexTokenFunc);        
    }
    
    LG_DEBUG("totaltokens :%d\n", totalTokens);
    if (totalTokens > 0) {
        ForwardIndexIterator it = ForwardIndex_Iterate(idx);
        
        ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
        
        while (entry != NULL) {
            LG_DEBUG("entry: %s freq %d\n", entry->term, entry->freq);
            IndexWriter *w = Redis_OpenWriter(ctx, entry->term);
            
            IW_WriteEntry(w, entry);
            
            Redis_CloseWriter(w);
            
            entry = ForwardIndexIterator_Next(&it); 
        }
        
        
    }
    
    //ForwardIndexFree(idx);
    return 0;
}


Result *Search(const char *query) {
    return NULL;
}

/*
FT.ADD <docId> <score> [<field> <text>, ....] 
*/
int AddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    // at least one field, and number of field/text args must be even
    if (argc < 5 || (argc - 3) % 2 == 1) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);
    
    Document doc;
    doc.docKey = argv[1];
    doc.score = 1.0;
    doc.numFields = (argc-3)/2;
    doc.fields = calloc(doc.numFields, sizeof(DocumentField));
    
    size_t len;
    for (int i = 3; i < argc; i+=2) {
        doc.fields[i-3].name = RedisModule_StringPtrLen(argv[i], &len);
        doc.fields[i-3].text = RedisModule_StringPtrLen(argv[i+1], &len);;
    }
    LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL), doc.numFields);
    int rc = AddDocument(ctx, doc);
    if (rc == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "Could not index document");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    
    return REDISMODULE_OK;
    
}


u_int32_t _getHitScore(void * ctx) {
    return ctx ? (u_int32_t)((IndexHit *)ctx)->freq : 0;
}
/** SEARCH <term> <term> */
int SearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    // at least one field, and number of field/text args must be even
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }
    
    RedisModule_AutoMemory(ctx);
    size_t len;
    const char *qs = RedisModule_StringPtrLen(argv[1], &len);
    Query *q = ParseQuery((char *)qs, len, 0, 10);
    
    QueryResult *r = Query_Execute(ctx, q);
    if (r == NULL) {
        RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
        return REDISMODULE_OK;
    }
    
    if (r->errorString != NULL) {
        RedisModule_ReplyWithError(ctx, r->errorString);
        goto cleanup;
    }
    
    RedisModule_ReplyWithArray(ctx, r->numIds+1);
    RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);
    
    for (int i = 0; i < r->numIds; i++) {
        RedisModule_ReplyWithString(ctx, r->ids[i]);
    }
    
cleanup:    
    QueryResult_Free(r);
    Query_Free(q);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    
    LOGGING_LEVEL = 0xFFFFFFFF;

    
    if (RedisModule_Init(ctx,"ft",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"ft.add",
        AddDocumentCommand) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
        
    if (RedisModule_CreateCommand(ctx,"ft.search",
        SearchCommand) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
        
//  if (RedisModule_CreateCommand(ctx,"hgetset",
//         HGetSetCommand) == REDISMODULE_ERR)
//         return REDISMODULE_ERR;

    return REDISMODULE_OK;
}