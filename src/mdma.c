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
#include "spec.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"




int AddDocument(RedisSearchCtx *ctx, Document doc, const char **errorString) {
    
    
    int isnew;
    t_docId docId = Redis_GetDocId(ctx, doc.docKey, &isnew);
    
    // Make sure the document is not already in the index - it needs to be incremental!
    if (docId == 0 || !isnew) {
        *errorString = "Document already in index";
        return REDISMODULE_ERR;
    }
    
    // first save the document as hash
    if (Redis_SaveDocument(ctx, &doc) != REDISMODULE_OK) {
        *errorString = "Could not save document data";
        return REDISMODULE_ERR;
    }
    
    DocTable dt;
    if (InitDocTable(ctx, &dt) == REDISMODULE_ERR)  return REDISMODULE_ERR;
    if (DocTable_PutDocument(&dt, docId, doc.score, 0) == REDISMODULE_ERR) {
        *errorString = "Could not save document metadata";
        return REDISMODULE_ERR;
    }
    
    ForwardIndex *idx = NewForwardIndex(docId,  doc.score);
    
    
    int totalTokens = 0;
    for (int i = 0; i < doc.numFields; i++) {
        //LG_DEBUG("Tokenizing %s: %s\n", doc.fields[i].name, doc.fields[i].text );
        size_t len;
        const char *c = RedisModule_StringPtrLen(doc.fields[i].text, &len);
        totalTokens += tokenize(c, 1, 1, idx, forwardIndexTokenFunc);        
    }
    
    LG_DEBUG("totaltokens :%d\n", totalTokens);
    if (totalTokens > 0) {
        ForwardIndexIterator it = ForwardIndex_Iterate(idx);
        
        ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
        
        while (entry != NULL) {
            LG_DEBUG("entry: %s freq %d\n", entry->term, entry->freq);
            ForwardIndex_NormalizeFreq(idx, entry);
            IndexWriter *w = Redis_OpenWriter(ctx, entry->term);
            
            IW_WriteEntry(w, entry);
            
            Redis_CloseWriter(w);
            
            entry = ForwardIndexIterator_Next(&it); 
        }
        
        
    }
    
    ForwardIndexFree(idx);
    return 0;
}


/*
FT.ADD <index> <docId> <score> [<field> <text>, ....] 
*/
int AddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    // at least one field, and number of field/text args must be even
    if (argc < 6 || (argc - 4) % 2 == 1) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);
    
    
    IndexSpec sp;
    // load the index by name
    if (IndexSpec_Load(ctx, &sp, RedisModule_StringPtrLen(argv[1], NULL)) != REDISMODULE_OK) {        
        RedisModule_ReplyWithError(ctx, "Index not defined or could not be loaded");
        return REDISMODULE_OK;
    }
    
    RedisSearchCtx sctx = {ctx, &sp};
  
    double ds = 0;
    if (RedisModule_StringToDouble(argv[3], &ds) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "Could not parse document score");
        return REDISMODULE_OK;
    }
    
    
    Document doc;
    doc.docKey = argv[2];
    doc.score = (float)ds;
    doc.numFields = (argc-4)/2;
    doc.fields = calloc(doc.numFields, sizeof(DocumentField));
    
    size_t len;
    int n = 0;
    for (int i = 4; i < argc; i+=2, n++) {
        doc.fields[n].name = argv[i];
        doc.fields[n].text = argv[i+1];
    }
    
    LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL), doc.numFields);
    const char *msg = NULL;
    int rc = AddDocument(&sctx, doc, &msg);
    if (rc == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, msg ? msg : "Could not index document");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    free(doc.fields);
    
    return REDISMODULE_OK;
    
}

u_int32_t _getHitScore(void * ctx) {
    return ctx ? (u_int32_t)((IndexHit *)ctx)->totalFreq : 0;
}

/** SEARCH <index> <query> [LIMIT first num] */
int SearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    // at least one field, and number of field/text args must be even
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    
    RedisModule_AutoMemory(ctx);
    
    DocTable dt;
    IndexSpec sp;
    sp.numFields = 0;
    sp.name = RedisModule_StringPtrLen(argv[1], NULL);
    
    long long first = 0, limit = 10;
    RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &first, &limit);
    if (limit <= 0) {
       return RedisModule_WrongArity(ctx);
    }
    
    // load the index by name
    if (IndexSpec_Load(ctx, &sp, RedisModule_StringPtrLen(argv[1], NULL)) !=
        REDISMODULE_OK) {
        
        RedisModule_ReplyWithError(ctx, "Index not defined or could not be loaded");
        return REDISMODULE_OK;
    }
    
    RedisSearchCtx sctx = {ctx, &sp};
    
    // open the documents metadata table
    InitDocTable(&sctx, &dt);
    
    size_t len;
    const char *qs = RedisModule_StringPtrLen(argv[2], &len);
    Query *q = ParseQuery(&sctx, (char *)qs, len, first, limit);
    q->docTable = &dt;
        
    // Execute the query 
    QueryResult *r = Query_Execute(q);
    if (r == NULL) {
        RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
        return REDISMODULE_OK;
    }
    
    if (r->errorString != NULL) {
        RedisModule_ReplyWithError(ctx, r->errorString);
        goto cleanup;
    }
    
    
    int ndocs;
    Document *docs = Redis_LoadDocuments(&sctx, r->ids, r->numIds, &ndocs);
    // format response
    RedisModule_ReplyWithArray(ctx, 2*ndocs+1);
    RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);
    
    
    
    for (int i = 0; i < ndocs; i++) {
        Document doc = docs[i];
        RedisModule_ReplyWithString(ctx, doc.docKey);
        RedisModule_ReplyWithArray(ctx, doc.numFields*2);        
        for (int f = 0; f < doc.numFields; f++) {
            RedisModule_ReplyWithString(ctx, doc.fields[f].name);
            RedisModule_ReplyWithString(ctx, doc.fields[f].text);
        }
        
    }
    free(docs);
cleanup:    
    
    QueryResult_Free(r);
    Query_Free(q);
    return REDISMODULE_OK;
}

/* FT.CREATE <index name> <field> <weight>, ... */
int CreateIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
  
    // at least one field, and number of field/text args must be even
    if (argc < 4 || argc % 2==1) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);
        
    IndexSpec sp;
    if (IndexSpec_ParseRedisArgs(&sp, ctx, &argv[2], argc-2) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "Could not parse field specs");
        return REDISMODULE_OK;
    }
    
    size_t len;
    sp.name = RedisModule_StringPtrLen(argv[1], &len);
   
    if (IndexSpec_Save(ctx, &sp) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "Could not save index spec");
        return REDISMODULE_OK;
    }
    
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
    
}
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    
    //LOGGING_INIT(0xFFFFFFFF);
    
    if (RedisModule_Init(ctx,"ft",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"ft.add",
        AddDocumentCommand, "write deny-oom no-cluster", 1,1,1)
        == REDISMODULE_ERR)
        return REDISMODULE_ERR;
        
    if (RedisModule_CreateCommand(ctx,"ft.search", 
        SearchCommand,
        "readonly deny-oom no-cluster", 1,1,1)
         == REDISMODULE_ERR)
        return REDISMODULE_ERR;
   
   
   if (RedisModule_CreateCommand(ctx,"ft.create",
        CreateIndexCommand, "write no-cluster", 1,1,1)
        == REDISMODULE_ERR)
        return REDISMODULE_ERR;
        
//  if (RedisModule_CreateCommand(ctx,"hgetset",
//         HGetSetCommand) == REDISMODULE_ERR)
//         return REDISMODULE_ERR;

    return REDISMODULE_OK;
}