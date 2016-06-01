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
#include "query.h"
#include "spec.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "numeric_index.h"



int AddDocument(RedisSearchCtx *ctx, Document doc, const char **errorString, int nosave) {
    
    
    int isnew;
    t_docId docId = Redis_GetDocId(ctx, doc.docKey, &isnew);
    
    // Make sure the document is not already in the index - it needs to be incremental!
    if (docId == 0 || !isnew) {
        *errorString = "Document already in index";
        return REDISMODULE_ERR;
    }
    
    // first save the document as hash
    if (nosave == 0 && Redis_SaveDocument(ctx, &doc) != REDISMODULE_OK) {
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
        const char *f = RedisModule_StringPtrLen(doc.fields[i].name, &len);
        const char *c = RedisModule_StringPtrLen(doc.fields[i].text, NULL);
        
        FieldSpec *fs = IndexSpec_GetField(ctx->spec, f, len);
        if (fs == NULL) {
            LG_DEBUG("Skipping field %s not in index!", c);
            continue;
        }
        
        switch (fs->type) {
            case F_FULLTEXT:
                totalTokens += tokenize(c, fs->weight, fs->id, idx, forwardIndexTokenFunc, 1);
                break;
            case F_NUMERIC: {
                
                double score;
                
                if (RedisModule_StringToDouble(doc.fields[i].text, &score) == REDISMODULE_ERR) {
                    *errorString = "Could not parse numeric index value";
                    goto error;
                }
                
                NumericIndex *ni = NewNumericIndex(ctx, fs);
                if (NumerIndex_Add(ni, docId, score) == REDISMODULE_ERR) {
                    *errorString = "Could not save numeric index value";
                    goto error;
                }
                NumerIndex_Free(ni);
                break;
            }
                    
        }
        
                
    }
    
    LG_DEBUG("totaltokens :%d\n", totalTokens);
    if (totalTokens > 0) {
        ForwardIndexIterator it = ForwardIndex_Iterate(idx);
        
        ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
        
        while (entry != NULL) {
            LG_DEBUG("entry: %s freq %f\n", entry->term, entry->freq);
            ForwardIndex_NormalizeFreq(idx, entry);
            IndexWriter *w = Redis_OpenWriter(ctx, entry->term);
            
            IW_WriteEntry(w, entry);
            
            Redis_CloseWriter(w);
            
            entry = ForwardIndexIterator_Next(&it); 
        }
        
        
    }
    ForwardIndexFree(idx);
    return REDISMODULE_OK;
    
error:
    ForwardIndexFree(idx);
    
    return REDISMODULE_ERR;
}

/*
## FT.ADD <index> <docId> <score> [NOSAVE] FIELDS <field> <text> ....]
Add a documet to the index.

## Parameters:

    - index: The Fulltext index name. The index must be first created with FT.CREATE

    - docId: The document's id that will be returned from searches. Note that the same docId cannot be 
    added twice to the same index

    - score: The document's rank based on the user's ranking. This must be between 0.0 and 1.0. 
    If you don't have a score just set it to 1

    - NOSAVE: If set to true, we will not save the actual document in the index and only index it.
    
    - FIELDS: Following the FIELDS specifier, we are looking for pairs of <field> <text> to be indexed.
    Each field will be scored based on the index spec given in FT.CREATE. 
    Passing fields that are not in the index spec will make them be stored as part of the document, 
    or ignored if NOSAVE is set 
    
Returns OK on success, or an error if something went wrong. 
*/
int AddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    int nosave = RMUtil_ArgExists("nosave", argv, argc, 1);
    int fieldsIdx = RMUtil_ArgExists("fields", argv, argc, 1);
    
    //printf("argc: %d, fieldsIdx: %d, argc - fieldsIdx: %d, nosave: %d\n", argc, fieldsIdx, argc-fieldsIdx, nosave); 
    // nosave must be at place 4 and we must have at least 7 fields
    if (argc < 7 || fieldsIdx == 0 || (argc - fieldsIdx) % 2 == 0 || (nosave && nosave != 4)) {
        return RedisModule_WrongArity(ctx);
    }
    
    RedisModule_AutoMemory(ctx);
    
    
    IndexSpec sp;
    // load the index by name
    if (IndexSpec_Load(ctx, &sp, RedisModule_StringPtrLen(argv[1], NULL)) != REDISMODULE_OK) {        
        RedisModule_ReplyWithError(ctx, "Index not defined or could not be loaded");
        goto cleanup;
        
    }
    
    RedisSearchCtx sctx = {ctx, &sp};
    
    // Load the document score
    double ds = 0;
    if (RedisModule_StringToDouble(argv[3], &ds) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "Could not parse document score");
        goto cleanup;
    }
    if (ds > 1 || ds < 0) {
        RedisModule_ReplyWithError(ctx, "Document scores must be normalized between 0.0 ... 1.0");
        goto cleanup;
    }
       
    
    Document doc;
    doc.docKey = argv[2];
    doc.score = (float)ds;
    doc.numFields = (argc-fieldsIdx)/2;
    doc.fields = calloc(doc.numFields, sizeof(DocumentField));
    
    size_t len;
    int n = 0;
    for (int i = fieldsIdx + 1; i < argc - 1; i+=2, n++) {
        //printf ("indexing '%s' => '%s'\n", RedisModule_StringPtrLen(argv[i], NULL), RedisModule_StringPtrLen(argv[i+1], NULL));
        doc.fields[n].name = argv[i];
        doc.fields[n].text = argv[i+1];
    }
    
    LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL), doc.numFields);
    const char *msg = NULL;
    int rc = AddDocument(&sctx, doc, &msg, nosave);
    if (rc == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, msg ? msg : "Could not index document");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    free(doc.fields);

cleanup:    
    IndexSpec_Free(&sp);
    return REDISMODULE_OK;
    
}

u_int32_t _getHitScore(void * ctx) {
    return ctx ? (u_int32_t)((IndexHit *)ctx)->totalFreq : 0;
}

/* 
## FT.SEARCH <index> <query> [NOCONTENT] [LIMIT offset num] [INFIELDS <num> <field> ...]
Seach the index with a textual query, returning either documents or just ids.

### Parameters:
   - index: The Fulltext index name. The index must be first created with FT.CREATE
   
   - query: the text query to search. If it's more than a single word, put it in quotes.
   Basic syntax like quotes for exact matching is supported.
   
   - NOCONTENT: If it appears after the query, we only return the document ids and not 
   the content. This is useful if rediseach is only an index on an external document collection
   
   - LIMIT fist num: If the parameters appear after the query, we limit the results to 
   the offset and number of results given. The default is 0 10
   
   - INFIELDS num field1 field2 ...: If set, filter the results to ones appearing only in specific
   fields of the document, like title or url. num is the number of specified field arguments  

### Returns:

    An array reply, where the first element is the total number of results, and then pairs of
    document id, and a nested array of field/value, unless NOCONTENT was given   
*/
int SearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    
    // at least one field, and number of field/text args must be even
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    
    RedisModule_AutoMemory(ctx);
    
    int nocontent = RMUtil_ArgExists("nocontent", argv, argc, 3);
    
    DocTable dt;
   
    
    long long first = 0, limit = 10;
    RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &first, &limit);
    if (limit <= 0) {
       return RedisModule_WrongArity(ctx);
    }

    IndexSpec sp;
    sp.numFields = 0;
    sp.name = RedisModule_StringPtrLen(argv[1], NULL);
    
    // load the index by name
    if (IndexSpec_Load(ctx, &sp, RedisModule_StringPtrLen(argv[1], NULL)) !=
        REDISMODULE_OK) {
        
        RedisModule_ReplyWithError(ctx, "Index not defined or could not be loaded");
        return REDISMODULE_OK;
    }
    
     // if INFIELDS exists, parse the field mask    
    int inFieldsIdx = RMUtil_ArgExists("INFIELDS", argv, argc, 3);
    long long numFields = 0;
    u_char fieldMask = 0xff;
    if (inFieldsIdx > 0) {
        RMUtil_ParseArgs(argv, argc, inFieldsIdx+1, "l", &numFields);
        if (numFields > 0 && inFieldsIdx + 1 + numFields < argc) {
            fieldMask = IndexSpec_ParseFieldMask(&sp, &argv[inFieldsIdx+2], numFields);
        }
        LG_DEBUG("Parsed field mask: 0x%x\n", fieldMask);
    }
    
    
    RedisSearchCtx sctx = {ctx, &sp};
    
    NumericFilter *nf = NULL;
    int filterIdx = RMUtil_ArgExists("FILTER", argv,argc, 3);
    if (filterIdx > 0 && filterIdx + 4 <= argc) {
        nf = ParseNumericFilter(&sctx, &argv[filterIdx+1], 3);
        if (nf == NULL) {
            
            RedisModule_ReplyWithError(ctx, "Invalid numeric filter");
            goto end;
            
        }
    }
    
    
     // open the documents metadata table
    InitDocTable(&sctx, &dt);
    
    size_t len;
    const char *qs = RedisModule_StringPtrLen(argv[2], &len);
    Query *q = NewQuery(&sctx, (char *)qs, len, first, limit, fieldMask);
    Query_Tokenize(q);
    
    if (nf != NULL) {
        QueryStage_AddChild(q->root, NewNumericStage(nf));
    }
    q->docTable = &dt;

        
        
    // Execute the query 
    QueryResult *r = Query_Execute(q);
    if (r == NULL) {
        RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
        goto end;
    }
    
    if (r->errorString != NULL) {
        RedisModule_ReplyWithError(ctx, r->errorString);
        goto cleanup;
    }
    
    // NOCONTENT mode - just return the ids
    if (nocontent) {
        RedisModule_ReplyWithArray(ctx, r->numIds+1);
        RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);
        for (int i = 0; i < r->numIds; i++) {
            RedisModule_ReplyWithString(ctx, r->ids[i]);
        }
        
        goto cleanup;   
    }
    
    // With content mode - return and load the documents
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
        
        Document_Free(doc);
    }
    
    
    free(docs);

cleanup:    
    QueryResult_Free(r);
    Query_Free(q);
end:    
    IndexSpec_Free(&sp);
    return REDISMODULE_OK;
}

/* 
## FT.CREATE <index> <field> <weight>, ...

Creates an index with the given spec. The index name will be used in all the key names
so keep it short!

### Parameters:

    - index: the index name to create. If it exists the old spec will be overwritten
    
    - field / weight pairs: pairs of field name and relative weight in scoring. 
    The weight is a double, but does not need to be normalized.

### Returns:
    
    OK or an error    
*/
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
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");    
    }
    
    IndexSpec_Free(&sp);
    
    return REDISMODULE_OK;
    
}

/* FT.OPTIMIZE <index>
*  After the index is built (and doesn't need to be updated again withuot a complete rebuild)
*  we can optimize memory consumption by trimming all index buffers to their actual size.
*
*  Warning 1: This will delete score indexes for small words (n < 5000), so updating the index after
*  optimizing it might lead to screwed up results (TODO: rebuild score indexes if needed).
*  The simple solution to that is to call optimize again after adding documents to the index.
*
*  Warning 2: This blocks redis for a long time. Do not run it on production instances
*
*/
int OptimizeIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // at least one field, and number of field/text args must be even
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    
    RedisModule_AutoMemory(ctx);
    
    IndexSpec sp;
    // load the index by name
    if (IndexSpec_Load(ctx, &sp, RedisModule_StringPtrLen(argv[1], NULL)) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "Index not defined or could not be loaded");
    }
    
    RedisSearchCtx sctx = {ctx, &sp};
    RedisModuleString *pf = fmtRedisTermKey(&sctx, "*");
    size_t len;
    const char *prefix = RedisModule_StringPtrLen(pf, &len);
    
    //RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    int num = Redis_ScanKeys(ctx, prefix, Redis_OptimizeScanHandler, &sctx);
    return RedisModule_ReplyWithLongLong(ctx, num);
    
}


/*
* FT.DROP <index>
* Deletes all the keys associated with the index. 
* If no other data is on the redis instance, this is equivalent to FLUSHDB, apart from the fact
* that the index specification is not deleted.
*/
int DropIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // at least one field, and number of field/text args must be even
    if (argc != 2) {
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
    
    Redis_DropIndex(&sctx, 1);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
    
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    
  //  LOGGING_INIT(0xFFFFFFFF);
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
        
   if (RedisModule_CreateCommand(ctx,"ft.optimize",
        OptimizeIndexCommand, "write no-cluster", 1,1,1)
        == REDISMODULE_ERR)
        return REDISMODULE_ERR;
        
   if (RedisModule_CreateCommand(ctx,"ft.drop",
        DropIndexCommand, "write no-cluster", 1,1,1)
        == REDISMODULE_ERR)
        return REDISMODULE_ERR;
        
//  if (RedisModule_CreateCommand(ctx,"hgetset",
//         HGetSetCommand) == REDISMODULE_ERR)
//         return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
