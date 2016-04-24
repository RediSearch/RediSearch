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
    
    // char b[32], bb[32];
    
    // for (int i =0; i < 1000000; i++) {
    //     sprintf(b, "k%d", i);
    //     sprintf(bb, "v%d", i);
    //     RedisModule_Call(ctx, "HSET", "scc", argv[1], b, bb);
    // }
        
   
    // RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    // clock_t start = clock();
    
    // for (int i =0; i < 1000000; i++) {
    //     sprintf(b, "k%d", i);
    //     RedisModuleString *s = RedisModule_HashGet(key, RedisModule_CreateString(ctx, b, strlen(b)));    
    // }
    
    // clock_t end = clock();
    // printf ("1M hgets took %fsec\n", (float)(end - start) / (float)CLOCKS_PER_SEC * 1000.0f);
    
    IndexReader *ir = Redis_OpenReader(ctx, RedisModule_StringPtrLen(argv[1], NULL));
    if (ir == NULL) {
        RedisModule_ReplyWithError(ctx, "Could not open index");
        return REDISMODULE_OK;        
    } 
    
    IndexIterator *it = NewIndexIterator(ir);
    
    
    PQUEUE pq; 
    PQueueInitialise(&pq, 5, 0, 0);
    
    size_t totalResults = 0; 
    while (it->HasNext(it->ctx)) {
        IndexHit *h = malloc(sizeof(IndexHit));
        if (it->Read(it->ctx, h) == INDEXREAD_EOF) {
            free(h);
            break;
        }
        
        ++totalResults;
        PQueuePush(&pq, h, _getHitScore);
        
    }
    
    
    
    size_t n = pq.CurrentSize;
    IndexHit *result[n];
    
    for (int i = n-1; i >=0; --i) {
        result[i] = PQueuePop(&pq, _getHitScore);
    }

    PQueueFree(&pq);
    

    RedisModule_ReplyWithArray(ctx, 1+n);
    RedisModule_ReplyWithLongLong(ctx, (long long)totalResults);
    char idbuf[16];
    for (int i = 0; i < n; i++) {
        IndexHit *h = result[i];
        
        if (h != NULL) {
            RedisModuleString *s = Redis_GetDocKey(ctx, h->docId);
            
            if (s != NULL){
                RedisModule_ReplyWithString(ctx, s);
            }  else {
                RedisModule_ReplyWithNull(ctx);
            }
            free(h);
        }
         else {
                RedisModule_ReplyWithNull(ctx);
            }
    }
    
    
    
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