#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#include "index.h"
#include "tokenize.h"
#include "redis_index.h"
#include "util/logging.h"
#include "util/pqueue.h"


typedef enum {
    Q_INTERSECT,
    Q_UNION,
    Q_EXACT,
    Q_LOAD
} QueryOp;

typedef struct queryStage {
    const char *term;
    QueryOp op;
    
    struct queryStage **children;
    int nchildren;
} QueryStage;



typedef struct {
    const char *raw;
    size_t offset;
    size_t limit;
        
    QueryStage *root;
    RedisModuleCtx *ctx;
} Query;


typedef struct {
    size_t totalResults;
    RedisModuleString **ids;
    size_t numIds;
    
    
    int error;
    char *errorString;
} QueryResult;


IndexIterator *Query_EvalStage(RedisModuleCtx *ctx, QueryStage *s);

void QueryStage_Free(QueryStage *s) {
    for (int i = 0; i < s->nchildren; i++) {
        QueryStage_Free(s->children[i]);
    }
    
    free(s);
}

QueryStage *NewQueryStage(const char *term, QueryOp op) {
    QueryStage *s = malloc(sizeof(QueryStage));
    s->children = NULL;
    s->nchildren = 0;
    s->op = op;
    s->term = term;
    return s;
}

IndexIterator *evalLoadStage(RedisModuleCtx *ctx, QueryStage *stage) {
    IndexReader *ir = Redis_OpenReader(ctx, stage->term);
    if (ir == NULL) {
        return NULL;
    } 
    
    return NewIndexIterator(ir);
}

IndexIterator *evalIntersectStage(RedisModuleCtx *ctx, QueryStage *stage) {
    
    IndexIterator *iters[stage->nchildren];
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(ctx, stage->children[i]);
    }
    
    IndexIterator *ret = NewUnionIterator(iters, stage->nchildren);
    return ret;
}


IndexIterator *evalUnionStage(RedisModuleCtx *ctx, QueryStage *stage) {
    IndexIterator *iters[stage->nchildren];
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(ctx, stage->children[i]);
    }
    
    IndexIterator *ret = NewUnionIterator(iters, stage->nchildren);
    return ret;
}


IndexIterator *evalExactIntersectStage(RedisModuleCtx *ctx, QueryStage *stage) {
    IndexIterator *iters[stage->nchildren];
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(ctx, stage->children[i]);
    }
    
    IndexIterator *ret = NewUnionIterator(iters, stage->nchildren);
    return ret;
}

IndexIterator *Query_EvalStage(RedisModuleCtx *ctx, QueryStage *s) {
    
    switch (s->op) {
        case Q_LOAD:
            return evalLoadStage(ctx, s);
        case Q_INTERSECT:
            return evalIntersectStage(ctx, s);
        case Q_EXACT:
            return evalExactIntersectStage(ctx, s);
        case Q_UNION:
           return evalUnionStage(ctx, s);
    }
    
}

void QueryStage_AddChild(QueryStage *parent, QueryStage *child) {
    
    parent->nchildren++;
    parent->children = realloc(parent->children, sizeof(QueryStage*)*parent->nchildren);
    parent->children[parent->nchildren-1] = child;
    
}


int queryTokenFunc(void *ctx, Token t) {
    Query *q = ctx;
    
    QueryStage_AddChild(q->root, NewQueryStage(t.s, Q_LOAD));
    
}

Query *ParseQuery(const char *query, int offset, int limit) {
    Query *ret = calloc(1, sizeof(Query));
    
    ret->limit = limit;
    ret->offset = offset;
    ret->raw = strdup(query);
    ret->root = NewQueryStage(NULL, Q_INTERSECT);
    
    
    tokenize(ret->raw, 1, 1, ret, queryTokenFunc);
    
    return ret;
}



void Query_Free(Query *q) {
    QueryStage_Free(q->root);
    
    free(q);
}


#define QUERY_ERROR_INTERNAL_STR "Internal error processing query"
#define QUERY_ERROR_INTERNAL -1
u_int32_t getHitScore(void * ctx) {
    return ctx ? (u_int32_t)((IndexHit *)ctx)->freq : 0;
}

QueryResult *Query_Execute(RedisModuleCtx *ctx, Query *query) {
    
    QueryResult *res = malloc(sizeof(QueryResult));
    res->error = 0;
    res->errorString = NULL;
    res->totalResults = 0;
    res->ids = NULL;
    res->numIds = 0;
    
    
    PQUEUE pq; 
    PQueueInitialise(&pq, query->limit, 0, 0);
    
    //  start lazy evaluation of all query steps
    IndexIterator *it = NULL;
    if (query->root != NULL) {
        it = Query_EvalStage(ctx, query->root);
    }

    // no query evaluation plan?    
    if (query->root == NULL || it == NULL) {
        res->error = QUERY_ERROR_INTERNAL;
        res->errorString = QUERY_ERROR_INTERNAL_STR; 
        return res;
    }
    
    // iterate the root iterator and push everything to the PQ
    while (it->HasNext(it->ctx)) {
        // TODO - Use static allocation
        IndexHit *h = malloc(sizeof(IndexHit));
        if (it->Read(it->ctx, h) == INDEXREAD_EOF) {
            free(h);
            break;
        }
        
        ++res->totalResults;
        if (PQueueIsFull(&pq)) {
            void *popped = PQueuePop(&pq, getHitScore);
            if (popped != NULL) {
                free(popped);
            }
        }
        PQueuePush(&pq, h, getHitScore);
        
    }
    
    // Reverse the results into the final result
    size_t n = pq.CurrentSize;
    res->numIds = n;
    res->ids = calloc(n, sizeof(RedisModuleString*));
    
    for (int i = n-1; i >=0; --i) {
        IndexHit *h = PQueuePop(&pq, getHitScore);
        res->ids[i] = Redis_GetDocKey(ctx, h->docId);
        free(h);
    }

    PQueueFree(&pq);
    return res;
}

void QueryResult_Free(QueryResult *q) {
   
   free (q->ids);
   free(q);
}

