#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#include "index.h"
#include "tokenize.h"
#include "redis_index.h"
#include "util/logging.h"
#include "util/pqueue.h"
#include "query.h"

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
    
    if (stage->nchildren == 1) {
        return Query_EvalStage(ctx, stage->children[0]);
    }
    IndexIterator **iters = calloc(stage->nchildren, sizeof(IndexIterator*));
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(ctx, stage->children[i]);
        if (iters[i] == NULL) {
            printf("Got NULL!\n");
        }
    }
    
    IndexIterator *ret = NewIntersecIterator(iters, stage->nchildren, 0);
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
    
    IndexIterator *ret = NewIntersecIterator(iters, stage->nchildren, 1);
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
    
    return NULL;
    
}

void QueryStage_AddChild(QueryStage *parent, QueryStage *child) {
    
    
    parent->children = realloc(parent->children, sizeof(QueryStage*)*(parent->nchildren+1));
    parent->children[parent->nchildren++] = child;
    
}


int queryTokenFunc(void *ctx, Token t) {
    Query *q = ctx;
    printf("Adding token %s\n", t.s);
    QueryStage_AddChild(q->root, NewQueryStage(t.s, Q_LOAD));

    return 0;
}

Query *ParseQuery(const char *query, size_t len, int offset, int limit) {
    Query *ret = calloc(1, sizeof(Query));
    
    ret->limit = limit;
    ret->offset = offset;
    ret->raw = strndup(query, len);
    ret->root = NewQueryStage(NULL, Q_INTERSECT);
    tokenize(ret->raw, 1, 1, ret, queryTokenFunc);
    
    return ret;
}



void Query_Free(Query *q) {
    QueryStage_Free(q->root);
    
    free(q);
}

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

