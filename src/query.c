#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>
#include <sys/param.h>

#include "index.h"
#include "tokenize.h"
#include "redis_index.h"
#include "util/logging.h"
#include "util/heap.h"
#include "query.h"

void QueryStage_Free(QueryStage *s) {
    // recursively free the child stages
    for (int i = 0; i < s->nchildren; i++) {
        QueryStage_Free(s->children[i]);
    }
    // free the children array
    if (s->children) {
        free(s->children);
    }
    // the term is strdupped, so needs to be freed
    if (s->value && s->valueFreeable) {
        free(s->value);
    }
    free(s);
}

QueryStage *__newQueryStage(void *value, QueryOp op, int freeable) {
    QueryStage *s = malloc(sizeof(QueryStage));
    s->children = NULL;
    s->nchildren = 0;
    s->op = op;
    s->value = value;
    s->valueFreeable = freeable;
    s->parent = NULL;
    return s;
}

QueryStage *NewTokenStage(Query *q, QueryToken *qt) {
    // If we are using stemming, stem the current token, and if needed add a
    // UNION of it an the stem
    q->numTokens++;
    if (q->stemmer) {
        size_t sl;
        const char *stemmed = q->stemmer->Stem(q->stemmer->ctx, qt->s, qt->len, &sl);

        if (stemmed && strncasecmp(stemmed, qt->s, qt->len)) {
            // we are now evaluating two tokens and not 1
            q->numTokens++;
            // Create a new union
            QueryStage *us = NewLogicStage(Q_UNION);

            // Add the token and the ste as the union's children
            QueryStage_AddChild(us, __newQueryStage((char *)qt->s, Q_LOAD, 1));
            QueryStage_AddChild(us, __newQueryStage(strndup(stemmed, sl), Q_LOAD, 1));
            return us;
        }
    }

    return __newQueryStage((char *)qt->s, Q_LOAD, 1);
}

QueryStage *NewLogicStage(QueryOp op) { return __newQueryStage(NULL, op, 0); }

QueryStage *NewNumericStage(NumericFilter *flt) { return __newQueryStage(flt, Q_NUMERIC, 0); }

IndexIterator *query_EvalLoadStage(Query *q, QueryStage *stage) {
    // if there's only one word in the query and no special field filtering,
    // and we are not paging beyond MAX_SCOREINDEX_SIZE 
    // we can just use the optimized score index
    
    int isSingleWord = q->numTokens == 1 && q->root->nchildren == 1 &&
            q->fieldMask == 0xff &&
            q->offset + q->limit <= MAX_SCOREINDEX_SIZE;
    //printf("singleword? %d, numTokens: %d, fields %x\n", isSingleWord, q->numTokens, q->fieldMask);

    IndexReader *ir = Redis_OpenReader(q->ctx, stage->value, strlen(stage->value), q->docTable,
                                       isSingleWord, q->fieldMask);
    if (ir == NULL) {
        return NULL;
    }

    return NewReadIterator(ir);
}

IndexIterator *query_EvalIntersectStage(Query *q, QueryStage *stage) {
    // an intersect stage with one child is the same as the child, so we just
    // return it
    if (stage->nchildren == 1) {
        return Query_EvalStage(q, stage->children[0]);
    }

    // recursively eval the children
    IndexIterator **iters = calloc(stage->nchildren, sizeof(IndexIterator *));
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(q, stage->children[i]);
    }

    IndexIterator *ret = NewIntersecIterator(iters, stage->nchildren, 0, q->docTable, q->fieldMask);
    return ret;
}

IndexIterator *query_EvalNumericStage(Query *q, QueryStage *stage) {
    NumericFilter *nf = stage->value;

    return NewNumericFilterIterator(nf);
}

IndexIterator *query_EvalUnionStage(Query *q, QueryStage *stage) {
    // a union stage with one child is the same as the child, so we just return it
    if (stage->nchildren == 1) {
        return Query_EvalStage(q, stage->children[0]);
    }

    // recursively eval the children
    IndexIterator **iters = calloc(stage->nchildren, sizeof(IndexIterator *));
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(q, stage->children[i]);
    }

    IndexIterator *ret = NewUnionIterator(iters, stage->nchildren, q->docTable);
    return ret;
}

IndexIterator *query_EvalExactIntersectStage(Query *q, QueryStage *stage) {
    // an intersect stage with one child is the same as the child, so we just
    // return it
    if (stage->nchildren == 1) {
        return Query_EvalStage(q, stage->children[0]);
    }
    IndexIterator **iters = calloc(stage->nchildren, sizeof(IndexIterator *));
    for (int i = 0; i < stage->nchildren; i++) {
        iters[i] = Query_EvalStage(q, stage->children[i]);
    }

    IndexIterator *ret = NewIntersecIterator(iters, stage->nchildren, 1, q->docTable, q->fieldMask);
    return ret;
}

IndexIterator *Query_EvalStage(Query *q, QueryStage *s) {
    switch (s->op) {
        case Q_LOAD:
            return query_EvalLoadStage(q, s);
        case Q_INTERSECT:
            return query_EvalIntersectStage(q, s);
        case Q_EXACT:
            return query_EvalExactIntersectStage(q, s);
        case Q_UNION:
            return query_EvalUnionStage(q, s);
        case Q_NUMERIC:
            return query_EvalNumericStage(q, s);
    }

    return NULL;
}

void QueryStage_AddChild(QueryStage *parent, QueryStage *child) {
    parent->children = realloc(parent->children, sizeof(QueryStage *) * (parent->nchildren + 1));
    parent->children[parent->nchildren++] = child;
    child->parent = parent;
}

Query *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset, int limit,
                u_char fieldMask, int verbatim, const char *lang) {
    Query *ret = calloc(1, sizeof(Query));
    ret->ctx = ctx;
    ret->len = len;
    ret->limit = limit;
    ret->fieldMask = fieldMask;
    ret->offset = offset;
    ret->raw = strndup(query, len);
    ret->root = __newQueryStage(NULL, Q_INTERSECT, 0);
    ret->numTokens = 0;
    ret->stemmer = NULL;
    if (!verbatim) {
        ret->stemmer = NewStemmer(SnowballStemmer, lang ? lang : DEFAULT_LANGUAGE);
    }

    return ret;
}

void __queryStage_Print(QueryStage *qs, int depth) {
    for (int i = 0; i < depth; i++) {
        printf("  ");
    }
    switch (qs->op) {
        case Q_EXACT:
            printf("EXACT {\n");
            break;
        case Q_LOAD:
            printf("{%s", (char *)qs->value);
            break;
        case Q_INTERSECT:
            printf("INTERSECT {\n");
            break;
        case Q_NUMERIC: {
            NumericFilter *f = qs->value;
            printf("NUMERIC {%f < x < %f", f->min, f->max);
        } break;
        case Q_UNION:
            printf("UNION {\n");
            break;
    }

    for (int i = 0; i < qs->nchildren; i++) {
        __queryStage_Print(qs->children[i], depth + 1);
    }

    if (qs->nchildren > 0) {
        for (int i = 0; i < depth; i++) {
            printf("  ");
        }
    }
    printf("}\n");
}

int Query_Tokenize(Query *q) {
    QueryTokenizer t = NewQueryTokenizer(q->raw, q->len);

    QueryStage *current = q->root;
    while (QueryTokenizer_HasNext(&t)) {
        QueryToken qt = QueryTokenizer_Next(&t);

        switch (qt.type) {
            case T_WORD: {
              
                QueryStage_AddChild(current, NewTokenStage(q, &qt));
                break;
            }
            case T_QUOTE:
                if (current->op != Q_EXACT) {
                    QueryStage *ns = NewLogicStage(Q_EXACT);
                    QueryStage_AddChild(current, ns);
                    current = ns;
                } else {  // end of quote
                    current = current->parent;
                }
                break;

            case T_STOPWORD:
            case T_END:
            default:
                break;
        }

        if (current == NULL) break;
    }

    //__queryStage_Print(q->root, 0);

    return q->numTokens;
}

void Query_Free(Query *q) {
    QueryStage_Free(q->root);
    if (q->stemmer) {
        q->stemmer->Free(q->stemmer);
    }
    free(q->raw);
    free(q);
}

/* Compare hits for sorting in the heap during traversal of the top N */
static int cmpHits(const void *e1, const void *e2, const void *udata) {
    const IndexHit *h1 = e1, *h2 = e2;

    if (h1->totalFreq < h2->totalFreq) {
        return 1;
    } else if (h1->totalFreq > h2->totalFreq) {
        return -1;
    }
    return h1->docId - h2->docId;
}
/* Factor document score (and TBD - other factors) in the hit's score.
This is done only for the root iterator */
double processHitScore(IndexHit *h, DocTable *dt) {
    // for exact hits we don't need to calculate minimal offset dist
    int md = h->type == H_EXACT ? 1 : VV_MinDistance(h->offsetVecs, h->numOffsetVecs);
    return (h->totalFreq) / pow((double)(md ? md : 1), 2);
}

QueryResult *Query_Execute(Query *query) {
    //__queryStage_Print(query->root, 0);
    QueryResult *res = malloc(sizeof(QueryResult));
    res->error = 0;
    res->errorString = NULL;
    res->totalResults = 0;
    res->results = NULL;
    res->numResults = 0;

    int num = query->offset + query->limit;
    heap_t *pq = malloc(heap_sizeof(num));
    heap_init(pq, cmpHits, NULL, num);

    //  start lazy evaluation of all query steps
    IndexIterator *it = NULL;
    if (query->root != NULL) {
        it = Query_EvalStage(query, query->root);
    }

    // no query evaluation plan?
    if (query->root == NULL || it == NULL) {
        res->error = QUERY_ERROR_INTERNAL;
        res->errorString = QUERY_ERROR_INTERNAL_STR;
        return res;
    }

    IndexHit *pooledHit = NULL;
    double minScore = 0;
    // iterate the root iterator and push everything to the PQ
    while (1) {
        // TODO - Use static allocation
        if (pooledHit == NULL) {
            pooledHit = malloc(sizeof(IndexHit));
        }
        IndexHit *h = pooledHit;
        IndexHit_Init(h);
        int rc = it->Read(it->ctx, h);

        if (rc == INDEXREAD_EOF) {
            break;
        } else if (rc == INDEXREAD_NOTFOUND) {
            continue;
        }

        h->totalFreq = processHitScore(h, query->docTable);

        if (heap_count(pq) < heap_size(pq)) {
            heap_offerx(pq, h);
            pooledHit = NULL;
            if (heap_count(pq) == heap_size(pq)) {
                IndexHit *minh = heap_peek(pq);
                minScore = minh->totalFreq;
            }
        } else {
            if (h->totalFreq >= minScore) {
                pooledHit = heap_poll(pq);
                heap_offerx(pq, h);

                // get the new min score
                IndexHit *minh = heap_peek(pq);
                minScore = minh->totalFreq;
            } else {
                pooledHit = h;
            }
        }
    }

    if (pooledHit) {
        free(pooledHit);
    }
    res->totalResults = it->Len(it->ctx);
    it->Free(it);

    // Reverse the results into the final result
    size_t n = MIN(heap_count(pq), query->limit);
    res->numResults = n;
    res->results = calloc(n, sizeof(ResultEntry));

    for (int i = 0; i < n; ++i) {
        IndexHit *h = heap_poll(pq);
        // LG_DEBUG("Popping %d freq %f\n", h->docId, h->totalFreq);
        res->results[n - i - 1] =
            (ResultEntry){Redis_GetDocKey(query->ctx, h->docId), h->totalFreq};

        free(h);
    }

    // if we still have something in the heap (meaning offset > 0), we need to
    // poll...
    while (heap_count(pq) > 0) {
        IndexHit *h = heap_poll(pq);
        free(h);
    }

    heap_free(pq);
    return res;
}

void QueryResult_Free(QueryResult *q) {
    free(q->results);
    free(q);
}

int __queryResult_serializeNoContent(QueryResult *r, RedisModuleCtx *ctx, int withscores) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);
    size_t arrlen = 1;

    for (int i = 0; i < r->numResults; i++) {
        ++arrlen;
        RedisModule_ReplyWithString(ctx, r->results[i].id);
        if (withscores) {
            ++arrlen;
            RedisModule_ReplyWithDouble(ctx, r->results[i].score);
        }
    }
    RedisModule_ReplySetArrayLength(ctx, arrlen);

    return REDISMODULE_OK;
}

int __queryResult_serializeFullResults(QueryResult *r, RedisSearchCtx *sctx, int withscores) {
    // With content mode - return and load the documents
    RedisModuleCtx *ctx = sctx->redisCtx;
    int ndocs;
    RedisModuleString *ids[r->numResults];
    for (int i = 0; i < r->numResults; i++) {
        ids[i] = r->results[i].id;
    }

    Document *docs = Redis_LoadDocuments(sctx, ids, r->numResults, &ndocs);
    // format response
    RedisModule_ReplyWithArray(ctx, (withscores ? 3 : 2) * ndocs + 1);

    RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);

    for (int i = 0; i < ndocs; i++) {
        Document doc = docs[i];
        // send the id
        RedisModule_ReplyWithString(ctx, doc.docKey);
        // if needed - send the score as well
        if (withscores) {
            RedisModule_ReplyWithDouble(ctx, r->results[i].score);
        }

        // serialize the fields
        RedisModule_ReplyWithArray(ctx, doc.numFields * 2);
        for (int f = 0; f < doc.numFields; f++) {
            RedisModule_ReplyWithString(ctx, doc.fields[f].name);
            RedisModule_ReplyWithString(ctx, doc.fields[f].text);
        }

        Document_Free(doc);
    }

    free(docs);
    return REDISMODULE_OK;
}

int QueryResult_Serialize(QueryResult *r, RedisSearchCtx *sctx, int nocontent, int withscores) {
    RedisModuleCtx *ctx = sctx->redisCtx;

    if (r->errorString != NULL) {
        return RedisModule_ReplyWithError(ctx, r->errorString);
    }

    // NOCONTENT mode - just return the ids
    if (nocontent) {
        return __queryResult_serializeNoContent(r, ctx, withscores);
    }

    return __queryResult_serializeFullResults(r, sctx, withscores);
}
