#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>

#include "index.h"
#include "tokenize.h"
#include "redis_index.h"
#include "util/logging.h"
#include "util/heap.h"
#include "query.h"


void QueryStage_Free(QueryStage *s) {
  for (int i = 0; i < s->nchildren; i++) {
    QueryStage_Free(s->children[i]);
  }
  if (s->children) {
    free(s->children);
  }
  if (s->term) {
      free(s->term);
  }
  free(s);
}

QueryStage *NewQueryStage(const char *term, QueryOp op) {
  QueryStage *s = malloc(sizeof(QueryStage));
  s->children = NULL;
  s->nchildren = 0;
  s->op = op;
  s->term = (char *)term;
  s->parent = NULL;
  return s;
}

IndexIterator *query_EvalLoadStage(Query *q, QueryStage *stage,
                                   int isSingleWordQuery) {
  // printf("Query tokens: %d\n, single word? %d\n", q->numTokens,
  // isSingleWordQuery);
  IndexReader *ir =
      Redis_OpenReader(q->ctx, stage->term, q->docTable, isSingleWordQuery);
  if (ir == NULL) {
    return NULL;
  }

  return NewIndexIterator(ir);
}

IndexIterator *query_EvalIntersectStage(Query *q, QueryStage *stage) {
  if (stage->nchildren == 1) {
    return Query_EvalStage(q, stage->children[0]);
  }
  IndexIterator **iters = calloc(stage->nchildren, sizeof(IndexIterator *));
  for (int i = 0; i < stage->nchildren; i++) {
    iters[i] = Query_EvalStage(q, stage->children[i]);
  }

  IndexIterator *ret =
      NewIntersecIterator(iters, stage->nchildren, 0, q->docTable);
  return ret;
}

IndexIterator *query_EvalUnionStage(Query *q, QueryStage *stage) {
  IndexIterator *iters[stage->nchildren];
  for (int i = 0; i < stage->nchildren; i++) {
    iters[i] = Query_EvalStage(q, stage->children[i]);
  }

  IndexIterator *ret = NewUnionIterator(iters, stage->nchildren, q->docTable);
  return ret;
}

IndexIterator *query_EvalExactIntersectStage(Query *q, QueryStage *stage) {
  if (stage->nchildren == 1) {
    return Query_EvalStage(q, stage->children[0]);
  }
  IndexIterator **iters = calloc(stage->nchildren, sizeof(IndexIterator *));
  for (int i = 0; i < stage->nchildren; i++) {
    iters[i] = Query_EvalStage(q, stage->children[i]);
  }

  IndexIterator *ret =
      NewIntersecIterator(iters, stage->nchildren, 1, q->docTable);
  return ret;
}

IndexIterator *Query_EvalStage(Query *q, QueryStage *s) {
  switch (s->op) {
    case Q_LOAD:
      return query_EvalLoadStage(q, s, q->numTokens == 1);
    case Q_INTERSECT:
      return query_EvalIntersectStage(q, s);
    case Q_EXACT:
      return query_EvalExactIntersectStage(q, s);
    case Q_UNION:
      return query_EvalUnionStage(q, s);
  }

  return NULL;
}

void QueryStage_AddChild(QueryStage *parent, QueryStage *child) {
  parent->children =
      realloc(parent->children, sizeof(QueryStage *) * (parent->nchildren + 1));
  parent->children[parent->nchildren++] = child;
  child->parent = parent;
}

int queryTokenFunc(void *ctx, Token t) {
  Query *q = ctx;
  q->numTokens++;
  QueryStage_AddChild(q->root, NewQueryStage(t.s, Q_LOAD));

  return 0;
}

Query *ParseQuery(RedisSearchCtx *ctx, const char *query, size_t len,
                  int offset, int limit) {
  Query *ret = calloc(1, sizeof(Query));
  ret->ctx = ctx;
  ret->len = len;
  ret->limit = limit;
  ret->offset = offset;
  ret->raw = strndup(query, len);
  ret->root = NewQueryStage(NULL, Q_INTERSECT);
  ret->numTokens = 0;

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
      printf("{%s", qs->term);
      break;
    case Q_INTERSECT:
      printf("INTERSECT {\n");
      break;
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
      case T_WORD:
        q->numTokens++;
        QueryStage_AddChild(current, NewQueryStage(qt.s, Q_LOAD));
        break;
      case T_QUOTE:
        if (current->op != Q_EXACT) {
          QueryStage *ns = NewQueryStage(qt.s, Q_EXACT);
          QueryStage_AddChild(current, ns);
          current = ns;
        } else {  // end of quote
          current = current->parent;
        }
        break;
      case T_END:
      default:
        break;
    }

    if (current == NULL) break;
  }

  __queryStage_Print(q->root, 0);

  return q->numTokens;
}

void Query_Free(Query *q) {
  QueryStage_Free(q->root);
  free(q->raw);
  free(q);
}

static int cmpHits(const void *e1, const void *e2, const void *udata) {
  const IndexHit *h1 = e1, *h2 = e2;

  if (h1->totalFreq < h2->totalFreq) {
    return -1;
  } else if (h1->totalFreq > h2->totalFreq) {
    return 1;
  }
  return 0;
}
/* Factor document score (and TBD - other factors) in the hit's score.
This is done only for the root iterator */
double processHitScore(IndexHit *h, DocTable *dt) {
  if (h->type == H_EXACT) {
    printf("EXACT HIT, not calculating MD\n");
  }
  // for exact hits we don't need to calculate minimal offset dist
  int md = h->type == H_EXACT ? 1 : VV_MinDistance(h->offsetVecs, h->numOffsetVecs);
  return (h->totalFreq) / pow((double)md, 2);
}

QueryResult *Query_Execute(Query *query) {
  QueryResult *res = malloc(sizeof(QueryResult));
  res->error = 0;
  res->errorString = NULL;
  res->totalResults = 0;
  res->ids = NULL;
  res->numIds = 0;

  heap_t *pq = malloc(heap_sizeof(query->limit));
  heap_init(pq, cmpHits, NULL, query->limit);

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
  // iterate the root iterator and push everything to the PQ
  while (1) {
    // TODO - Use static allocation
    if (pooledHit == NULL) {
      pooledHit = malloc(sizeof(IndexHit));
    }
    IndexHit *h = pooledHit;
    IndexHit_Init(h);

    if (it->Read(it->ctx, h) == INDEXREAD_EOF) {
      break;
    }

    h->totalFreq = processHitScore(h, query->docTable);

    ++res->totalResults;

    if (heap_count(pq) < heap_size(pq)) {
      heap_offerx(pq, h);
      pooledHit = NULL;
    } else {
      IndexHit *qh = heap_peek(pq);
      if (qh->totalFreq < h->totalFreq) {
        pooledHit = heap_poll(pq);
        heap_offerx(pq, h);
        // IndexHit_Terminate(pooledHit);
      } else {
        pooledHit = h;
        // IndexHit_Terminate(pooledHit);
      }
    }
  }

  if (pooledHit) {
    free(pooledHit);
  }

  it->Free(it);

  // Reverse the results into the final result
  size_t n = heap_count(pq);
  res->numIds = n;
  res->ids = calloc(n, sizeof(RedisModuleString *));

  for (int i = 0; i < n; ++i) {
    IndexHit *h = heap_poll(pq);
    // printf("Popping %d freq %f\n", h->docId, h->totalFreq);
    res->ids[i] = Redis_GetDocKey(query->ctx, h->docId);
    free(h);
  }

  heap_free(pq);
  return res;
}

void QueryResult_Free(QueryResult *q) {
  free(q->ids);
  free(q);
}
