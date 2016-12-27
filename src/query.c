#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

#include "expander.h"
#include "index.h"
#include "query.h"
#include "query_parser/parser.h"
#include "redis_index.h"
#include "tokenize.h"
#include "util/heap.h"
#include "util/logging.h"

void __queryNode_Print(QueryNode *qs, int depth);

void _queryTokenNode_Free(QueryTokenNode *tn) {
  free(tn->str);
  if (tn->metadata) {
    free(tn->metadata);
  }
}

void _queryPhraseNode_Free(QueryPhraseNode *pn) {
  for (int i = 0; i < pn->numChildren; i++) {
    QueryNode_Free(pn->children[i]);
  }
  if (pn->children) {
    free(pn->children);
  }
}

void _queryUnionNode_Free(QueryUnionNode *pn) {
  for (int i = 0; i < pn->numChildren; i++) {
    QueryNode_Free(pn->children[i]);
  }
  if (pn->children) {
    free(pn->children);
  }
}

// void _queryNumericNode_Free(QueryNumericNode *nn) { free(nn->nf); }

void QueryNode_Free(QueryNode *n) {

  switch (n->type) {
    case QN_TOKEN:
      _queryTokenNode_Free(&n->tn);
      break;
    case QN_PHRASE:
      _queryPhraseNode_Free(&n->pn);
      break;
    case QN_UNION:
      _queryUnionNode_Free(&n->un);
      break;
    case QN_NUMERIC:
      break;  //_queryNumericNode_Free(&n->nn);
  }
  free(n);
}

QueryNode *__newQueryNode(QueryNodeType type) {
  QueryNode *s = calloc(1, sizeof(QueryNode));
  s->type = type;
  return s;
}

QueryNode *NewTokenNodeMetadata(Query *q, const char *s, size_t len, void *metadata) {
  // If we are using stemming, stem the current token, and if needed add a
  // UNION of it an the stem
  q->numTokens++;

  QueryNode *ret = __newQueryNode(QN_TOKEN);

  ret->tn = (QueryTokenNode){.str = (char *)s, .len = len, .metadata = metadata};
  return ret;
}

QueryNode *NewTokenNode(Query *q, const char *s, size_t len) {
  return NewTokenNodeMetadata(q, s, len, NULL);
}

QueryNode *NewUnionNode() {
  QueryNode *ret = __newQueryNode(QN_UNION);
  ret->un = (QueryUnionNode){.children = NULL, .numChildren = 0};
  return ret;
}

QueryNode *NewPhraseNode(int exact) {
  QueryNode *ret = __newQueryNode(QN_PHRASE);
  ret->pn = (QueryPhraseNode){.children = NULL, .numChildren = 0, .exact = exact};
  return ret;
}

QueryNode *NewNumericNode(NumericFilter *flt) {
  QueryNode *ret = __newQueryNode(QN_NUMERIC);
  ret->nn = (QueryNumericNode){.nf = flt};
  return ret;
}

IndexIterator *query_EvalTokenNode(Query *q, QueryTokenNode *node) {
  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index

  int isSingleWord =
      q->numTokens == 1 && q->fieldMask == 0xff && q->offset + q->limit <= MAX_SCOREINDEX_SIZE;

  IndexReader *ir =
      Redis_OpenReader(q->ctx, node->str, node->len, q->docTable, isSingleWord, q->fieldMask);

  if (ir == NULL) {
    return NULL;
  }
  ir->term->metadata = node->metadata;
  return NewReadIterator(ir);
}

IndexIterator *query_EvalPhraseNode(Query *q, QueryPhraseNode *node) {
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (node->numChildren == 1) {
    return Query_EvalNode(q, node->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  for (int i = 0; i < node->numChildren; i++) {
    iters[i] = Query_EvalNode(q, node->children[i]);
  }

  IndexIterator *ret =
      NewIntersecIterator(iters, node->numChildren, node->exact, q->docTable, q->fieldMask);
  return ret;
}

IndexIterator *query_EvalNumericNode(Query *q, QueryNumericNode *node) {
  return NewNumericFilterIterator(node->nf);
}

IndexIterator *query_EvalUnionNode(Query *q, QueryUnionNode *node) {
  // a union stage with one child is the same as the child, so we just return it
  if (node->numChildren == 1) {
    return Query_EvalNode(q, node->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  for (int i = 0; i < node->numChildren; i++) {
    iters[i] = Query_EvalNode(q, node->children[i]);
  }

  IndexIterator *ret = NewUnionIterator(iters, node->numChildren, q->docTable);
  return ret;
}

IndexIterator *Query_EvalNode(Query *q, QueryNode *n) {
  switch (n->type) {
    case QN_TOKEN:
      return query_EvalTokenNode(q, &n->tn);
    case QN_PHRASE:
      return query_EvalPhraseNode(q, &n->pn);
    case QN_UNION:
      return query_EvalUnionNode(q, &n->un);
    case QN_NUMERIC:
      return query_EvalNumericNode(q, &n->nn);
  }

  return NULL;
}

void QueryPhraseNode_AddChild(QueryPhraseNode *parent, QueryNode *child) {
  parent->children = realloc(parent->children, sizeof(QueryNode *) * (parent->numChildren + 1));
  parent->children[parent->numChildren++] = child;
}
void QueryUnionNode_AddChild(QueryUnionNode *parent, QueryNode *child) {
  parent->children = realloc(parent->children, sizeof(QueryNode *) * (parent->numChildren + 1));
  parent->children[parent->numChildren++] = child;
}

QueryNode *StemmerExpand(void *ctx, Query *q, QueryNode *n);

Query *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset, int limit,
                u_char fieldMask, int verbatim, const char *lang, const char **stopwords,
                const char *expander) {
  Query *ret = calloc(1, sizeof(Query));
  ret->ctx = ctx;
  ret->len = len;
  ret->limit = limit;
  ret->fieldMask = fieldMask;
  ret->offset = offset;
  ret->raw = strndup(query, len);
  ret->root = NewPhraseNode(0);
  ret->numTokens = 0;
  ret->stopwords = stopwords;
  ret->expander = verbatim ? NULL : expander ? GetQueryExpander(expander) : NULL;
  ret->language = lang ? lang : DEFAULT_LANGUAGE;

  return ret;
}
QueryNode *__queryNode_Expand(Query *q, QueryExpander *e, QueryNode *n) {
  QueryNode *xn = e->Expand(e->ctx, q, n);
  if (xn) {
    // printf("expanded node %p!\n", xn);
    //__queryNode_Print(xn, 0);
    return xn;
  }

  if (n->type == QN_PHRASE) {
    for (int i = 0; i < n->pn.numChildren; i++) {
      n->pn.children[i] = __queryNode_Expand(q, e, n->pn.children[i]);
    }
  } else if (n->type == QN_UNION) {
    for (int i = 0; i < n->pn.numChildren; i++) {
      n->pn.children[i] = __queryNode_Expand(q, e, n->pn.children[i]);
    }
  }
  return n;
}

void Query_Expand(Query *q) {
  if (q->expander) {
    q->root = __queryNode_Expand(q, q->expander, q->root);
  }
}

void __queryNode_Print(QueryNode *qs, int depth) {
  for (int i = 0; i < depth; i++) {
    printf("  ");
  }
  switch (qs->type) {
    case QN_PHRASE:
      printf("%s {\n", qs->pn.exact ? "EXACT" : "PHRASE");
      for (int i = 0; i < qs->pn.numChildren; i++) {
        __queryNode_Print(qs->pn.children[i], depth + 1);
      }

      break;
    case QN_TOKEN:
      printf("{%s", (char *)qs->tn.str);
      break;

    case QN_NUMERIC: {
      NumericFilter *f = qs->nn.nf;
      printf("NUMERIC {%f %s x %s %f", f->min, f->inclusiveMin ? "<=" : "<",
             f->inclusiveMax ? "<=" : "<", f->max);
    } break;
    case QN_UNION:
      printf("UNION {\n");
      for (int i = 0; i < qs->un.numChildren; i++) {
        __queryNode_Print(qs->un.children[i], depth + 1);
      }
      break;
  }

  printf("}\n");
}

void Query_Free(Query *q) {
  if (q->root) {
    QueryNode_Free(q->root);
  }

  // if (q->stemmer) {
  //   q->stemmer->Free(q->stemmer);
  // }
  if (q->expander && q->expander->ctx) {
    q->expander->Free(q->expander->ctx);
  }
  free(q->raw);
  free(q);
}

/* Compare hits for sorting in the heap during traversal of the top N */
static int cmpHits(const void *e1, const void *e2, const void *udata) {
  const IndexResult *h1 = e1, *h2 = e2;

  if (h1->totalTF < h2->totalTF) {
    return 1;
  } else if (h1->totalTF > h2->totalTF) {
    return -1;
  }
  return h1->docId - h2->docId;
}

/* Factor document score (and TBD - other factors) in the hit's score.
This is done only for the root iterator */
double CalculateResultScore(IndexResult *h) {
  // IndexResult_Print(h);
  if (h->numRecords == 1) {
    return h->totalTF;
  }

  double tfidf = 0;
  for (int i = 0; i < h->numRecords; i++) {
    tfidf += h->records[i].tf * h->records[i].term->idf;
  }

  int md = IndexResult_MinOffsetDelta(h);
  return tfidf / (double)(md);
}

QueryResult *Query_Execute(Query *query) {
  //__queryNode_Print(query->root, 0);
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
    it = Query_EvalNode(query, query->root);
  }

  // no query evaluation plan?
  if (query->root == NULL || it == NULL) {
    return res;
  }

  IndexResult *pooledHit = NULL;
  double minScore = 0;
  // iterate the root iterator and push everything to the PQ
  while (1) {
    // TODO - Use static allocation
    if (pooledHit == NULL) {
      pooledHit = malloc(sizeof(IndexResult));
      *pooledHit = NewIndexResult();
    }
    IndexResult *h = pooledHit;
    IndexResult_Init(h);
    int rc = it->Read(it->ctx, h);

    if (rc == INDEXREAD_EOF) {
      break;
    } else if (rc == INDEXREAD_NOTFOUND) {
      continue;
    }
    // IndexResult_Print(h);
    h->totalTF = CalculateResultScore(h);

    if (heap_count(pq) < heap_size(pq)) {
      heap_offerx(pq, h);
      pooledHit = NULL;
      if (heap_count(pq) == heap_size(pq)) {
        IndexResult *minh = heap_peek(pq);
        minScore = minh->totalTF;
      }
    } else {
      if (h->totalTF >= minScore) {
        pooledHit = heap_poll(pq);
        heap_offerx(pq, h);

        // get the new min score
        IndexResult *minh = heap_peek(pq);
        minScore = minh->totalTF;
      } else {
        pooledHit = h;
      }
    }
  }

  if (pooledHit) {
    IndexResult_Free(pooledHit);
    free(pooledHit);
    pooledHit = NULL;
  }
  res->totalResults = it->Len(it->ctx);
  it->Free(it);

  // Reverse the results into the final result
  size_t n = MIN(heap_count(pq), query->limit);
  res->numResults = n;
  res->results = calloc(n, sizeof(ResultEntry));

  for (int i = 0; i < n; ++i) {
    IndexResult *h = heap_poll(pq);
    // LG_DEBUG("Popping %d freq %f\n", h->docId, h->totalFreq);
    res->results[n - i - 1] = (ResultEntry){Redis_GetDocKey(query->ctx, h->docId), h->totalTF};

    free(h);
  }

  // if we still have something in the heap (meaning offset > 0), we need to poll...
  while (heap_count(pq) > 0) {
    IndexResult *h = heap_poll(pq);
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
