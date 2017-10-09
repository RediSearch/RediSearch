#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

#include "geo_index.h"
#include "index.h"
#include "query.h"
#include "query_parser/parser.h"
#include "redis_index.h"
#include "tokenize.h"
#include "util/minmax_heap.h"
#include "util/logging.h"
#include "extension.h"
#include "ext/default.h"
#include "rmutil/sds.h"
#include "concurrent_ctx.h"

#define MAX_PREFIX_EXPANSIONS 200

static void QueryTokenNode_Free(QueryTokenNode *tn) {
  if (tn->str) free(tn->str);
}

static void QueryPhraseNode_Free(QueryPhraseNode *pn) {
  for (int i = 0; i < pn->numChildren; i++) {
    QueryNode_Free(pn->children[i]);
  }
  if (pn->children && pn->numChildren) {
    free(pn->children);
    pn->children = NULL;
  }
}

static void QueryUnionNode_Free(QueryUnionNode *pn) {
  for (int i = 0; i < pn->numChildren; i++) {
    QueryNode_Free(pn->children[i]);
  }
  if (pn->children && pn->numChildren) {
    free(pn->children);
    pn->children = NULL;
  }
}

// void _queryNumericNode_Free(QueryNumericNode *nn) { free(nn->nf); }

void QueryNode_Free(QueryNode *n) {
  if (!n) return;
  switch (n->type) {
    case QN_TOKEN:
      QueryTokenNode_Free(&n->tn);
      break;
    case QN_PHRASE:
      QueryPhraseNode_Free(&n->pn);
      break;
    case QN_UNION:
      QueryUnionNode_Free(&n->un);
      break;
    case QN_NUMERIC:

      NumericFilter_Free(n->nn.nf);

      break;  //
    case QN_NOT:
      QueryNode_Free(n->not.child);
      break;
    case QN_OPTIONAL:
      QueryNode_Free(n->opt.child);
      break;
    case QN_PREFX:
      QueryTokenNode_Free(&n->pfx);
      break;
    case QN_GEO:
    case QN_WILDCARD:
    case QN_IDS:
      break;
  }
  free(n);
}

static QueryNode *NewQueryNode(QueryNodeType type) {
  QueryNode *s = calloc(1, sizeof(QueryNode));
  s->type = type;
  s->fieldMask = RS_FIELDMASK_ALL;
  return s;
}

QueryNode *NewTokenNodeExpanded(QueryParseCtx *q, const char *s, size_t len, RSTokenFlags flags) {
  QueryNode *ret = NewQueryNode(QN_TOKEN);
  q->numTokens++;

  ret->tn = (QueryTokenNode){.str = (char *)s, .len = len, .expanded = 1, .flags = flags};
  return ret;
}

QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len) {
  QueryNode *ret = NewQueryNode(QN_TOKEN);
  q->numTokens++;

  ret->tn = (QueryTokenNode){.str = (char *)s, .len = len, .expanded = 0, .flags = 0};
  return ret;
}

QueryNode *NewWildcardNode() {
  return NewQueryNode(QN_WILDCARD);
}

QueryNode *NewPrefixNode(QueryParseCtx *q, const char *s, size_t len) {
  QueryNode *ret = NewQueryNode(QN_PREFX);
  q->numTokens++;

  ret->pfx = (QueryPrefixNode){.str = (char *)s, .len = len, .expanded = 0, .flags = 0};
  return ret;
}

QueryNode *NewUnionNode() {
  QueryNode *ret = NewQueryNode(QN_UNION);
  ret->fieldMask = 0;
  ret->un = (QueryUnionNode){.children = NULL, .numChildren = 0};
  return ret;
}

QueryNode *NewPhraseNode(int exact) {
  QueryNode *ret = NewQueryNode(QN_PHRASE);
  ret->fieldMask = 0;

  ret->pn = (QueryPhraseNode){.children = NULL, .numChildren = 0, .exact = exact};
  return ret;
}

QueryNode *NewNotNode(QueryNode *n) {
  QueryNode *ret = NewQueryNode(QN_NOT);
  ret->not.child = n;
  return ret;
}

QueryNode *NewOptionalNode(QueryNode *n) {
  QueryNode *ret = NewQueryNode(QN_OPTIONAL);
  ret->not.child = n;
  return ret;
}

QueryNode *NewNumericNode(NumericFilter *flt) {
  QueryNode *ret = NewQueryNode(QN_NUMERIC);
  ret->nn = (QueryNumericNode){.nf = flt};

  return ret;
}

QueryNode *NewGeofilterNode(GeoFilter *flt) {
  QueryNode *ret = NewQueryNode(QN_GEO);
  ret->gn = (QueryGeofilterNode){.gf = flt};

  return ret;
}

static void Query_SetFilterNode(QueryParseCtx *q, QueryNode *n) {
  if (q->root == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (q->root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator.
    // TODO: do this in a smart manner
    QueryPhraseNode_AddChild(q->root, NULL);
    for (int i = q->root->pn.numChildren - 1; i > 0; --i) {
      q->root->pn.children[i] = q->root->pn.children[i - 1];
    }
    q->root->pn.children[0] = n;
    q->numTokens++;
  } else {  // for other types, we need to create a new phrase node
    QueryNode *nr = NewPhraseNode(0);
    QueryPhraseNode_AddChild(nr, n);
    QueryPhraseNode_AddChild(nr, q->root);
    q->numTokens++;
    q->root = nr;
  }
}

void Query_SetGeoFilter(QueryParseCtx *q, GeoFilter *gf) {
  Query_SetFilterNode(q, NewGeofilterNode(gf));
}

void Query_SetNumericFilter(QueryParseCtx *q, NumericFilter *nf) {

  Query_SetFilterNode(q, NewNumericNode(nf));
}

QueryNode *NewIdFilterNode(IdFilter *flt) {
  QueryNode *qn = NewQueryNode(QN_IDS);
  qn->fn.f = flt;
  return qn;
}

void Query_SetIdFilter(QueryParseCtx *q, IdFilter *f) {
  Query_SetFilterNode(q, NewIdFilterNode(f));
}

IndexIterator *Query_EvalTokenNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_TOKEN) {
    return NULL;
  }
  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index
  int isSingleWord = q->numTokens == 1 && q->fieldMask == RS_FIELDMASK_ALL;
  RSQueryTerm *term = NewQueryTerm(&qn->tn, q->tokenId++);
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, isSingleWord,
                                     q->fieldMask & qn->fieldMask, &q->conc);
  if (ir == NULL) {
    return NULL;
  }

  return NewReadIterator(ir);
}

/* Ealuate a prefix node by expanding all its possible matches and creating one big UNION on all of
 * them */
static IndexIterator *Query_EvalPrefixNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_PREFX) {
    return NULL;
  }
  // we allow a minimum of 2 letters in the prefx
  if (qn->pfx.len < 3) {
    return NULL;
  }
  Trie *terms = q->ctx->spec->terms;

  if (!terms) return NULL;

  TrieIterator *it = Trie_IteratePrefix(terms, qn->pfx.str, qn->pfx.len, 0);
  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"

  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist) &&
         itsSz < MAX_PREFIX_EXPANSIONS) {

    // Create a token for the reader
    RSToken tok = (RSToken){
        .expanded = 0, .flags = 0, .len = 0,
    };
    tok.str = runesToStr(rstr, slen, &tok.len);
    RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);

    // Open an index reader
    IndexReader *ir =
        Redis_OpenReader(q->ctx, term, q->docTable, 0, q->fieldMask & qn->fieldMask, &q->conc);

    free(tok.str);
    if (!ir) continue;

    // Add the reader to the iterator array
    its[itsSz++] = NewReadIterator(ir);
    if (itsSz == itsCap) {
      itsCap *= 2;
      its = realloc(its, itsCap * sizeof(*its));
    }
  }

  DFAFilter_Free(it->ctx);
  free(it->ctx);
  TrieIterator_Free(it);
  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    free(its);
    return NULL;
  }
  return NewUnionIterator(its, itsSz, q->docTable, 1);
}

static IndexIterator *Query_EvalPhraseNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_PHRASE) {
    return NULL;
  }
  QueryPhraseNode *node = &qn->pn;
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (node->numChildren == 1) {
    node->children[0]->fieldMask &= qn->fieldMask;
    return Query_EvalNode(q, node->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  for (int i = 0; i < node->numChildren; i++) {
    node->children[i]->fieldMask &= qn->fieldMask;
    iters[i] = Query_EvalNode(q, node->children[i]);
  }
  IndexIterator *ret;
  if (node->exact) {
    ret = NewIntersecIterator(iters, node->numChildren, q->docTable, q->fieldMask & qn->fieldMask,
                              0, 1);
  } else {
    ret = NewIntersecIterator(iters, node->numChildren, q->docTable, q->fieldMask & qn->fieldMask,
                              q->maxSlop, q->inOrder);
  }
  return ret;
}

static IndexIterator *Query_EvalWildcardNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_WILDCARD || !q->docTable) {
    return NULL;
  }

  return NewWildcardIterator(q->docTable->maxDocId);
}

static IndexIterator *Query_EvalNotNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_NOT) {
    return NULL;
  }
  QueryNotNode *node = &qn->not;

  return NewNotIterator(node->child ? Query_EvalNode(q, node->child) : NULL, q->docTable->maxDocId);
}

static IndexIterator *Query_EvalOptionalNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_OPTIONAL) {
    return NULL;
  }
  QueryOptionalNode *node = &qn->opt;

  return NewOptionalIterator(node->child ? Query_EvalNode(q, node->child) : NULL);
}

static IndexIterator *Query_EvalNumericNode(QueryParseCtx *q, QueryNumericNode *node) {

  FieldSpec *fs =
      IndexSpec_GetField(q->ctx->spec, node->nf->fieldName, strlen(node->nf->fieldName));
  if (!fs || fs->type != F_NUMERIC) {
    return NULL;
  }

  return NewNumericFilterIterator(q->ctx, node->nf, &q->conc);
}

static IndexIterator *Query_EvalGeofilterNode(QueryParseCtx *q, QueryGeofilterNode *node) {

  FieldSpec *fs = IndexSpec_GetField(q->ctx->spec, node->gf->property, strlen(node->gf->property));
  if (fs == NULL || fs->type != F_GEO) {
    return NULL;
  }

  GeoIndex gi = {.ctx = q->ctx, .sp = fs};
  return NewGeoRangeIterator(&gi, node->gf);
}

static IndexIterator *Query_EvalIdFilterNode(QueryParseCtx *q, QueryIdFilterNode *node) {

  return NewIdFilterIterator(node->f);
}

static IndexIterator *Query_EvalUnionNode(QueryParseCtx *q, QueryNode *qn) {
  if (qn->type != QN_UNION) {
    return NULL;
  }
  QueryUnionNode *node = &qn->un;

  // a union stage with one child is the same as the child, so we just return it
  if (node->numChildren == 1) {
    node->children[0]->fieldMask &= qn->fieldMask;
    return Query_EvalNode(q, node->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  int n = 0;
  for (int i = 0; i < node->numChildren; i++) {
    node->children[i]->fieldMask &= qn->fieldMask;
    IndexIterator *it = Query_EvalNode(q, node->children[i]);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    free(iters);
    return NULL;
  }

  IndexIterator *ret = NewUnionIterator(iters, n, q->docTable, 0);
  return ret;
}

IndexIterator *Query_EvalNode(QueryParseCtx *q, QueryNode *n) {
  switch (n->type) {
    case QN_TOKEN:
      return Query_EvalTokenNode(q, n);
    case QN_PHRASE:
      return Query_EvalPhraseNode(q, n);
    case QN_UNION:
      return Query_EvalUnionNode(q, n);
    case QN_NOT:
      return Query_EvalNotNode(q, n);
    case QN_PREFX:
      return Query_EvalPrefixNode(q, n);
    case QN_NUMERIC:
      return Query_EvalNumericNode(q, &n->nn);
    case QN_OPTIONAL:
      return Query_EvalOptionalNode(q, n);
    case QN_GEO:
      return Query_EvalGeofilterNode(q, &n->gn);
    case QN_IDS:
      return Query_EvalIdFilterNode(q, &n->fn);
    case QN_WILDCARD:
      return Query_EvalWildcardNode(q, n);
  }

  return NULL;
}

void QueryPhraseNode_AddChild(QueryNode *parent, QueryNode *child) {
  if (!child) return;
  QueryPhraseNode *pn = &parent->pn;
  // QueryNode_Print(NULL, parent, 0);
  // printf("parent mask %x, child mask %x\n", parent->fieldMask, child->fieldMask);
  if (child != NULL && (pn->numChildren == 0 || child->fieldMask != RS_FIELDMASK_ALL)) {
    parent->fieldMask |= child->fieldMask;
  }
  // printf("AFTER: parent mask %x, child mask %x\n", parent->fieldMask, child->fieldMask);

  // Child nodes inherit the field mask from their parent if they are
  if (child) {
    child->fieldMask &= parent->fieldMask;
  }

  pn->children = realloc(pn->children, sizeof(QueryNode *) * (pn->numChildren + 1));
  pn->children[pn->numChildren++] = child;
  // QueryNode_Print(NULL, parent, 0);
}

void QueryUnionNode_AddChild(QueryNode *parent, QueryNode *child) {
  if (!child) return;
  QueryUnionNode *un = &parent->un;
  if (child != NULL && (un->numChildren == 0 || child->fieldMask != RS_FIELDMASK_ALL)) {
    parent->fieldMask |= child->fieldMask;
  }
  if (child) {
    child->fieldMask &= parent->fieldMask;
  }
  un->children = realloc(un->children, sizeof(QueryNode *) * (un->numChildren + 1));
  un->children[un->numChildren++] = child;
}

QueryParseCtx *NewQueryParseCtx(RSSearchRequest *req) {

  QueryParseCtx *ctx = malloc(sizeof(*ctx));
  ctx->len = req->qlen;
  ctx->raw = strdup(req->rawQuery);
  ctx->numTokens = 0;
  ctx->ok = 1;
  ctx->root = NULL;
  ctx->spec = req->sctx->spec;
  ctx->stopwords =
      (ctx->spec && ctx->spec->stopwords) ? ctx->spec->stopwords : DefaultStopWordList();
  ctx->tokenId = 1;
  ctx->errorMsg = NULL;
  return ctx;
}

/* Set the concurrent mode of the query. By default it's on, setting here to 0 will turn it off,
 * resulting in the query not performing context switches */
// void Query_SetConcurrentMode(QueryExecutionCtx *q, int concurrent) {
//   q->concurrentMode = concurrent;
// }

// QueryParseCtx *NewQuery(RedisSearchCtx *ctx, const char *query, size_t len, int offset, int
// limit,
//                         t_fieldMask fieldMask, int verbatim, const char *lang,
//                         StopWordList *stopwords, const char *expander, int slop, int inOrder,
//                         const char *scorer, RSPayload payload, RSSortingKey *sk) {
//   QueryParseCtx *ret = calloc(1, sizeof(Query));
//   ret->ctx = ctx;
//   ret->len = len;
//   ret->limit = limit;
//   ret->maxSlop = slop;
//   ret->inOrder = inOrder;
//   ret->fieldMask = fieldMask;
//   ret->offset = offset;
//   ret->raw = strndup(query, len);
//   ret->root = NULL;
//   ret->numTokens = 0;
//   ret->tokenId = 1;
//   ret->stopwords = stopwords;
//   ret->payload = payload;
//   ret->sortKey = sk;
//   ret->aborted = 0;
//   ConcurrentSearchCtx_Init(ctx ? ctx->redisCtx : NULL, &ret->conc);

//   ret->concurrentMode = 1;

//   // ret->expander = verbatim ? NULL : expander ? GetQueryExpander(expander) : NULL;
//   ret->language = lang ? lang : DEFAULT_LANGUAGE;

//   /* Get the scorer - falling back to TF-IDF scoring if not found */
//   ret->scorer = NULL;
//   ret->scorerCtx.privdata = NULL;
//   ret->scorerCtx.payload = payload;
//   ret->scorerFree = NULL;
//   ExtScoringFunctionCtx *scx =
//       Extensions_GetScoringFunction(&ret->scorerCtx, scorer ? scorer : DEFAULT_SCORER_NAME);
//   if (!scx) {
//     scx = Extensions_GetScoringFunction(&ret->scorerCtx, DEFAULT_SCORER_NAME);
//   }
//   if (scx) {

//     ret->scorer = scx->sf;
//     ret->scorerFree = scx->ff;
//   }

//   /* Get the query expander */
//   ret->expCtx.query = ret;
//   ret->expCtx.language = ret->language;
//   ret->expander = NULL;
//   ret->expanderFree = NULL;
//   if (!verbatim) {
//     ExtQueryExpanderCtx *exp =
//         Extensions_GetQueryExpander(&ret->expCtx, expander ? expander : DEFAULT_EXPANDER_NAME);
//     if (exp) {
//       ret->expander = exp->exp;
//       ret->expCtx.privdata = exp->privdata;
//       ret->expanderFree = exp->ff;
//     }
//   }
//   return ret;
// }

// static void QueryNode_Expand(QueryParseCtx *q, QueryNode **pqn) {

//   QueryNode *qn = *pqn;
//   if (qn->type == QN_TOKEN) {
//     q->expCtx.currentNode = pqn;
//     q->expander(&q->expCtx, &qn->tn);

//   } else if (qn->type == QN_PHRASE) {
//     for (int i = 0; i < qn->pn.numChildren; i++) {
//       QueryNode_Expand(q, &qn->pn.children[i]);
//     }
//   } else if (qn->type == QN_UNION) {
//     for (int i = 0; i < qn->un.numChildren; i++) {
//       QueryNode_Expand(q, &qn->un.children[i]);
//     }
//   }
// }

// void Query_Expand(QueryParseCtx *q) {
//   if (q->expander && q->root) {
//     QueryNode_Expand(q, &q->root);
//   }
// }

static sds doPad(sds s, int len) {
  if (!len) return s;

  char buf[len * 2 + 1];
  memset(buf, ' ', len * 2);
  buf[len * 2] = 0;
  return sdscat(s, buf);
}

static sds QueryNode_DumpSds(sds s, QueryParseCtx *q, QueryNode *qs, int depth) {
  s = doPad(s, depth);

  if (qs->fieldMask == 0) {
    s = sdscat(s, "@NULL:");
  }

  if (qs->fieldMask && qs->fieldMask != RS_FIELDMASK_ALL && qs->type != QN_NUMERIC &&
      qs->type != QN_GEO && qs->type != QN_IDS) {
    if (!q || !q->spec) {
      s = sdscatprintf(s, "@%x", qs->fieldMask);
    } else {
      s = sdscat(s, "@");
      uint32_t fm = qs->fieldMask;
      int i = 0, n = 0;
      while (fm) {
        uint32_t bit = (fm & 1) << i;
        if (bit) {
          char *f = GetFieldNameByBit(q->spec, bit);
          s = sdscatprintf(s, "%s%s", n ? "|" : "", f ? f : "n/a");
          n++;
        }
        fm = fm >> 1;
        i++;
      }
    }
    s = sdscat(s, ":");
  }

  switch (qs->type) {
    case QN_PHRASE:
      s = sdscatprintf(s, "%s {\n", qs->pn.exact ? "EXACT" : "INTERSECT");
      for (int i = 0; i < qs->pn.numChildren; i++) {
        s = QueryNode_DumpSds(s, q, qs->pn.children[i], depth + 1);
      }
      s = doPad(s, depth);

      break;
    case QN_TOKEN:
      s = sdscatprintf(s, "%s%s\n", (char *)qs->tn.str, qs->tn.expanded ? "*" : "");
      return s;

    case QN_PREFX:
      s = sdscatprintf(s, "PREFIX{%s*", (char *)qs->pfx.str);
      break;

    case QN_NOT:
      s = sdscat(s, "NOT{\n");
      s = QueryNode_DumpSds(s, q, qs->not.child, depth + 1);
      s = doPad(s, depth);
      break;

    case QN_OPTIONAL:
      s = sdscat(s, "OPTIONAL{\n");
      s = QueryNode_DumpSds(s, q, qs->not.child, depth + 1);
      s = doPad(s, depth);
      break;

    case QN_NUMERIC: {
      NumericFilter *f = qs->nn.nf;
      s = sdscatprintf(s, "NUMERIC {%f %s @%s %s %f", f->min, f->inclusiveMin ? "<=" : "<",
                       f->fieldName, f->inclusiveMax ? "<=" : "<", f->max);
    } break;
    case QN_UNION:
      s = sdscat(s, "UNION {\n");
      for (int i = 0; i < qs->un.numChildren; i++) {
        s = QueryNode_DumpSds(s, q, qs->un.children[i], depth + 1);
      }
      s = doPad(s, depth);
      break;
    case QN_GEO:

      s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s", qs->gn.gf->property, qs->gn.gf->lon,
                       qs->gn.gf->lat, qs->gn.gf->radius, qs->gn.gf->unit);
      break;
    case QN_IDS:

      s = sdscat(s, "IDS { ");
      for (int i = 0; i < qs->fn.f->size; i++) {
        s = sdscatprintf(s, "%d,", qs->fn.f->ids[i]);
      }
      break;
    case QN_WILDCARD:

      s = sdscat(s, "<WILDCARD>");
      break;
  }

  s = sdscat(s, "}\n");
  return s;
}

/* Return a string representation of the query parse tree. The string should be freed by the caller
 */
const char *Query_DumpExplain(QueryParseCtx *q) {
  // empty query
  if (!q || !q->root) {
    return strdup("NULL");
  }

  sds s = QueryNode_DumpSds(sdsnew(""), q, q->root, 0);
  const char *ret = strndup(s, sdslen(s));
  sdsfree(s);
  return ret;
}

void QueryNode_Print(QueryParseCtx *q, QueryNode *qn, int depth) {
  sds s = QueryNode_DumpSds(sdsnew(""), q, qn, depth);
  printf("%s", s);
  sdsfree(s);
}

void Query_Free(QueryParseCtx *q) {
  if (q->root) {
    QueryNode_Free(q->root);
  }
  // // if we have a custom expander with a free function - call it now
  // // printf("expander free %p. privdata %p\n", q->expanderFree, q->expCtx.privdata);
  // if (q->expanderFree) {
  //   q->expanderFree(q->expCtx.privdata);
  // }

  // // we have a custom scorer with a free function - call it now
  // if (q->scorerFree) {
  //   q->scorerFree(q->scorerCtx.privdata);
  // }

  // ConcurrentSearchCtx_Free(&q->conc);
  // if (q->rootFilter) {
  //   q->rootFilter->Free(q->rootFilter);
  // }

  // if (q->rootProcessor) {
  //   ResultProcessor_Free(q->rootProcessor);
  // }
  free(q->raw);
  free(q);
}

/* Compare hits for sorting in the heap during traversal of the top N */
static inline int cmpHits(const void *e1, const void *e2, const void *udata) {
  const SearchResult *h1 = e1, *h2 = e2;

  if (h1->score < h2->score) {
    return -1;
  } else if (h1->score > h2->score) {
    return 1;
  }
  return h1->docId < h2->docId ? -1 : 1;
}

static int sortByCmp(const void *e1, const void *e2, const void *udata) {
  const RSSortingKey *sk = udata;
  const SearchResult *h1 = e1, *h2 = e2;
  if (!h1->sv || !h2->sv) {
    return h1->docId < h2->docId ? -1 : 1;
  }
  return -RSSortingVector_Cmp(h1->sv, h2->sv, (RSSortingKey *)sk);
}

/* A callback called when we regain concurrent execution context, and the index spec key is
 * reopened. We protect against the case that the spec has been deleted during query execution */
void Query_OnReopen(RedisModuleKey *k, void *privdata) {
  IndexSpec *sp = RedisModule_ModuleTypeGetValue(k);
  QueryExecutionCtx *q = privdata;
  // If we don't have a spec or key - we abort the query
  if (k == NULL || sp == NULL) {
    q->resultCtx.state = QueryState_Aborted;
    q->ctx->spec = NULL;
    return;
  }

  // The spec might have changed while we were sleeping - for example a realloc of the doc table
  q->ctx->spec = sp;
  // q->docTable = &sp->docs;
}

size_t Query_SerializeResult(QueryExecutionCtx *qex, SearchResult *r, RSSearchFlags flags) {
  size_t count = 1;

  RedisModuleCtx *ctx = qex->ctx->redisCtx;
  RedisModule_ReplyWithStringBuffer(ctx, r->md->key, strlen(r->md->key));

  if (flags & Search_WithScores) {
    printf("Result score: %g\n", r->score);
    RedisModule_ReplyWithDouble(ctx, r->score);
    count++;
  }

  if (flags & Search_WithPayloads) {
    ++count;
    const RSPayload *payload = r->md->payload;
    if (payload) {
      RedisModule_ReplyWithStringBuffer(ctx, payload->data, payload->len);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }

  if (flags & Search_WithSortKeys) {
    ++count;
    const RSSortableValue *sortkey = RSSortingVector_Get(r->sv, qex->sortKey);
    if (sortkey) {
      if (sortkey->type == RS_SORTABLE_NUM) {
        RedisModule_ReplyWithDouble(ctx, sortkey->num);
      } else {
        RedisModule_ReplyWithStringBuffer(ctx, sortkey->str, strlen(sortkey->str));
      }
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }

  if (!(flags & Search_NoContent)) {
    count++;
    RedisModule_ReplyWithArray(ctx, r->fields->len * 2);
    for (int i = 0; i < r->fields->len; i++) {
      RedisModule_ReplyWithStringBuffer(ctx, r->fields->fields[i].key,
                                        strlen(r->fields->fields[i].key));
      RSValue_SendReply(ctx, RSFieldMap_Item(r->fields, i));
    }
  }
  return count;
}

int Query_SerializeResults(QueryExecutionCtx *qex, RSSearchFlags flags) {
  int rc;
  int count = 0;
  RedisModuleCtx *ctx = qex->ctx->redisCtx;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  do {
    SearchResult r;
    rc = ResultProcessor_Next(qex->rootProcessor, &r, 1);
    if (rc == RS_RESULT_EOF) break;

    if (count == 0) {
      RedisModule_ReplyWithLongLong(ctx, ResultProcessor_Total(qex->rootProcessor));
      count++;
    }
    count += Query_SerializeResult(qex, &r, flags);
  } while (rc != RS_RESULT_EOF);
  if (count == 0) {
    RedisModule_ReplyWithLongLong(ctx, ResultProcessor_Total(qex->rootProcessor));
    count++;
  }
  RedisModule_ReplySetArrayLength(ctx, count);
  return REDISMODULE_OK;
}

int Query_Execute(QueryParseCtx *query) {
  query->rootFilter = Query_EvalNode(query, query->root);
  query->rootProcessor = Query_BuildProcessorChain(query, query->req);
  return Query_SerializeResults(query);

  //   ConcurrentSearch_AddKey(&query->conc, query->ctx->key, REDISMODULE_READ, query->ctx->keyName,
  //                           Query_OnReopen, query, NULL);

  //   // QueryNode_Print(query, query->root, 0);
  //   QueryResult *res = malloc(sizeof(QueryResult));
  //   res->error = 0;
  //   res->errorString = NULL;
  //   res->totalResults = 0;
  //   res->results = NULL;
  //   res->numResults = 0;

  //   // If 1, the query has SORTBY and is not score based
  //   int sortByMode = query->sortKey != NULL;

  //   //  start lazy evaluation of all query steps
  //   IndexIterator *it = NULL;
  //   if (query->root != NULL) {
  //     it = Query_EvalNode(query, query->root);
  //   }

  //   // no query evaluation plan?
  //   if (query->root == NULL || it == NULL) {
  //     return res;
  //   }

  //   int num = query->offset + query->limit;

  //   heap_t *pq = malloc(heap_sizeof(num));
  //   if (sortByMode) {
  //     heap_init(pq, sortByCmp, query->sortKey, num);
  //   } else {
  //     heap_init(pq, cmpHits, NULL, num);
  //   }

  //   heapResult *pooledHit = NULL;
  //   double minScore = 0;
  //   int numDeleted = 0;
  //   RSIndexResult *r = NULL;
  //   ConcurrentSearchCtx *cxc = query->concurrentMode ? &query->conc : NULL;

  //   // iterate the root iterator and push everything to the PQ
  //   while (1) {
  //     // TODO - Use static allocation
  //     if (pooledHit == NULL) {
  //       pooledHit = malloc(sizeof(heapResult));
  //     }
  //     heapResult *h = pooledHit;

  //     // Read the next result from the execution tree
  //     int rc = it->Read(it->ctx, &r);

  //     // This means we are done!
  //     if (rc == INDEXREAD_EOF) {
  //       break;
  //     } else if (!r || rc == INDEXREAD_NOTFOUND) {
  //       continue;
  //     }

  //     RSDocumentMetadata *dmd = DocTable_Get(&query->ctx->spec->docs, r->docId);

  //     // skip deleted documents
  //     if (!dmd || (dmd->flags & Document_Deleted)) {
  //       ++numDeleted;
  //       continue;
  //     }

  //     /* Call the query scoring function to calculate the score */
  //     if (sortByMode) {
  //       h->sv = dmd->sortVector;
  //       h->score = 0;
  //     } else {
  //       h->score = query->scorer(&query->scorerCtx, r, dmd, minScore);
  //       h->sv = NULL;
  //       // filter out 0 score results
  //       if (h->score == RS_SCORE_FILTEROUT) {
  //         ++numDeleted;
  //         continue;
  //       }
  //     }
  //     h->docId = r->docId;

  //     CONCURRENT_CTX_TICK(cxc);
  //     if (query->aborted) goto cleanup;

  //     if (heap_count(pq) < heap_size(pq)) {
  //       heap_offerx(pq, h);
  //       pooledHit = NULL;
  //       if (heap_count(pq) == heap_size(pq)) {
  //         heapResult *minh = heap_peek(pq);
  //         minScore = minh->score;
  //       }
  //     } else {
  //       /* In SORTBY mode - compare the hit with the lowest ranked entry in the heap */
  //       if (sortByMode) {
  //         heapResult *minh = heap_peek(pq);

  //         /* if the current hit should be in the heap - remoe the lowest hit and add the new hit
  //         */
  //         if (sortByCmp(h, minh, query->sortKey) < 0) {
  //           pooledHit = heap_poll(pq);
  //           heap_offerx(pq, h);
  //         } else {
  //           /* The current should not enter the pool, so just leave it as is */
  //           pooledHit = h;
  //         }

  //       } else {
  //         /* In Scored mode - compare scores with the lowest ranked result */
  //         if (h->score < minScore) {
  //           pooledHit = h;
  //         } else {
  //           /* if the new result has a larger score, or has the same score
  //            * but a larger id (we sort by score then id), we add it to the heap */
  //           if (h->score > minScore || cmpHits(h, heap_peek(pq), NULL) < 0) {

  //             pooledHit = heap_poll(pq);
  //             heap_offerx(pq, h);

  //             // get the new min score
  //             minScore = ((heapResult *)heap_peek(pq))->score;
  //           } else {
  //             pooledHit = h;
  //           }
  //         }
  //       }
  //     }
  //   }

  //   //  IndexResult_Free(r);
  //   if (pooledHit) {
  //     // IndexResult_Free(pooledHit);
  //     free(pooledHit);
  //     pooledHit = NULL;
  //   }
  //   res->totalResults = it->Len(it->ctx) - numDeleted;
  //   it->Free(it);

  //   // if not enough results - just return nothing now
  //   if (heap_count(pq) <= query->offset) {
  //     res->numResults = 0;
  //     res->results = NULL;
  //     goto cleanup;
  //   }

  //   // Reverse the results into the final result

  //   // first - calculate the number of results in the heap matching our paging
  //   size_t n = MIN(heap_count(pq) - query->offset, query->limit);
  //   res->numResults = n;
  //   res->results = calloc(n, sizeof(ResultEntry));

  //   // pop from the end of the heap the lowest n results in reverse order
  //   for (int i = 0; i < n; ++i) {
  //     heapResult *h = heap_poll(pq);
  //     // LG_DEBUG("Popping %d freq %f\n", h->docId, h->totalFreq);
  //     RSDocumentMetadata *dmd = DocTable_Get(&query->ctx->spec->docs, h->docId);
  //     RSSortableValue *sv = NULL;
  //     if (dmd) {
  //       // For sort key based queries, the score is the inverse of the rank
  //       if (sortByMode) {
  //         h->score = (double)i + 1;
  //         if (h->sv) {
  //           sv = RSSortingVector_Get(h->sv, query->sortKey);
  //         }
  //       }
  //       res->results[n - i - 1] =
  //           (ResultEntry){.id = dmd->key, .score = h->score, .payload = dmd->payload, .sortKey =
  //           sv};
  //     }
  //     free(h);
  //   }

  // cleanup:
  //   // if we still have something in the heap (meaning offset > 0), we need to poll...
  //   while (heap_count(pq) > 0) {
  //     heapResult *h = heap_poll(pq);
  //     free(h);
  //   }

  //   heap_free(pq);
  //   return res;
}

ResultProcessor *NewResultProcessor(ResultProcessor *upstream, void *privdata) {
  ResultProcessor *p = calloc(1, sizeof(ResultProcessor));
  p->ctx = (ResultProcessorCtx){
      .privdata = privdata, .upstream = upstream, .resCtx = upstream ? upstream->ctx.resCtx : NULL,
  };

  return p;
}

static inline int ResultProcessor_Next(ResultProcessor *rp, SearchResult *res, int allowSwitching) {
  int rc;
  ConcurrentSearchCtx *cxc = rp->ctx.resCtx->conc;
  do {
    if (allowSwitching) {
      CONCURRENT_CTX_TICK(cxc);
      // need to abort
      if (rp->ctx.resCtx->state == QueryState_Aborted) {
        return RS_RESULT_EOF;
      }
    }
    rc = rp->Next(&rp->ctx, res);

  } while (rc == RS_RESULT_QUEUED);
  return rc;
}

int baseResultProcessor_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  QueryExecutionCtx *q = ctx->privdata;
  if (q->rootFilter == NULL) {
    return RS_RESULT_EOF;
  }
  RSIndexResult *r;
  RSDocumentMetadata *dmd;
  do {
    int rc = q->rootFilter->Read(q->rootFilter->ctx, &r);

    // This means we are done!
    if (rc == INDEXREAD_EOF) {
      return RS_RESULT_EOF;
    } else if (!r || rc == INDEXREAD_NOTFOUND) {
      continue;
    }

    dmd = DocTable_Get(&q->ctx->spec->docs, r->docId);

    // skip deleted documents
    if (!dmd || (dmd->flags & Document_Deleted)) {
      continue;
    }
    ++ctx->resCtx->totalResults;

    break;
  } while (1);

  res->docId = r->docId;
  res->indexResult = r;
  res->score = 0;
  res->sv = dmd->sortVector;
  res->md = dmd;

  return RS_RESULT_OK;
}

SearchResult *NewSearchResult() {
  SearchResult *ret = calloc(1, sizeof(*ret));
  ret->fields = RS_NewFieldMap(4);
  ret->score = 0;
  return ret;
}

ResultProcessor *NewBaseProcessor(QueryExecutionCtx *q, SearchResultCtx *rc) {
  ResultProcessor *rp = NewResultProcessor(NULL, q);
  rp->ctx.resCtx = rc;
  rp->Next = baseResultProcessor_Next;
  return rp;
}

struct scorerCtx {
  RSScoringFunction scorer;
  RSScoringFunctionCtx *scorerCtx;
};

int scorerProcessor_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  struct scorerCtx *sc = ctx->privdata;
  int rc;

  do {

    if (RS_RESULT_EOF == (rc = ResultProcessor_Next(ctx->upstream, res, 0))) return rc;

    res->score = sc->scorer(sc->scorerCtx, res->indexResult, res->md, ctx->resCtx->minScore);
    if (res->score == RS_SCORE_FILTEROUT) ctx->resCtx->totalResults--;

    break;
  } while (1);

  return rc;
}

static void resultProcessor_GenericFree(ResultProcessor *rp) {
  free(rp->ctx.privdata);
  free(rp);
}

ResultProcessor *NewScorer(RSScoringFunction scorer, RSScoringFunctionCtx *scorerCtx,
                           ResultProcessor *upstream) {
  struct scorerCtx *sc = malloc(sizeof(*sc));

  sc->scorer = scorer;
  sc->scorerCtx = scorerCtx;
  ResultProcessor *rp = NewResultProcessor(upstream, sc);
  rp->Next = scorerProcessor_Next;
  rp->Free = resultProcessor_GenericFree;
  return rp;
}

struct sorterCtx {
  uint32_t size;
  uint32_t offset;
  heap_t *pq;
  int (*cmp)(const void *e1, const void *e2, const void *udata);
  void *cmpCtx;
  SearchResult *pooledResult;
  int accumulating;
};

void SearchResult_Free(void *p) {
  SearchResult *r = p;

  if (!r) return;
  if (r->indexResult) {
    IndexResult_Free(r->indexResult);
  }
  free(r);
}

int sorter_Yield(struct sorterCtx *sc, SearchResult *r) {

  if (sc->pq->count > 0 && sc->offset++ < sc->size) {
    SearchResult *sr = mmh_pop_max(sc->pq);
    *r = *sr;
    return RS_RESULT_OK;
  }
  return RS_RESULT_EOF;
}

void sorter_Free(ResultProcessor *rp) {
  struct sorterCtx *sc = rp->ctx.privdata;
  if (sc->pooledResult) {
    SearchResult_Free(sc->pooledResult);
  }
  mmh_free(sc->pq);
  free(sc);
  free(rp);
}

int sorter_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct sorterCtx *sc = ctx->privdata;
  if (!sc->accumulating) {
    return sorter_Yield(sc, r);
  }

  if (sc->pooledResult == NULL) {
    sc->pooledResult = NewSearchResult();
  } else {
    sc->pooledResult->fields->len = 0;
  }
  SearchResult *h = sc->pooledResult;

  int rc = ResultProcessor_Next(ctx->upstream, h, 0);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    sc->accumulating = 0;
    return sorter_Yield(sc, r);
  }

  // printf("pq count %d, pq size %d\n", sc->pq->count, sc->pq->size);
  if (sc->pq->count + 1 < sc->pq->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    h->indexResult = IndexResult_DeepCopy(h->indexResult);
    mmh_insert(sc->pq, h);
    sc->pooledResult = NULL;

  } else {
    // find the min result
    SearchResult *minh = mmh_peek_min(sc->pq);

    // update the min score. Irrelevant to SORTBY mode but hardly costs anything...
    if (minh->score > ctx->resCtx->minScore) {
      ctx->resCtx->minScore = minh->score;
    }

    // if needed - pop it and insert a new result
    if (sc->cmp(h, minh, sc->cmpCtx) > 0) {
      // copy the index result to make it thread safe - but only if it is pushed to the heap
      h->indexResult = IndexResult_DeepCopy(h->indexResult);

      sc->pooledResult = mmh_pop_min(sc->pq);
      IndexResult_Free(sc->pooledResult->indexResult);
      sc->pooledResult->indexResult = NULL;
      mmh_insert(sc->pq, h);
    } else {
      /* The current should not enter the pool, so just leave it as is */
      sc->pooledResult = h;
    }
  }

  return RS_RESULT_QUEUED;
}

ResultProcessor *NewSorter(RSSortingKey *sk, uint32_t size, ResultProcessor *upstream) {

  struct sorterCtx *sc = malloc(sizeof(*sc));

  if (sk) {
    sc->cmp = sortByCmp;
    sc->cmpCtx = sk;
  } else {
    sc->cmp = cmpHits;
    sc->cmpCtx = NULL;
  }

  sc->pq = mmh_init_with_size(size + 1, sc->cmp, sc->cmpCtx, SearchResult_Free);
  sc->size = size;
  sc->offset = 0;
  sc->pooledResult = NULL;
  sc->accumulating = 1;

  ResultProcessor *rp = NewResultProcessor(upstream, sc);
  rp->Next = sorter_Next;
  rp->Free = sorter_Free;
  return rp;
}

struct pagerCtx {
  uint32_t offset;
  uint32_t limit;
  uint32_t count;
};

int pager_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct pagerCtx *pc = ctx->privdata;

  int rc = ResultProcessor_Next(ctx->upstream, r, 1);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    return rc;
  }

  // not reached beginning of results
  if (pc->count < pc->offset) {
    pc->count++;
    return RS_RESULT_QUEUED;
  }
  // overshoot the count
  if (pc->count >= pc->limit + pc->offset) {
    return RS_RESULT_EOF;
  }
  pc->count++;
  return RS_RESULT_OK;
}

ResultProcessor *NewPager(ResultProcessor *upstream, uint32_t offset, uint32_t limit) {
  struct pagerCtx *pc = malloc(sizeof(*pc));
  pc->offset = offset;
  pc->limit = limit;
  pc->count = 0;
  ResultProcessor *rp = NewResultProcessor(upstream, pc);

  rp->Next = pager_Next;
  rp->Free = resultProcessor_GenericFree;
  return rp;
}

struct loaderCtx {
  RedisSearchCtx *ctx;
  size_t numFields;
  const char **loadfields;
};

int loader_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct loaderCtx *lc = ctx->privdata;

  SearchResult res;
  int rc = ResultProcessor_Next(ctx->upstream, &res, 1);
  // END - let's write the total processed size
  if (rc == RS_RESULT_EOF) {
    return rc;
  }
  Document doc = {NULL};
  RedisModuleKey *rkey = NULL;

  // Current behavior skips entire result if document does not exist.
  // I'm unusre if that's intentional or an oversight.

  RedisModuleString *idstr =
      RedisModule_CreateString(lc->ctx->redisCtx, res.md->key, strlen(res.md->key));
  Redis_LoadDocumentEx(lc->ctx, idstr, lc->loadfields, lc->numFields, &doc, &rkey);
  RedisModule_FreeString(lc->ctx->redisCtx, idstr);

  // TODO: load should return strings, not redis strings
  for (int i = 0; i < doc.numFields; i++) {
    RSFieldMap_Set(res.fields, doc.fields[i].name,
                   doc.fields[i].text
                       ? RS_CStringVal((char *)RedisModule_StringPtrLen(doc.fields[i].text, NULL))
                       : RS_NullVal());
  }
  *r = res;

  return RS_RESULT_OK;
}

ResultProcessor *NewLoader(ResultProcessor *upstream, RSSearchRequest *r) {
  struct loaderCtx *sc = malloc(sizeof(*sc));
  sc->ctx = r->sctx;
  sc->loadfields = r->retfields;
  sc->numFields = r->nretfields;

  ResultProcessor *rp = NewResultProcessor(upstream, sc);

  rp->Next = loader_Next;
  rp->Free = resultProcessor_GenericFree;
  return rp;
}

// Free just frees up the processor
ResultProcessor *Query_BuildProcessorChain(QueryExecutionCtx *q, RSSearchRequest *req) {

  q->resultCtx = (SearchResultCtx){
      .conc = &q->conc,
      .errorString = NULL,
      .minScore = 0,
      .totalResults = 0,
      .state = QueryState_OK,
  };

  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(q, &q->resultCtx);

  // If we are not in SORTBY mode - add a scorer to the chain
  if (q->sortKey == NULL) {
    next = NewScorer(q->scorer, &q->scorerCtx, next);
  }

  // The sorter sorts the top-N results
  next = NewSorter(q->sortKey, req->offset + req->num, next);

  // The pager pages over the results of the sorter
  next = NewPager(next, req->offset, req->num);

  // The loader loads the documents from redis
  next = NewLoader(next, req);

  return next;
}