#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

#include "geo_index.h"
#include "index.h"
#include "query.h"
#include "config.h"
#include "query_parser/parser.h"
#include "redis_index.h"
#include "tokenize.h"
#include "util/logging.h"
#include "extension.h"
#include "ext/default.h"
#include "rmutil/sds.h"
#include "tag_index.h"
#include "err.h"
#include "concurrent_ctx.h"
#include "numeric_index.h"
#include "numeric_filter.h"
#include "util/strconv.h"

#define EFFECTIVE_FIELDMASK(q_, qn_) ((qn_)->opts.fieldMask & (q)->opts->fieldmask)

static IndexIterator *getEOFIterator(void);

static void QueryTokenNode_Free(QueryTokenNode *tn) {

  if (tn->str) free(tn->str);
}

static void QueryPhraseNode_Free(QueryPhraseNode *pn) {
  for (int i = 0; i < pn->numChildren; i++) {
    QueryNode_Free(pn->children[i]);
  }
  if (pn->children) {
    free(pn->children);
    pn->children = NULL;
  }
}

static void QueryUnionNode_Free(QueryUnionNode *pn) {
  for (int i = 0; i < pn->numChildren; i++) {
    QueryNode_Free(pn->children[i]);
  }
  if (pn->children) {
    free(pn->children);
    pn->children = NULL;
  }
}

static void QueryTagNode_Free(QueryTagNode *tag) {

  for (int i = 0; i < tag->numChildren; i++) {
    QueryNode_Free(tag->children[i]);
  }
  if (tag->children) {
    free(tag->children);
    tag->children = NULL;
  }
  free((char *)tag->fieldName);
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
      NumericFilter_Free((void *)n->nn.nf);

      break;  //
    case QN_NOT:
      QueryNode_Free(n->inverted.child);
      break;
    case QN_OPTIONAL:
      QueryNode_Free(n->opt.child);
      break;
    case QN_PREFX:
      QueryTokenNode_Free(&n->pfx);
      break;
    case QN_GEO:
      if (n->gn.gf) {
        GeoFilter_Free((void *)n->gn.gf);
      }
      break;
    case QN_FUZZY:
      QueryTokenNode_Free(&n->fz.tok);
      break;
    case QN_WILDCARD:
    case QN_IDS:

      break;

    case QN_TAG:
      QueryTagNode_Free(&n->tag);

    case QN_NULL:
      break;
  }
  free(n);
}

QueryNode *NewQueryNode(QueryNodeType type) {
  QueryNode *s = calloc(1, sizeof(QueryNode));
  s->type = type;
  s->opts = (QueryNodeOptions){
      .fieldMask = RS_FIELDMASK_ALL,
      .flags = 0,
      .maxSlop = -1,
      .inOrder = 0,
      .weight = 1,
  };
  return s;
}

QueryNode *NewTokenNodeExpanded(QueryAST *q, const char *s, size_t len, RSTokenFlags flags) {
  QueryNode *ret = NewQueryNode(QN_TOKEN);
  q->numTokens++;
  ret->tn = (QueryTokenNode){.str = (char *)s, .len = len, .expanded = 1, .flags = flags};
  return ret;
}

QueryNode *NewTokenNode(QueryParseCtx *q, const char *s, size_t len) {
  if (len == (size_t)-1) {
    len = strlen(s);
  }

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

QueryNode *NewFuzzyNode(QueryParseCtx *q, const char *s, size_t len, int maxDist) {
  QueryNode *ret = NewQueryNode(QN_FUZZY);
  q->numTokens++;

  ret->fz = (QueryFuzzyNode){
      .tok =
          (RSToken){
              .str = (char *)s,
              .len = len,
              .expanded = 0,
              .flags = 0,
          },
      .maxDist = maxDist,
  };
  return ret;
}

QueryNode *NewUnionNode() {
  QueryNode *ret = NewQueryNode(QN_UNION);
  // ret->fieldMask = 0;
  ret->un = (QueryUnionNode){.children = NULL, .numChildren = 0};
  return ret;
}

QueryNode *NewPhraseNode(int exact) {
  QueryNode *ret = NewQueryNode(QN_PHRASE);
  // ret->fieldMask = 0;

  ret->pn = (QueryPhraseNode){.children = NULL, .numChildren = 0, .exact = exact};
  return ret;
}

QueryNode *NewNotNode(QueryNode *n) {
  QueryNode *ret = NewQueryNode(QN_NOT);
  ret->inverted.child = n;
  return ret;
}

QueryNode *NewOptionalNode(QueryNode *n) {
  QueryNode *ret = NewQueryNode(QN_OPTIONAL);
  ret->inverted.child = n;
  return ret;
}

QueryNode *NewTagNode(const char *field, size_t len) {

  QueryNode *ret = NewQueryNode(QN_TAG);
  ret->tag.fieldName = field;
  ret->tag.len = len;
  ret->tag.numChildren = 0;
  ret->tag.children = NULL;
  return ret;
}

QueryNode *NewNumericNode(const NumericFilter *flt) {
  QueryNode *ret = NewQueryNode(QN_NUMERIC);
  ret->nn = (QueryNumericNode){.nf = (NumericFilter *)flt};

  return ret;
}

QueryNode *NewGeofilterNode(const GeoFilter *flt) {
  QueryNode *ret = NewQueryNode(QN_GEO);
  ret->gn.gf = flt;
  return ret;
}

static void setFilterNode(QueryAST *q, QueryNode *n) {
  if (q->root == NULL || n == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (q->root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator.
    // TODO: do this in a smart manner
    QueryPhraseNode_AddChild(q->root, n);
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

void QAST_SetGlobalFilters(QueryAST *ast, const QAST_GlobalFilterOptions *options) {
  if (options->numeric) {
    QueryNode *n = NewQueryNode(QN_NUMERIC);
    n->nn.nf = (NumericFilter *)options->numeric;
    setFilterNode(ast, n);
  }
  if (options->geo) {
    QueryNode *n = NewQueryNode(QN_GEO);
    n->gn.gf = options->geo;
    setFilterNode(ast, n);
  }
  if (options->ids) {
    QueryNode *n = NewQueryNode(QN_IDS);
    n->fn.ids = options->ids;
    n->fn.len = options->nids;
    setFilterNode(ast, n);
  }
}

static void QueryNode_Expand(RSQueryTokenExpander expander, RSQueryExpanderCtx *expCtx,
                             QueryNode **pqn) {

  QueryNode *qn = *pqn;
  // Do not expand verbatim nodes
  if (qn->opts.flags & QueryNode_Verbatim) {
    return;
  }

  if (qn->type == QN_TOKEN) {
    expCtx->currentNode = pqn;
    expander(expCtx, &qn->tn);

  } else if (qn->type == QN_PHRASE && !qn->pn.exact) {  // do not expand exact phrases
    for (size_t i = 0; i < qn->pn.numChildren; i++) {
      QueryNode_Expand(expander, expCtx, &qn->pn.children[i]);
    }
  } else if (qn->type == QN_UNION) {
    for (size_t i = 0; i < qn->un.numChildren; i++) {
      QueryNode_Expand(expander, expCtx, &qn->un.children[i]);
    }
  }
}

IndexIterator *Query_EvalTokenNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_TOKEN) {
    return NULL;
  }

  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index
  int isSingleWord = q->numTokens == 1 && q->opts->fieldmask == RS_FIELDMASK_ALL;

  RSQueryTerm *term = NewQueryTerm(&qn->tn, q->tokenId++);

  // printf("Opening reader.. `%s` FieldMask: %llx\n", term->str, EFFECTIVE_FIELDMASK(q, qn));

  IndexReader *ir = Redis_OpenReader(q->sctx, term, q->docTable, isSingleWord,
                                     EFFECTIVE_FIELDMASK(q, qn), q->conc, qn->opts.weight);
  if (ir == NULL) {
    Term_Free(term);
    return NULL;
  }

  return NewReadIterator(ir);
}

static IndexIterator *iterateExpandedTerms(QueryEvalCtx *q, Trie *terms, const char *str,
                                           size_t len, int maxDist, int prefixMode,
                                           QueryNodeOptions *opts) {
  TrieIterator *it = Trie_Iterate(terms, str, len, maxDist, prefixMode);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"

  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist) &&
         (itsSz < q->sctx->spec->maxPrefixExpansions || q->sctx->spec->maxPrefixExpansions == -1)) {

    // Create a token for the reader
    RSToken tok = (RSToken){
        .expanded = 0,
        .flags = 0,
        .len = 0,
    };
    tok.str = runesToStr(rstr, slen, &tok.len);
    if (q->sctx && q->sctx->redisCtx) {
      RedisModule_Log(q->sctx->redisCtx, "debug", "Found fuzzy expansion: %s %f", tok.str, score);
    }

    RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);

    // Open an index reader
    IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                       q->opts->fieldmask & opts->fieldMask, q->conc, 1);

    free(tok.str);
    if (!ir) {
      Term_Free(term);
      continue;
    }

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
  return NewUnionIterator(its, itsSz, q->docTable, 1, opts->weight);
}
/* Ealuate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them */
static IndexIterator *Query_EvalPrefixNode(QueryEvalCtx *q, QueryNode *qn) {
  assert(qn->type == QN_PREFX);

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (qn->pfx.len < RSGlobalConfig.minTermPrefix) {
    return NULL;
  }
  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, qn->pfx.str, qn->pfx.len, 0, 1, &qn->opts);
}

static IndexIterator *Query_EvalFuzzyNode(QueryEvalCtx *q, QueryNode *qn) {
  assert(qn->type == QN_FUZZY);

  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, qn->pfx.str, qn->pfx.len, qn->fz.maxDist, 0, &qn->opts);
}

static IndexIterator *Query_EvalPhraseNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_PHRASE) {
    // printf("Not a phrase node!\n");
    return NULL;
  }
  QueryPhraseNode *node = &qn->pn;
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (node->numChildren == 1) {
    node->children[0]->opts.fieldMask &= qn->opts.fieldMask;
    return Query_EvalNode(q, node->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  for (int i = 0; i < node->numChildren; i++) {
    node->children[i]->opts.fieldMask &= qn->opts.fieldMask;
    iters[i] = Query_EvalNode(q, node->children[i]);
  }
  IndexIterator *ret;

  if (node->exact) {
    ret = NewIntersecIterator(iters, node->numChildren, q->docTable, EFFECTIVE_FIELDMASK(q, qn), 0,
                              1, qn->opts.weight);
  } else {
    // Let the query node override the slop/order parameters
    int slop = qn->opts.maxSlop;
    if (slop == -1) slop = q->opts->slop;

    // Let the query node override the inorder of the whole query
    int inOrder = q->opts->flags & Search_InOrder;
    if (qn->opts.inOrder) inOrder = 1;

    // If in order was specified and not slop, set slop to maximum possible value.
    // Otherwise we can't check if the results are in order
    if (inOrder && slop == -1) {
      slop = __INT_MAX__;
    }

    ret = NewIntersecIterator(iters, node->numChildren, q->docTable, EFFECTIVE_FIELDMASK(q, qn),
                              slop, inOrder, qn->opts.weight);
  }
  return ret;
}

static IndexIterator *Query_EvalWildcardNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_WILDCARD || !q->docTable) {
    return NULL;
  }

  return NewWildcardIterator(q->docTable->maxDocId);
}

static IndexIterator *Query_EvalNotNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_NOT) {
    return NULL;
  }
  QueryNotNode *node = &qn->inverted;

  return NewNotIterator(node->child ? Query_EvalNode(q, node->child) : NULL, q->docTable->maxDocId,
                        qn->opts.weight);
}

static IndexIterator *Query_EvalOptionalNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_OPTIONAL) {
    return NULL;
  }
  QueryOptionalNode *node = &qn->opt;

  return NewOptionalIterator(node->child ? Query_EvalNode(q, node->child) : NULL,
                             q->docTable->maxDocId, qn->opts.weight);
}

static IndexIterator *Query_EvalNumericNode(QueryEvalCtx *q, QueryNumericNode *node) {

  const FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, node->nf->fieldName, strlen(node->nf->fieldName));
  if (!fs || fs->type != FIELD_NUMERIC) {
    return NULL;
  }

  return NewNumericFilterIterator(q->sctx, node->nf, q->conc);
}

static IndexIterator *Query_EvalGeofilterNode(QueryEvalCtx *q, QueryGeofilterNode *node,
                                              double weight) {

  const FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, node->gf->property, strlen(node->gf->property));
  if (fs == NULL || fs->type != FIELD_GEO) {
    return NULL;
  }

  GeoIndex gi = {.ctx = q->sctx, .sp = fs};
  return NewGeoRangeIterator(&gi, node->gf, weight);
}

static IndexIterator *Query_EvalIdFilterNode(QueryEvalCtx *q, QueryIdFilterNode *node) {
  return NewIdListIterator(node->ids, node->len, 1);
}

static IndexIterator *Query_EvalUnionNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_UNION) {
    return NULL;
  }
  QueryUnionNode *node = &qn->un;

  // a union stage with one child is the same as the child, so we just return it
  if (node->numChildren == 1) {
    node->children[0]->opts.fieldMask &= qn->opts.fieldMask;
    return Query_EvalNode(q, node->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  int n = 0;
  for (int i = 0; i < node->numChildren; i++) {
    node->children[i]->opts.fieldMask &= qn->opts.fieldMask;
    IndexIterator *it = Query_EvalNode(q, node->children[i]);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    free(iters);
    return NULL;
  }

  if (n == 1) {
    IndexIterator *ret = iters[0];
    free(iters);
    return ret;
  }

  IndexIterator *ret = NewUnionIterator(iters, n, q->docTable, 0, qn->opts.weight);
  return ret;
}

typedef IndexIterator **IndexIteratorArray;

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static IndexIterator *Query_EvalTagPrefixNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                              IndexIteratorArray *iterout, double weight) {
  if (qn->type != QN_PREFX) {
    return NULL;
  }

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (qn->pfx.len < q->sctx->spec->minPrexif) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  TrieMapIterator *it = TrieMap_Iterate(idx->values, qn->pfx.str, qn->pfx.len);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = calloc(itsCap, sizeof(*its));

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  char *s;
  tm_len_t sl;
  void *ptr;

  // Find all completions of the prefix
  while (TrieMapIterator_Next(it, &s, &sl, &ptr) &&
         (itsSz < q->sctx->spec->maxPrefixExpansions || q->sctx->spec->maxPrefixExpansions == -1)) {
    IndexIterator *ret = TagIndex_OpenReader(idx, q->sctx->spec, s, sl, 1);
    if (!ret) continue;

    // Add the reader to the iterator array
    its[itsSz++] = ret;
    if (itsSz == itsCap) {
      itsCap *= 2;
      its = realloc(its, itsCap * sizeof(*its));
    }
  }

  TrieMapIterator_Free(it);

  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    free(its);
    return NULL;
  }

  *iterout = array_ensure_append(*iterout, its, itsSz, IndexIterator *);
  return NewUnionIterator(its, itsSz, q->docTable, 1, weight);
}

static IndexIterator *query_EvalSingleTagNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *n,
                                              IndexIteratorArray *iterout, double weight) {
  IndexIterator *ret = NULL;
  switch (n->type) {
    case QN_TOKEN: {
      ret = TagIndex_OpenReader(idx, q->sctx->spec, n->tn.str, n->tn.len, weight);
      break;
    }
    case QN_PREFX:
      return Query_EvalTagPrefixNode(q, idx, n, iterout, weight);

    case QN_PHRASE: {
      char *terms[n->pn.numChildren];
      for (int i = 0; i < n->pn.numChildren; i++) {
        if (n->pn.children[i]->type == QN_TOKEN) {
          terms[i] = n->pn.children[i]->tn.str;
        } else {
          terms[i] = "";
        }
      }

      sds s = sdsjoin(terms, n->pn.numChildren, " ");

      ret = TagIndex_OpenReader(idx, q->sctx->spec, s, sdslen(s), weight);
      sdsfree(s);
      break;
    }

    default:
      return NULL;
  }

  if (ret) {
    *array_ensure_tail(iterout, IndexIterator *) = ret;
  }
  return ret;
}

static IndexIterator *Query_EvalTagNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_TAG) {
    return NULL;
  }
  QueryTagNode *node = &qn->tag;
  RedisModuleKey *k = NULL;
  const FieldSpec *fs =
      IndexSpec_GetFieldCase(q->sctx->spec, node->fieldName, strlen(node->fieldName));
  if (!fs) {
    return NULL;
  }
  RedisModuleString *kstr = IndexSpec_GetFormattedKey(q->sctx->spec, fs);
  TagIndex *idx = TagIndex_Open(q->sctx, kstr, 0, &k);
  IndexIterator **total_its = NULL;

  if (!idx) {
    return NULL;
  }
  // a union stage with one child is the same as the child, so we just return it
  if (node->numChildren == 1) {
    IndexIterator *ret =
        query_EvalSingleTagNode(q, idx, node->children[0], &total_its, qn->opts.weight);
    if (ret && q->conc) {
      TagIndex_RegisterConcurrentIterators(idx, q->conc, k, kstr, (array_t *)total_its);
    }
    return ret;
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  size_t n = 0;
  for (size_t i = 0; i < node->numChildren; i++) {
    IndexIterator *it =
        query_EvalSingleTagNode(q, idx, node->children[i], &total_its, qn->opts.weight);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    free(iters);
    return NULL;
  }

  if (total_its) {
    if (q->conc) {
      TagIndex_RegisterConcurrentIterators(idx, q->conc, k, kstr, (array_t *)total_its);
    } else {
      array_free(total_its);
    }
  }

  IndexIterator *ret = NewUnionIterator(iters, n, q->docTable, 0, qn->opts.weight);
  return ret;
}

IndexIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n) {
  switch (n->type) {
    case QN_TOKEN:
      return Query_EvalTokenNode(q, n);
    case QN_PHRASE:
      return Query_EvalPhraseNode(q, n);
    case QN_UNION:
      return Query_EvalUnionNode(q, n);
    case QN_TAG:
      return Query_EvalTagNode(q, n);
    case QN_NOT:
      return Query_EvalNotNode(q, n);
    case QN_PREFX:
      return Query_EvalPrefixNode(q, n);
    case QN_FUZZY:
      return Query_EvalFuzzyNode(q, n);
    case QN_NUMERIC:
      return Query_EvalNumericNode(q, &n->nn);
    case QN_OPTIONAL:
      return Query_EvalOptionalNode(q, n);
    case QN_GEO:
      return Query_EvalGeofilterNode(q, &n->gn, n->opts.weight);
    case QN_IDS:
      return Query_EvalIdFilterNode(q, &n->fn);
    case QN_WILDCARD:
      return Query_EvalWildcardNode(q, n);
    case QN_NULL:
      return getEOFIterator();
  }

  return NULL;
}

QueryNode *Query_Parse(QueryParseCtx *);

int QAST_Parse(QueryAST *dst, const RedisSearchCtx *sctx, const RSSearchOptions *opts,
               const char *q, size_t n, QueryError *status) {
  if (!dst->query) {
    dst->query = strndup(q, n);
    dst->nquery = n;
  }
  QueryParseCtx qpCtx = {// force multiline
                         .raw = dst->query,
                         .len = dst->nquery,
                         .sctx = (RedisSearchCtx *)sctx,
                         .opts = opts,
                         .status = status};
  dst->root = Query_Parse(&qpCtx);
  // printf("Parsed %.*s. Error (Y/N): %d. Root: %p\n", (int)n, q, QueryError_HasError(status),
  //  dst->root);
  if (!dst->root) {
    if (QueryError_HasError(status)) {
      return REDISMODULE_ERR;
    } else {
      dst->root = NewQueryNode(QN_NULL);
    }
  }
  if (QueryError_HasError(status)) {
    if (dst->root) {
      QueryNode_Free(dst->root);
      dst->root = NULL;
    }
    return REDISMODULE_ERR;
  }
  dst->numTokens = qpCtx.numTokens;
  return REDISMODULE_OK;
}

static int EOI_Read(void *p, RSIndexResult **e) {
  return INDEXREAD_EOF;
}
static void EOI_Free(struct indexIterator *self) {
  // Nothing
}

static IndexIterator eofIterator = {.Read = EOI_Read, .Free = EOI_Free};

static IndexIterator *getEOFIterator(void) {
  return &eofIterator;
}

IndexIterator *QAST_Iterate(const QueryAST *qast, const RSSearchOptions *opts, RedisSearchCtx *sctx,
                            ConcurrentSearchCtx *conc, QueryError *status) {
  QueryEvalCtx qectx = {
      .conc = conc,
      .opts = opts,
      .numTokens = qast->numTokens,
      .docTable = &sctx->spec->docs,
      .sctx = sctx,
  };
  IndexIterator *root = Query_EvalNode(&qectx, qast->root);
  if (!root) {
    if (!QueryError_HasError(status)) {
      // Return the dummy iterator
      return &eofIterator;
    }
  }
  return root;
}

void QAST_Destroy(QueryAST *q) {
  QueryNode_Free(q->root);
  q->root = NULL;
  q->numTokens = 0;
  free(q->query);
  q->nquery = 0;
  q->query = NULL;
}

int QAST_Expand(QueryAST *q, const char *expander, RSSearchOptions *opts, RedisSearchCtx *sctx,
                QueryError *status) {
  if (!q->root) {
    return REDISMODULE_OK;
  }
  RSQueryExpanderCtx expCtx = {.qast = q,
                               .language = opts->language ? opts->language : DEFAULT_LANGUAGE,
                               .handle = sctx,
                               .status = status};

  ExtQueryExpanderCtx *xpc =
      Extensions_GetQueryExpander(&expCtx, expander ? expander : DEFAULT_EXPANDER_NAME);
  if (xpc && xpc->exp) {
    QueryNode_Expand(xpc->exp, &expCtx, &q->root);
    if (xpc->ff) {
      xpc->ff(expCtx.privdata);
    }
  }
  if (QueryError_HasError(status)) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
  ;
}

/* Set the field mask recursively on a query node. This is called by the parser to handle
 * situations like @foo:(bar baz|gaz), where a complex tree is being applied a field mask */
void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask) {
  if (!n) return;
  n->opts.fieldMask &= mask;
  switch (n->type) {

    case QN_PHRASE:
      for (int i = 0; i < n->pn.numChildren; i++) {
        QueryNode_SetFieldMask(n->pn.children[i], mask);
      }
      break;

    case QN_UNION:
      for (int i = 0; i < n->un.numChildren; i++) {
        QueryNode_SetFieldMask(n->un.children[i], mask);
      }
      break;

    case QN_NOT:
      QueryNode_SetFieldMask(n->inverted.child, mask);
      break;
    case QN_OPTIONAL:
      QueryNode_SetFieldMask(n->opt.child, mask);
      break;

    default:
      break;
  }
}
void QueryPhraseNode_AddChild(QueryNode *parent, QueryNode *child) {
  if (!child) return;
  QueryPhraseNode *pn = &parent->pn;

  pn->children = realloc(pn->children, sizeof(QueryNode *) * (pn->numChildren + 1));
  pn->children[pn->numChildren++] = child;
}

void QueryUnionNode_AddChild(QueryNode *parent, QueryNode *child) {
  if (!child) return;
  QueryUnionNode *un = &parent->un;

  un->children = realloc(un->children, sizeof(QueryNode *) * (un->numChildren + 1));
  un->children[un->numChildren++] = child;
}

void QueryTagNode_AddChildren(QueryNode *parent, QueryNode **children, size_t num) {
  if (!children) return;
  QueryTagNode *tn = &parent->tag;

  tn->children = realloc(tn->children, sizeof(QueryNode *) * (tn->numChildren + num));
  for (size_t i = 0; i < num; i++) {
    if (children[i] && (children[i]->type == QN_TOKEN || children[i]->type == QN_PHRASE ||
                        children[i]->type == QN_PREFX)) {
      tn->children[tn->numChildren++] = children[i];
    }
  }
}

/* Set the concurrent mode of the query. By default it's on, setting here to 0 will turn it off,
 * resulting in the query not performing context switches */
// void Query_SetConcurrentMode(QueryPlan *q, int concurrent) {
//   q->concurrentMode = concurrent;
// }

static sds doPad(sds s, int len) {
  if (!len) return s;

  char buf[len * 2 + 1];
  memset(buf, ' ', len * 2);
  buf[len * 2] = 0;
  return sdscat(s, buf);
}

static sds QueryNode_DumpSds(sds s, const IndexSpec *spec, const QueryNode *qs, int depth) {
  s = doPad(s, depth);

  if (qs->opts.fieldMask == 0) {
    s = sdscat(s, "@NULL:");
  }

  if (qs->opts.fieldMask && qs->opts.fieldMask != RS_FIELDMASK_ALL && qs->type != QN_NUMERIC &&
      qs->type != QN_GEO && qs->type != QN_IDS) {
    if (!spec) {
      s = sdscatprintf(s, "@%" PRIu64, (uint64_t)qs->opts.fieldMask);
    } else {
      s = sdscat(s, "@");
      t_fieldMask fm = qs->opts.fieldMask;
      int i = 0, n = 0;
      while (fm) {
        t_fieldMask bit = (fm & 1) << i;
        if (bit) {
          const char *f = GetFieldNameByBit(spec, bit);
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
        s = QueryNode_DumpSds(s, spec, qs->pn.children[i], depth + 1);
      }
      s = doPad(s, depth);

      break;
    case QN_TOKEN:
      s = sdscatprintf(s, "%s%s", (char *)qs->tn.str, qs->tn.expanded ? "(expanded)" : "");
      if (qs->opts.weight != 1) {
        s = sdscatprintf(s, " => {$weight: %g;}", qs->opts.weight);
      }
      s = sdscat(s, "\n");
      return s;

    case QN_PREFX:
      s = sdscatprintf(s, "PREFIX{%s*", (char *)qs->pfx.str);
      break;

    case QN_NOT:
      s = sdscat(s, "NOT{\n");
      s = QueryNode_DumpSds(s, spec, qs->inverted.child, depth + 1);
      s = doPad(s, depth);
      break;

    case QN_OPTIONAL:
      s = sdscat(s, "OPTIONAL{\n");
      s = QueryNode_DumpSds(s, spec, qs->inverted.child, depth + 1);
      s = doPad(s, depth);
      break;

    case QN_NUMERIC: {
      const NumericFilter *f = qs->nn.nf;
      s = sdscatprintf(s, "NUMERIC {%f %s @%s %s %f", f->min, f->inclusiveMin ? "<=" : "<",
                       f->fieldName, f->inclusiveMax ? "<=" : "<", f->max);
    } break;
    case QN_UNION:
      s = sdscat(s, "UNION {\n");
      for (int i = 0; i < qs->un.numChildren; i++) {
        s = QueryNode_DumpSds(s, spec, qs->un.children[i], depth + 1);
      }
      s = doPad(s, depth);
      break;
    case QN_TAG:
      s = sdscatprintf(s, "TAG:@%.*s {\n", (int)qs->tag.len, qs->tag.fieldName);
      for (int i = 0; i < qs->tag.numChildren; i++) {
        s = QueryNode_DumpSds(s, spec, qs->tag.children[i], depth + 1);
      }
      s = doPad(s, depth);
      break;
    case QN_GEO:

      s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s", qs->gn.gf->property, qs->gn.gf->lon,
                       qs->gn.gf->lat, qs->gn.gf->radius, qs->gn.gf->unit);
      break;
    case QN_IDS:

      s = sdscat(s, "IDS { ");
      for (int i = 0; i < qs->fn.len; i++) {
        s = sdscatprintf(s, "%llu,", (unsigned long long)qs->fn.ids[i]);
      }
      break;
    case QN_WILDCARD:

      s = sdscat(s, "<WILDCARD>");
      break;
    case QN_FUZZY:
      s = sdscatprintf(s, "FUZZY{%s}\n", qs->fz.tok.str);
      return s;

    case QN_NULL:
      s = sdscat(s, "<empty>");
  }

  s = sdscat(s, "}");
  // print attributes if not the default
  if (qs->opts.weight != 1 || qs->opts.maxSlop != -1 || qs->opts.inOrder) {
    s = sdscat(s, " => {");
    if (qs->opts.weight != 1) {
      s = sdscatprintf(s, " $weight: %g;", qs->opts.weight);
    }
    if (qs->opts.maxSlop != -1) {
      s = sdscatprintf(s, " $slop: %d;", qs->opts.maxSlop);
    }
    if (qs->opts.inOrder || qs->opts.maxSlop != -1) {
      s = sdscatprintf(s, " $inorder: %s;", qs->opts.inOrder ? "true" : "false");
    }
    s = sdscat(s, " }");
  }
  s = sdscat(s, "\n");
  return s;
}

/* Return a string representation of the query parse tree. The string should be freed by the
 * caller
 */
char *QAST_DumpExplain(const QueryAST *q, const IndexSpec *spec) {
  // empty query
  if (!q || !q->root) {
    return strdup("NULL");
  }

  sds s = QueryNode_DumpSds(sdsnew(""), spec, q->root, 0);
  char *ret = strndup(s, sdslen(s));
  sdsfree(s);
  return ret;
}

void QAST_Print(const QueryAST *ast, const IndexSpec *spec) {
  sds s = QueryNode_DumpSds(sdsnew(""), spec, ast->root, 0);
  printf("%s\n", s);
  sdsfree(s);
}

int Query_NodeForEach(QueryAST *q, QueryNode_ForEachCallback callback, void *ctx) {
#define INITIAL_ARRAY_NODE_SIZE 5
  QueryNode **nodes = array_new(QueryNode *, INITIAL_ARRAY_NODE_SIZE);
  nodes = array_append(nodes, q->root);
  int retVal = 1;
  while (array_len(nodes) > 0) {
    QueryNode *curr = array_pop(nodes);
    if (!callback(curr, q, ctx)) {
      retVal = 0;
      break;
    }
    switch (curr->type) {
      case QN_PHRASE:
        for (int i = 0; i < curr->pn.numChildren; i++) {
          nodes = array_append(nodes, curr->pn.children[i]);
        }
        break;

      case QN_NOT:
        nodes = array_append(nodes, curr->inverted.child);
        break;

      case QN_OPTIONAL:
        nodes = array_append(nodes, curr->opt.child);
        break;

      case QN_UNION:
        for (int i = 0; i < curr->un.numChildren; i++) {
          nodes = array_append(nodes, curr->un.children[i]);
        }
        break;

      case QN_TAG:
        for (int i = 0; i < curr->tag.numChildren; i++) {
          nodes = array_append(nodes, curr->tag.children[i]);
        }
        break;

      case QN_GEO:
      case QN_IDS:
      case QN_WILDCARD:
      case QN_FUZZY:
      case QN_TOKEN:
      case QN_PREFX:
      case QN_NUMERIC:
      case QN_NULL:
        break;
    }
  }

  array_free(nodes);
  return retVal;
}

static int QueryNode_ApplyAttribute(QueryNode *qn, QueryAttribute *attr, QueryError *status) {

#define MK_INVALID_VALUE()                                                         \
  QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "Invalid value (%.*s) for `%.*s`", \
                         (int)attr->vallen, attr->value, (int)attr->namelen, attr->name)

  // Apply slop: [-1 ... INF]
  if (STR_EQCASE(attr->name, attr->namelen, "slop")) {
    long long n;
    if (!ParseInteger(attr->value, &n) || n < -1) {
      MK_INVALID_VALUE();
      return 0;
    }
    qn->opts.maxSlop = n;

  } else if (STR_EQCASE(attr->name, attr->namelen, "inorder")) {
    // Apply inorder: true|false
    int b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return 0;
    }
    qn->opts.inOrder = b;

  } else if (STR_EQCASE(attr->name, attr->namelen, "weight")) {
    // Apply weight: [0  ... INF]
    double d;
    if (!ParseDouble(attr->value, &d) || d < 0) {
      MK_INVALID_VALUE();
      return 0;
    }
    qn->opts.weight = d;

  } else if (STR_EQCASE(attr->name, attr->namelen, "phonetic")) {
    // Apply phonetic: true|false
    int b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return 0;
    }
    if (b) {
      qn->opts.phonetic = PHONETIC_ENABLED;  // means we specifically asked for phonetic matching
    } else {
      qn->opts.phonetic =
          PHONETIC_DESABLED;  // means we specifically asked no for phonetic matching
    }
    // qn->opts.noPhonetic = PHONETIC_DEFAULT -> means no special asks regarding phonetics
    //                                          will be enable if field was declared phonetic

  } else {
    QueryError_SetErrorFmt(status, QUERY_ENOOPTION, "Invalid attribute %.*s", (int)attr->namelen,
                           attr->name);
    return 0;
  }

  return 1;
}

int QueryNode_ApplyAttributes(QueryNode *qn, QueryAttribute *attrs, size_t len,
                              QueryError *status) {
  for (size_t i = 0; i < len; i++) {
    if (!QueryNode_ApplyAttribute(qn, &attrs[i], status)) {
      return 0;
    }
  }
  return 1;
}
