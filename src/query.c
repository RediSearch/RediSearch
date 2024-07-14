/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

#include "geo_index.h"
#include "index.h"
#include "query.h"
#include "config.h"
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
#include "util/arr.h"
#include "rmutil/rm_assert.h"
#include "module.h"
#include "query_internal.h"
#include "aggregate/aggregate.h"
#include "suffix.h"
#include "wildcard/wildcard.h"
#include "geometry/geometry_api.h"

#define EFFECTIVE_FIELDMASK(q_, qn_) ((qn_)->opts.fieldMask & (q)->opts->fieldmask)

static void QueryTokenNode_Free(QueryTokenNode *tn) {
  if (tn->str) rm_free(tn->str);
}

static void QueryTagNode_Free(QueryTagNode *tag) {
  rm_free((char *)tag->fieldName);
}

static void QueryGeometryNode_Free(QueryGeometryNode *geom) {
  if (geom->geomq) {
    GeometryQuery_Free(geom->geomq);
    geom->geomq = NULL;
  }
}

static void QueryLexRangeNode_Free(QueryLexRangeNode *lx) {
  if (lx->begin) rm_free(lx->begin);
  if (lx->end) rm_free(lx->end);
}

static void QueryVectorNode_Free(QueryVectorNode *vn) {
  if (vn->vq) {
    VectorQuery_Free(vn->vq);
    vn->vq = NULL;
  }
}

static void QueryMissingNode_Free(QueryMissingNode *missn) {
  rm_free((char *)missn->fieldName);
}

void QueryNode_Free(QueryNode *n) {
  if (!n) return;

  if (n->children) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
      QueryNode_Free(n->children[ii]);
    }
    array_free(n->children);
    n->children = NULL;
  }

  if (n->params) {
    for (size_t ii = 0; ii < QueryNode_NumParams(n); ++ii) {
      Param_FreeInternal(&n->params[ii]);
    }
    array_free(n->params);
    n->params = NULL;
  }
  if (n->opts.distField) {
    rm_free(n->opts.distField);
  }

  switch (n->type) {
    case QN_TOKEN:
      QueryTokenNode_Free(&n->tn);
      break;
    case QN_NUMERIC:
      NumericFilter_Free((void *)n->nn.nf);
      break;
    case QN_PREFIX:
      QueryTokenNode_Free(&n->pfx.tok);
      break;
    case QN_GEO:
      if (n->gn.gf) {
        GeoFilter_Free((void *)n->gn.gf);
      }
      break;
    case QN_FUZZY:
      QueryTokenNode_Free(&n->fz.tok);
      break;
    case QN_LEXRANGE:
      QueryLexRangeNode_Free(&n->lxrng);
      break;
    case QN_VECTOR:
      QueryVectorNode_Free(&n->vn);
      break;
    case QN_WILDCARD_QUERY:
      QueryTokenNode_Free(&n->verb.tok);
      break;
    case QN_WILDCARD:
    case QN_IDS:
      break;
    case QN_TAG:
      QueryTagNode_Free(&n->tag);
      break;
    case QN_GEOMETRY:
      QueryGeometryNode_Free(&n->gmn);
      break;
    case QN_MISSING:
      QueryMissingNode_Free(&n->miss);
      break;
    case QN_UNION:
    case QN_NOT:
    case QN_OPTIONAL:
    case QN_NULL:
    case QN_PHRASE:
      break;
  }
  rm_free(n);
}

void RangeNumber_Free(RangeNumber *r) {
  rm_free(r);
}

// Add a new metric request to the metricRequests array. Returns the index of the request
static int addMetricRequest(QueryEvalCtx *q, char *metric_name, RLookupKey **key_addr) {
  MetricRequest mr = {metric_name, key_addr};
  *q->metricRequestsP = array_ensure_append_1(*q->metricRequestsP, mr);
  return array_len(*q->metricRequestsP) - 1;
}

QueryNode *NewQueryNode(QueryNodeType type) {
  QueryNode *s = rm_calloc(1, sizeof(QueryNode));
  s->type = type;
  s->opts = (QueryNodeOptions){
    .fieldMask = RS_FIELDMASK_ALL,
    .flags = 0,
    .maxSlop = -1,
    .inOrder = 0,
    .weight = 1,
    .distField = NULL,
  };
  return s;
}

QueryNode *NewQueryNodeChildren(QueryNodeType type, QueryNode **children, size_t n) {
  QueryNode *ret = NewQueryNode(type);
  ret->children = array_ensure_append(ret->children, children, n, QueryNode *);
  return ret;
}

QueryNode *NewTokenNodeExpanded(QueryAST *q, const char *s, size_t len, RSTokenFlags flags) {
  QueryNode *ret = NewQueryNode(QN_TOKEN);
  q->numTokens++;
  ret->tn = (QueryTokenNode){
    .str = (char *)s,
    .len = len,
    .expanded = 1,
    .flags = flags,
  };
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

QueryNode *NewTokenNode_WithParams(QueryParseCtx *q, QueryToken *qt) {
  QueryNode *ret = NewQueryNode(QN_TOKEN);
  q->numTokens++;

  if (qt->type == QT_TERM || qt->type == QT_TERM_CASE || qt->type == QT_NUMERIC
      || qt->type == QT_SIZE) {
    char *s;
    size_t len;
    if (qt->type == QT_TERM) {
      s = rm_strdupcase(qt->s, qt->len);
      len = strlen(s);
    } else {
      s = rm_strndup(qt->s, qt->len);
      len = qt->len;
    }
    ret->tn = (QueryTokenNode){.str = s, .len = len, .expanded = 0, .flags = 0};
    // Do not expand numbers
    if(qt->type == QT_NUMERIC || qt->type == QT_SIZE) {
        ret->opts.flags |= QueryNode_Verbatim;
    }
  } else {
    ret->tn = (QueryTokenNode){.str = NULL, .len = 0, .expanded = 0, .flags = 0};
    QueryNode_InitParams(ret, 1);
    QueryNode_SetParam(q, &ret->params[0], &ret->tn.str, &ret->tn.len, qt);
  }
  return ret;
}

void QueryNode_InitParams(QueryNode *n, size_t num) {
  n->params = array_newlen(Param, num);
  memset(n->params, 0, sizeof(*n->params) * num);
}

bool QueryNode_SetParam(QueryParseCtx *q, Param *target_param, void *target_value,
                        size_t *target_len, QueryToken *source) {
  return QueryParam_SetParam(
    q, target_param, target_value, target_len,
    source); //FIXME: Move to a common location for QueryNode and QueryParam
}

QueryNode *NewPrefixNode_WithParams(QueryParseCtx *q, QueryToken *qt, bool prefix, bool suffix) {
  QueryNode *ret = NewQueryNode(QN_PREFIX);
  ret->pfx.prefix = prefix;
  ret->pfx.suffix = suffix;
  q->numTokens++;
  if (qt->type == QT_TERM) {
    char *s = rm_strndup_unescape(qt->s, qt->len);
    ret->pfx.tok = (RSToken){.str = s, .len = strlen(s), .expanded = 0, .flags = 0};
  } else {
    assert (qt->type == QT_PARAM_TERM);
    QueryNode_InitParams(ret, 1);
    QueryNode_SetParam(q, &ret->params[0], &ret->pfx.tok.str, &ret->pfx.tok.len, qt);
  }
  return ret;
}

QueryNode *NewWildcardNode_WithParams(QueryParseCtx *q, QueryToken *qt) {
  QueryNode *ret = NewQueryNode(QN_WILDCARD_QUERY);
  q->numTokens++;
  if (qt->type == QT_WILDCARD) {
    // ensure str is NULL terminated
    char *s = rm_malloc(qt->len + 1);
    memcpy(s, qt->s, qt->len);
    s[qt->len] = '\0';
    ret->verb.tok = (RSToken){.str = s, .len = qt->len, .expanded = 0, .flags = 0};
  } else {
    assert(qt->type == QT_PARAM_WILDCARD);
    QueryNode_InitParams(ret, 1);
    QueryNode_SetParam(q, &ret->params[0], &ret->verb.tok.str, &ret->verb.tok.len, qt);
    ret->params[0].type = PARAM_WILDCARD;
  }
  return ret;
}

QueryNode *NewFuzzyNode_WithParams(QueryParseCtx *q, QueryToken *qt, int maxDist) {
  QueryNode *ret = NewQueryNode(QN_FUZZY);
  q->numTokens++;

  if (qt->type == QT_TERM || qt->type == QT_NUMERIC || qt->type == QT_SIZE) {
    char *s = rm_strdupcase(qt->s, qt->len);
    ret->fz = (QueryFuzzyNode){
      .tok =
          (RSToken){
            .str = (char *)s,
            .len = strlen(s),
            .expanded = 0,
            .flags = 0,
            },
      .maxDist = maxDist,
    };
  } else {
    ret->fz.maxDist = maxDist;
    assert(qt->type == QT_PARAM_TERM);
    QueryNode_InitParams(ret, 1);
    QueryNode_SetParam(q, &ret->params[0], &ret->fz.tok.str, &ret->fz.tok.len, qt);
  }
  return ret;
}

QueryNode *NewPhraseNode(int exact) {
  QueryNode *ret = NewQueryNode(QN_PHRASE);
  ret->pn.exact = exact;
  return ret;
}

QueryNode *NewTagNode(const char *field, size_t len) {
  QueryNode *ret = NewQueryNode(QN_TAG);
  ret->tag.fieldName = field;
  ret->tag.len = len;
  return ret;
}

QueryNode *NewNumericNode(QueryParam *p) {
  QueryNode *ret = NewQueryNode(QN_NUMERIC);
  // Move data and params pointers
  ret->nn.nf = p->nf;
  ret->params = p->params;
  p->nf = NULL;
  p->params = NULL;
  rm_free(p);
  return ret;
}

QueryNode *NewGeofilterNode(QueryParam *p) {
  assert(p->type == QP_GEO_FILTER);
  QueryNode *ret = NewQueryNode(QN_GEO);
  // Move data and params pointers
  ret->gn.gf = p->gf;
  ret->params = p->params;
  p->gf = NULL;
  p->params = NULL;
  rm_free(p);
  return ret;
}

QueryNode *NewMissingNode(const char *field, size_t len) {
  QueryNode *ret = NewQueryNode(QN_MISSING);
  ret->miss.fieldName = field;
  ret->miss.len = len;
  return ret;
}

static enum QueryType parseGeometryPredicate(const char *predicate, size_t len) {
  enum QueryType query_type;
  // length is insufficient to uniquely identify predicates. CONTAINS, DISJOINT, and DISTANCE all have 8 chars.
  // first letter is insufficient. DISJOINT and DISTANCE both start with DIS.
  // last letter is insufficient. CONTAINS and INTERSECTS both end with S, DISJOINT and NEAREST both end with T.
  // TODO: consider comparing 8-byte values instead of 2-byte
  const int cmp = ((len << CHAR_BIT) | toupper(predicate[len-1]));
#define CASE(s) (((sizeof(s)-1) << CHAR_BIT) | s[sizeof(s)-2])  // two bytes: len | last char
#define COND(s) ((cmp == CASE(s)) && !strncasecmp(predicate, s, len))
  if COND("WITHIN") {  // 0x06'4E
    return WITHIN;
  }
  if COND("CONTAINS") { // 0x08'53
    return CONTAINS;
  }
  if COND("DISJOINT") { // 0x08'54
    return DISJOINT;
  }
  if COND("INTERSECTS") { // 0x0A'53
    return INTERSECTS;
  }
  COND("DISTANCE"); // 0x08'45
  COND("NEAREST"); // 0x07'54
  return UNKNOWN_QUERY;
}

QueryNode *NewGeometryNode_FromWkt_WithParams(struct QueryParseCtx *q, const char *predicate, size_t len, QueryToken *wkt) {
  enum QueryType query_type = parseGeometryPredicate(predicate, len);
  if (query_type == UNKNOWN_QUERY) {
    return NULL;
  }
  QueryNode *ret = NewQueryNode(QN_GEOMETRY);
  GeometryQuery *geomq = rm_calloc(1, sizeof(*geomq));
  geomq->format = GEOMETRY_FORMAT_WKT;
  geomq->query_type = query_type;
  QueryNode_InitParams(ret, 1);
  QueryNode_SetParam(q, &ret->params[0], &geomq->str, &geomq->str_len, wkt);
  ret->gmn.geomq = geomq;
  return ret;
}

// TODO: to be more generic, consider using variadic function, or use different functions for each command
QueryNode *NewVectorNode_WithParams(struct QueryParseCtx *q, VectorQueryType type, QueryToken *value, QueryToken *vec) {
  QueryNode *ret = NewQueryNode(QN_VECTOR);
  VectorQuery *vq = rm_calloc(1, sizeof(*vq));
  ret->vn.vq = vq;
  vq->type = type;
  ret->opts.flags |= QueryNode_YieldsDistance;
  switch (type) {
    case VECSIM_QT_KNN:
      QueryNode_InitParams(ret, 2);
      QueryNode_SetParam(q, &ret->params[0], &vq->knn.vector, &vq->knn.vecLen, vec);
      QueryNode_SetParam(q, &ret->params[1], &vq->knn.k, NULL, value);
      break;
    case VECSIM_QT_RANGE:
      QueryNode_InitParams(ret, 2);
      QueryNode_SetParam(q, &ret->params[0], &vq->range.vector, &vq->range.vecLen, vec);
      QueryNode_SetParam(q, &ret->params[1], &vq->range.radius, NULL, value);
      vq->range.order = BY_ID;
      break;
    default:
      QueryNode_Free(ret);
      return NULL;
  }
  return ret;
}

static void setFilterNode(QueryAST *q, QueryNode *n) {
  if (q->root == NULL || n == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (q->root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator.
    q->root->children = array_ensure_prepend(q->root->children, &n, 1, QueryNode *);
    q->numTokens++;
  // vector node of type KNN should always be in the root, so we have a special case here.
  } else if (q->root->type == QN_VECTOR && q->root->vn.vq->type == VECSIM_QT_KNN) {
    // for non-hybrid - add the filter node as the child of the vector node.
    if (QueryNode_NumChildren(q->root) == 0) {
      QueryNode_AddChild(q->root, n);
    // otherwise, add a new phrase node as the parent of the current child of the hybrid vector node,
    // and set its children to be the previous child and the new filter node.
    } else {
      RS_LOG_ASSERT(QueryNode_NumChildren(q->root) == 1, "Vector query node can have at most one child");
      QueryNode *nr = NewPhraseNode(0);
      QueryNode_AddChild(nr, n);
      QueryNode_AddChild(nr, q->root->children[0]);
      q->root->children[0] = nr;
    }
  } else {  // for other types, we need to create a new phrase node
    QueryNode *nr = NewPhraseNode(0);
    QueryNode_AddChild(nr, n);
    QueryNode_AddChild(nr, q->root);
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

  int expandChildren = 0;

  if (qn->type == QN_TOKEN && qn->tn.len > 0) {
    expCtx->currentNode = pqn;
    expander(expCtx, &qn->tn);
  } else if (qn->type == QN_UNION ||
             (qn->type == QN_PHRASE && !qn->pn.exact)) {  // do not expand exact phrases
    expandChildren = 1;
  }
  if (expandChildren) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(qn); ++ii) {
      QueryNode_Expand(expander, expCtx, &qn->children[ii]);
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

  const FieldSpec *fs = IndexSpec_GetFieldByBit(q->sctx->spec, qn->opts.fieldMask);
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

static inline void addTerm(char *str, size_t tok_len, QueryEvalCtx *q,
  QueryNodeOptions *opts, IndexIterator ***its, size_t *itsSz, size_t *itsCap) {
  // Create a token for the reader
  RSToken tok = (RSToken){
      .expanded = 0,
      .flags = 0,
      .len = tok_len,
      .str = str
  };

  RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);

  // Open an index reader
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                     q->opts->fieldmask & opts->fieldMask, q->conc, 1);

  if (!ir) {
    Term_Free(term);
    return;
  }

  // Add the reader to the iterator array
  (*its)[(*itsSz)++] = NewReadIterator(ir);
  if (*itsSz == *itsCap) {
    *itsCap *= 2;
    *its = rm_realloc(*its, (*itsCap) * sizeof(*its));
  }
}

static IndexIterator *iterateExpandedTerms(QueryEvalCtx *q, Trie *terms, const char *str,
                                           size_t len, int maxDist, int prefixMode,
                                           QueryNodeOptions *opts) {
  TrieIterator *it = Trie_Iterate(terms, str, len, maxDist, prefixMode);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  char *target_str = NULL;
  size_t tok_len = 0;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist) &&
         (itsSz < q->config->maxPrefixExpansions)) {
    target_str = runesToStr(rstr, slen, &tok_len);
    addTerm(target_str, tok_len, q, opts, &its, &itsSz, &itsCap);
    rm_free(target_str);
  }

  // Add an iterator over the inverted index of the empty string for fuzzy search
  if (!prefixMode && q->sctx->apiVersion >= 2 && len <= maxDist) {
    addTerm("", 0, q, opts, &its, &itsSz, &itsCap);
  }

  TrieIterator_Free(it);
  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    rm_free(its);
    return NULL;
  }
  QueryNodeType type = prefixMode ? QN_PREFIX : QN_FUZZY;
  return NewUnionIterator(its, itsSz, 1, opts->weight, type, str, q->config);
}

typedef struct {
  IndexIterator **its;
  size_t nits;
  size_t cap;
  QueryEvalCtx *q;
  QueryNodeOptions *opts;
  double weight;
} ContainsCtx;

static int runeIterCb(const rune *r, size_t n, void *p, void *payload);
static int charIterCb(const char *s, size_t n, void *p, void *payload);

/* Evaluate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them.
 * Used for Prefix, Contains and suffix nodes.
*/
static IndexIterator *Query_EvalPrefixNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_PREFIX, "query node type should be prefix");

  // we allow a minimum of 2 letters in the prefix by default (configurable)
  if (qn->pfx.tok.len < q->config->minTermPrefix) {
    return NULL;
  }

  IndexSpec *spec = q->sctx->spec;
  Trie *t = spec->terms;
  ContainsCtx ctx = {.q = q, .opts = &qn->opts};

  if (!t) {
    return NULL;
  }

  rune *str = NULL;
  size_t nstr;
  if (qn->pfx.tok.str) {
    str = strToFoldedRunes(qn->pfx.tok.str, &nstr);
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  // spec support contains queries
  if (spec->suffix && qn->pfx.suffix) {
    // all modifier fields are supported
    if (qn->opts.fieldMask == RS_FIELDMASK_ALL ||
       (spec->suffixMask & qn->opts.fieldMask) == qn->opts.fieldMask) {
      SuffixCtx sufCtx = {
        .root = spec->suffix->root,
        .rune = str,
        .runelen = nstr,
        .type = qn->pfx.prefix ? SUFFIX_TYPE_CONTAINS : SUFFIX_TYPE_SUFFIX,
        .callback = charIterCb,
        .cbCtx = &ctx,

      };
      Suffix_IterateContains(&sufCtx);
    } else {
      QueryError_SetErrorFmt(q->status, QUERY_EGENERIC, "Contains query on fields without WITHSUFFIXTRIE support");
    }
  } else {
    TrieNode_IterateContains(t->root, str, nstr, qn->pfx.prefix, qn->pfx.suffix,
                           runeIterCb, &ctx, &q->sctx->timeout);
  }

  rm_free(str);
  if (!ctx.its || ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  // TODO: This should be a single iterator.
  // } else if (ctx.nits == 1) {
  //   // In case of a single iterator, we can just return it
  //   return ctx.its[0];
  } else {
    return NewUnionIterator(ctx.its, ctx.nits, 1, qn->opts.weight,
                            QN_PREFIX, qn->pfx.tok.str, q->config);
  }
}

/* Evaluate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them.
 * Used for Prefix, Contains and suffix nodes.
*/
static IndexIterator *Query_EvalWildcardQueryNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_WILDCARD_QUERY, "query node type should be wildcard query");

  IndexSpec *spec = q->sctx->spec;
  Trie *t = spec->terms;
  ContainsCtx ctx = {.q = q, .opts = &qn->opts};
  RSToken *token = &qn->verb.tok;

  if (!t || !token->str) {
    return NULL;
  }

  token->len = Wildcard_RemoveEscape(token->str, token->len);
  size_t nstr;
  rune *str = strToFoldedRunes(token->str, &nstr);

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  bool fallbackBruteForce = false;
  // spec support using suffix trie
  if (spec->suffix) {
    // all modifier fields are supported
    if (qn->opts.fieldMask == RS_FIELDMASK_ALL ||
       (spec->suffixMask & qn->opts.fieldMask) == qn->opts.fieldMask) {
      SuffixCtx sufCtx = {
        .root = spec->suffix->root,
        .rune = str,
        .runelen = nstr,
        .cstr = token->str,
        .cstrlen = token->len,
        .type = SUFFIX_TYPE_WILDCARD,
        .callback = charIterCb, // the difference is weather the function receives char or rune
        .cbCtx = &ctx,
        .timeout = &q->sctx->timeout,
      };
      if (Suffix_IterateWildcard(&sufCtx) == 0) {
        // if suffix trie cannot be used, use brute force
        fallbackBruteForce = true;
      }
    } else {
      QueryError_SetErrorFmt(q->status, QUERY_EGENERIC, "Contains query on fields without WITHSUFFIXTRIE support");
    }
  }

  if (!spec->suffix || fallbackBruteForce) {
    TrieNode_IterateWildcard(t->root, str, nstr, runeIterCb, &ctx, &q->sctx->timeout);
  }

  rm_free(str);
  if (!ctx.its || ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return NewUnionIterator(ctx.its, ctx.nits, 1, qn->opts.weight,
                            QN_WILDCARD_QUERY, qn->verb.tok.str, q->config);
  }
}

typedef struct {
  IndexIterator **its;
  size_t nits;
  size_t cap;
  QueryEvalCtx *q;
  QueryNodeOptions *opts;
  double weight;
} LexRangeCtx;

static void rangeItersAddIterator(LexRangeCtx *ctx, IndexReader *ir) {
  ctx->its[ctx->nits++] = NewReadIterator(ir);
  if (ctx->nits == ctx->cap) {
    ctx->cap *= 2;
    ctx->its = rm_realloc(ctx->its, ctx->cap * sizeof(*ctx->its));
  }
}

static void rangeIterCbStrs(const char *r, size_t n, void *p, void *invidx) {
  LexRangeCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  RSToken tok = {0};
  tok.str = (char *)r;
  tok.len = n;
  RSQueryTerm *term = NewQueryTerm(&tok, ctx->q->tokenId++);
  IndexReader *ir = NewTermIndexReader(invidx, q->sctx->spec, RS_FIELDMASK_ALL, term, ctx->weight);
  if (!ir) {
    Term_Free(term);
    return;
  }

  rangeItersAddIterator(ctx, ir);
}

static int runeIterCb(const rune *r, size_t n, void *p, void *payload) {
  LexRangeCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  if (!RS_IsMock && ctx->nits >= q->config->maxPrefixExpansions) {
    return REDISEARCH_ERR;
  }
  RSToken tok = {0};
  tok.str = runesToStr(r, n, &tok.len);
  RSQueryTerm *term = NewQueryTerm(&tok, ctx->q->tokenId++);
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                     q->opts->fieldmask & ctx->opts->fieldMask, q->conc, 1);
  rm_free(tok.str);
  if (!ir) {
    Term_Free(term);
    return REDISEARCH_OK;
  }

  rangeItersAddIterator(ctx, ir);
  return REDISEARCH_OK;
}

static int charIterCb(const char *s, size_t n, void *p, void *payload) {
  LexRangeCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  if (ctx->nits >= q->config->maxPrefixExpansions) {
    return REDISEARCH_ERR;
  }
  RSToken tok = {.str = (char *)s, .len = n};
  RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                     q->opts->fieldmask & ctx->opts->fieldMask, q->conc, 1);
  if (!ir) {
    Term_Free(term);
    return REDISEARCH_OK;
  }

  rangeItersAddIterator(ctx, ir);
  return REDISEARCH_OK;
}

static IndexIterator *Query_EvalLexRangeNode(QueryEvalCtx *q, QueryNode *lx) {
  Trie *t = q->sctx->spec->terms;
  LexRangeCtx ctx = {.q = q, .opts = &lx->opts};

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  rune *begin = NULL, *end = NULL;
  size_t nbegin, nend;
  if (lx->lxrng.begin) {
    begin = strToFoldedRunes(lx->lxrng.begin, &nbegin);
  }
  if (lx->lxrng.end) {
    end = strToFoldedRunes(lx->lxrng.end, &nend);
  }

  TrieNode_IterateRange(t->root, begin, begin ? nbegin : -1, lx->lxrng.includeBegin, end,
                        end ? nend : -1, lx->lxrng.includeEnd, runeIterCb, &ctx);
  rm_free(begin);
  rm_free(end);
  if (!ctx.its || ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return NewUnionIterator(ctx.its, ctx.nits, 1, lx->opts.weight, QN_LEXRANGE, NULL, q->config);
  }
}

static IndexIterator *Query_EvalFuzzyNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_FUZZY, "query node type should be fuzzy");

  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, qn->pfx.tok.str, qn->pfx.tok.len, qn->fz.maxDist, 0, &qn->opts);
}

static IndexIterator *Query_EvalPhraseNode(QueryEvalCtx *q, QueryNode *qn) {
  QueryPhraseNode *node = &qn->pn;
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (QueryNode_NumChildren(qn) == 1) {
    qn->children[0]->opts.fieldMask &= qn->opts.fieldMask;
    return Query_EvalNode(q, qn->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(QueryNode_NumChildren(qn), sizeof(IndexIterator *));
  for (size_t ii = 0; ii < QueryNode_NumChildren(qn); ++ii) {
    qn->children[ii]->opts.fieldMask &= qn->opts.fieldMask;
    iters[ii] = Query_EvalNode(q, qn->children[ii]);
  }
  IndexIterator *ret;

  if (node->exact) {
    ret = NewIntersectIterator(iters, QueryNode_NumChildren(qn), q->docTable,
                              EFFECTIVE_FIELDMASK(q, qn), 0, 1, qn->opts.weight);
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

    ret = NewIntersectIterator(iters, QueryNode_NumChildren(qn), q->docTable,
                              EFFECTIVE_FIELDMASK(q, qn), slop, inOrder, qn->opts.weight);
  }
  return ret;
}

static IndexIterator *Query_EvalWildcardNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_WILDCARD || !q->docTable) {
    return NULL;
  }

  return NewWildcardIterator(q->docTable->maxDocId, q->docTable->size);
}

static IndexIterator *Query_EvalNotNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_NOT) {
    return NULL;
  }

  return NewNotIterator(QueryNode_NumChildren(qn) ? Query_EvalNode(q, qn->children[0]) : NULL,
                        q->docTable->maxDocId, qn->opts.weight, q->sctx->timeout);
}

static IndexIterator *Query_EvalOptionalNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_OPTIONAL) {
    return NULL;
  }

  return NewOptionalIterator(Query_EvalNode(q, qn->children[0]),
                             q->docTable->maxDocId, qn->opts.weight);
}

static IndexIterator *Query_EvalNumericNode(QueryEvalCtx *q, QueryNode *node) {
  const FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, node->nn.nf->fieldName, strlen(node->nn.nf->fieldName));
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    return NULL;
  }

  FieldIndexFilterContext filterCtx = {.fieldIndex = fs->index, .predicate = FIELD_EXPIRATION_DEFAULT};
  return NewNumericFilterIterator(q->sctx, node->nn.nf, q->conc, INDEXFLD_T_NUMERIC, q->config, &filterCtx);
}

static IndexIterator *Query_EvalGeofilterNode(QueryEvalCtx *q, QueryNode *node,
                                              double weight) {

  if (!GeoFilter_Validate(node->gn.gf, q->status)) {
    return NULL;
  }

  const FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, node->gn.gf->property, strlen(node->gn.gf->property));
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_GEO)) {
    return NULL;
  }
  return NewGeoRangeIterator(q->sctx, node->gn.gf, q->conc, q->config, fs->index);
}

static IndexIterator *Query_EvalGeometryNode(QueryEvalCtx *q, QueryNode *node) {

  const FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, node->gmn.geomq->attr, strlen(node->gmn.geomq->attr));
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_GEOMETRY)) {
    return NULL;
  }
  const GeometryIndex *index = OpenGeometryIndex(q->sctx->redisCtx, q->sctx->spec, NULL, fs);
  if (!index) {
    return NULL;
  }
  const GeometryApi *api = GeometryApi_Get(index);
  const GeometryQuery *gq = node->gmn.geomq;
  RedisModuleString *errMsg;
  FieldIndexFilterContext filterCtx = {.fieldIndex = fs->index, .predicate = FIELD_EXPIRATION_DEFAULT};
  IndexIterator *ret = api->query(q->sctx->spec, &filterCtx, index, gq->query_type, gq->format, gq->str, gq->str_len, &errMsg);
  if (ret == NULL) {
    QueryError_SetErrorFmt(q->status, QUERY_EBADVAL, "Error querying geoshape index: %s",
                           RedisModule_StringPtrLen(errMsg, NULL));
    RedisModule_FreeString(NULL, errMsg);
  }
  return ret;
}


static IndexIterator *Query_EvalVectorNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_VECTOR) {
    return NULL;
  }
  const FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, qn->vn.vq->property, strlen(qn->vn.vq->property));
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_VECTOR)) {
    return NULL;
  }

  if (qn->opts.distField) {
    if (qn->vn.vq->scoreField) {
      // Since the KNN syntax allows specifying the distance field in two ways (...=>[KNN ... AS <dist_field>] and
      // ...=>[KNN ...]=>{$YIELD_DISTANCE_AS:<dist_field>), we validate that we got it only once.
      char default_score_field[strlen(qn->vn.vq->property) + 9];  // buffer for __<field>_score
      sprintf(default_score_field, "__%s_score", qn->vn.vq->property);
      // If the saved score field is NOT the default one, we return an error, otherwise, just override it.
      if (strcasecmp(qn->vn.vq->scoreField, default_score_field) != 0) {
        QueryError_SetErrorFmt(q->status, QUERY_EDUPFIELD,
                               "Distance field was specified twice for vector query: %s and %s",
                               qn->vn.vq->scoreField, qn->opts.distField);
        return NULL;
      }
      rm_free(qn->vn.vq->scoreField);
    }
    qn->vn.vq->scoreField = qn->opts.distField; // move ownership
    qn->opts.distField = NULL;
  }

  // Add the score field name to the ast score field names array.
  // This function creates the array if it's the first name, and ensure its size is sufficient.
  size_t idx = -1;
  if (qn->vn.vq->scoreField) {
    idx = addMetricRequest(q, qn->vn.vq->scoreField, NULL);
  }
  IndexIterator *child_it = NULL;
  if (QueryNode_NumChildren(qn) > 0) {
    RedisModule_Assert(QueryNode_NumChildren(qn) == 1);
    child_it = Query_EvalNode(q, qn->children[0]);
    // If child iterator is in valid or empty, the hybrid iterator is empty as well.
    if (child_it == NULL) {
      return NULL;
    }
  }
  IndexIterator *it = NewVectorIterator(q, qn->vn.vq, child_it, fs->index);
  // If iterator was created successfully, and we have a metric to yield, update the
  // relevant position in the metricRequests ptr array to the iterator's RLookup key ptr.
  if (it && qn->vn.vq->scoreField) {
    array_ensure_at(q->metricRequestsP, idx, MetricRequest)->key_ptr = &it->ownKey;
  }
  if (it == NULL && child_it != NULL) {
    child_it->Free(child_it);
  }
  return it;
}

static IndexIterator *Query_EvalIdFilterNode(QueryEvalCtx *q, QueryIdFilterNode *node) {
  return NewIdListIterator(node->ids, node->len, 1);
}

static IndexIterator *Query_EvalUnionNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_UNION) {
    return NULL;
  }

  // a union stage with one child is the same as the child, so we just return it
  if (QueryNode_NumChildren(qn) == 1) {
    return Query_EvalNode(q, qn->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(QueryNode_NumChildren(qn), sizeof(IndexIterator *));
  int n = 0;
  for (size_t i = 0; i < QueryNode_NumChildren(qn); ++i) {
    qn->children[i]->opts.fieldMask &= qn->opts.fieldMask;
    IndexIterator *it = Query_EvalNode(q, qn->children[i]);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    rm_free(iters);
    return NULL;
  }

  if (n == 1) {
    IndexIterator *ret = iters[0];
    rm_free(iters);
    return ret;
  }

  IndexIterator *ret = NewUnionIterator(iters, n, 0, qn->opts.weight, QN_UNION, NULL, q->config);
  return ret;
}

typedef IndexIterator **IndexIteratorArray;

static IndexIterator *Query_EvalTagLexRangeNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                                IndexIteratorArray *iterout, double weight) {
  TrieMap *t = idx->values;
  LexRangeCtx ctx = {.q = q, .opts = &qn->opts, .weight = weight};

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  const char *begin = qn->lxrng.begin, *end = qn->lxrng.end;
  int nbegin = begin ? strlen(begin) : -1, nend = end ? strlen(end) : -1;

  TrieMap_IterateRange(t, begin, nbegin, qn->lxrng.includeBegin, end, nend, qn->lxrng.includeEnd,
                       rangeIterCbStrs, &ctx);
  if (ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return NewUnionIterator(ctx.its, ctx.nits, 1, qn->opts.weight, QN_LEXRANGE, NULL, q->config);
  }
}

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static IndexIterator *Query_EvalTagPrefixNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                              IndexIteratorArray *iterout, double weight,
                                              int withSuffixTrie) {
  RSToken *tok = &qn->pfx.tok;
  if (qn->type != QN_PREFIX) {
    return NULL;
  }

  // we allow a minimum of 2 letters in the prefix by default (configurable)
  if (tok->len < q->config->minTermPrefix) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  if (!qn->pfx.suffix || !withSuffixTrie) {    // prefix query or no suffix triemap, use bruteforce
    TrieMapIterator *it = TrieMap_Iterate(idx->values, tok->str, tok->len);
    if (!it) {
      rm_free(its);
      return NULL;
    }
    TrieMapIterator_SetTimeout(it, q->sctx->timeout);
    TrieMapIterator_NextFunc nextFunc = TrieMapIterator_Next;

    if (qn->pfx.suffix) {
      nextFunc = TrieMapIterator_NextContains;
      if (qn->pfx.prefix) { // contains mode
        it->mode = TM_CONTAINS_MODE;
      } else {
        it->mode = TM_SUFFIX_MODE;
      }
    }

    // an upper limit on the number of expansions is enforced to avoid stuff like "*"
    char *s;
    tm_len_t sl;
    void *ptr;

    // Find all completions of the prefix
    while (nextFunc(it, &s, &sl, &ptr) &&
          (itsSz < q->config->maxPrefixExpansions)) {
      IndexIterator *ret = TagIndex_OpenReader(idx, q->sctx->spec, s, sl, 1);
      if (!ret) continue;

      // Add the reader to the iterator array
      its[itsSz++] = ret;
      if (itsSz == itsCap) {
        itsCap *= 2;
        its = rm_realloc(its, itsCap * sizeof(*its));
      }
    }
    TrieMapIterator_Free(it);
  } else {    // TAG field has suffix triemap
    arrayof(char**) arr = GetList_SuffixTrieMap(idx->suffix, tok->str, tok->len,
                                                qn->pfx.prefix, q->sctx->timeout);
    if (!arr) {
      rm_free(its);
      return NULL;
    }
    for (int i = 0; i < array_len(arr); ++i) {
      size_t iarrlen = array_len(arr);
      for (int j = 0; j < array_len(arr[i]); ++j) {
        size_t jarrlen = array_len(arr[i]);
        if (itsSz >= q->config->maxPrefixExpansions) {
          break;
        }
        IndexIterator *ret = TagIndex_OpenReader(idx, q->sctx->spec, arr[i][j], strlen(arr[i][j]), 1);
        if (!ret) continue;

        // Add the reader to the iterator array
        its[itsSz++] = ret;
        if (itsSz == itsCap) {
          itsCap *= 2;
          its = rm_realloc(its, itsCap * sizeof(*its));
        }
      }
    }
    array_free(arr);
  }

  if (itsSz < 2) {
    IndexIterator *iter = itsSz ? its[0] : NULL;
    rm_free(its);
    return iter;
  }

  *iterout = array_ensure_append(*iterout, its, itsSz, IndexIterator *);
  return NewUnionIterator(its, itsSz, 1, weight, QN_PREFIX, qn->pfx.tok.str, q->config);
}

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static IndexIterator *Query_EvalTagWildcardNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                              IndexIteratorArray *iterout, double weight) {
  if (qn->type != QN_WILDCARD_QUERY) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  RSToken *tok = &qn->verb.tok;
  tok->len = Wildcard_RemoveEscape(tok->str, tok->len);

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  bool fallbackBruteForce = false;
  if (idx->suffix) {
    // with suffix
    arrayof(char*) arr = GetList_SuffixTrieMap_Wildcard(idx->suffix, tok->str, tok->len,
                                                        q->sctx->timeout, q->config->maxPrefixExpansions);
    if (!arr) {
      // No matching terms
      rm_free(its);
      return NULL;
    } else if (arr == BAD_POINTER) {
      // The wildcard pattern does not include tokens that can be used with suffix trie
      fallbackBruteForce = true;
    } else {
      for (int i = 0; i < array_len(arr); ++i) {
        if (itsSz >= q->config->maxPrefixExpansions) {
          break;
        }
        IndexIterator *ret = TagIndex_OpenReader(idx, q->sctx->spec, arr[i], strlen(arr[i]), 1);
        if (!ret) continue;

          // Add the reader to the iterator array
        its[itsSz++] = ret;
        if (itsSz == itsCap) {
          itsCap *= 2;
          its = rm_realloc(its, itsCap * sizeof(*its));
        }
      }
      array_free(arr);
    }
  }

  if (!idx->suffix || fallbackBruteForce) {
    // brute force wildcard query
    TrieMapIterator *it = TrieMap_Iterate(idx->values, tok->str, tok->len);
    TrieMapIterator_SetTimeout(it, q->sctx->timeout);
    // If there is no '*`, the length is known which can be used for optimization
    it->mode = strchr(tok->str, '*') ? TM_WILDCARD_MODE : TM_WILDCARD_FIXED_LEN_MODE;

    char *s;
    tm_len_t sl;
    void *ptr;

    // Find all completions of the prefix
    while (TrieMapIterator_NextWildcard(it, &s, &sl, &ptr) &&
          (itsSz < q->config->maxPrefixExpansions)) {
      IndexIterator *ret = TagIndex_OpenReader(idx, q->sctx->spec, s, sl, 1);
      if (!ret) continue;

      // Add the reader to the iterator array
      its[itsSz++] = ret;
      if (itsSz == itsCap) {
        itsCap *= 2;
        its = rm_realloc(its, itsCap * sizeof(*its));
      }
    }
    TrieMapIterator_Free(it);
  } else

  if (itsSz == 0) {
    rm_free(its);
    return NULL;
  }
  if (itsSz == 1) {
    // TODO:
    IndexIterator *iter = its[0];
    rm_free(its);
    return iter;
  }

  *iterout = array_ensure_append(*iterout, its, itsSz, IndexIterator *);
  return NewUnionIterator(its, itsSz, 1, weight, QN_WILDCARD_QUERY, qn->pfx.tok.str, q->config);
}

static void tag_strtolower(char *str, size_t *len, int caseSensitive) {
  char *p = str;
  if (caseSensitive) {
    while (*p) {
      if (*p == '\\' && (ispunct(*(p+1)) || isspace(*(p+1)))) {
        ++p;
        --*len;
      }
      *str++ = *p++;
      }
  } else {
    while (*p) {
      if (*p == '\\' && (ispunct(*(p+1)) || isspace(*(p+1)))) {
        ++p;
        --*len;
      }
      *str++ = tolower(*p++);
    }
  }
  *str = '\0';
}

static IndexIterator *query_EvalSingleTagNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *n,
                                              IndexIteratorArray *iterout, double weight,
                                              const FieldSpec *fs) {
  IndexIterator *ret = NULL;
  if (n->tn.str) {
    tag_strtolower(n->tn.str, &n->tn.len, fs->tagOpts.tagFlags & TagField_CaseSensitive);
  }

  switch (n->type) {
    case QN_TOKEN: {
      ret = TagIndex_OpenReader(idx, q->sctx->spec, n->tn.str, n->tn.len, weight);
      break;
    }
    case QN_PREFIX:
      return Query_EvalTagPrefixNode(q, idx, n, iterout, weight, FieldSpec_HasSuffixTrie(fs));

    case QN_WILDCARD_QUERY:
      return Query_EvalTagWildcardNode(q, idx, n, iterout, weight);

    case QN_LEXRANGE:
      return Query_EvalTagLexRangeNode(q, idx, n, iterout, weight);

    case QN_PHRASE: {
      char *terms[QueryNode_NumChildren(n)];
      for (size_t i = 0; i < QueryNode_NumChildren(n); ++i) {
        if (n->children[i]->type == QN_TOKEN) {
          terms[i] = n->children[i]->tn.str;
        } else {
          terms[i] = "";
        }
      }

      sds s = sdsjoin(terms, QueryNode_NumChildren(n), " ");

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
  const FieldSpec *fs = IndexSpec_GetField(q->sctx->spec, node->fieldName, strlen(node->fieldName));
  if (!fs) {
    return NULL;
  }
  RedisModuleString *kstr = IndexSpec_GetFormattedKey(q->sctx->spec, fs, INDEXFLD_T_TAG);
  TagIndex *idx = TagIndex_Open(q->sctx, kstr, 0, &k);

  IndexIterator **total_its = NULL;
  IndexIterator *ret = NULL;
 
  if (!idx) {
    // There are no documents to traverse.
    goto done;
  }
  if (QueryNode_NumChildren(qn) == 1) {
    // a union stage with one child is the same as the child, so we just return it
    ret = query_EvalSingleTagNode(q, idx, qn->children[0], &total_its, qn->opts.weight, fs);
    if (ret) {
      if (q->conc) {
        TagIndex_RegisterConcurrentIterators(idx, q->conc, (array_t *)total_its);
        k = NULL;  // we passed ownershit
      } else {
        array_free(total_its);
      }
    }
    goto done;
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(QueryNode_NumChildren(qn), sizeof(IndexIterator *));
  size_t n = 0;
  for (size_t i = 0; i < QueryNode_NumChildren(qn); i++) {
    IndexIterator *it =
        query_EvalSingleTagNode(q, idx, qn->children[i], &total_its, qn->opts.weight, fs);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    rm_free(iters);
    goto done;
  }

  if (total_its) {
    if (q->conc) {
      TagIndex_RegisterConcurrentIterators(idx, q->conc, (array_t *)total_its);
      k = NULL;  // we passed ownershit
    } else {
      array_free(total_its);
    }
  }

  ret = NewUnionIterator(iters, n, 0, qn->opts.weight, QN_TAG, NULL, q->config);

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return ret;
}

static IndexIterator *Query_EvalMissingNode(QueryEvalCtx *q, QueryNode *qn) {
  const FieldSpec *fs = IndexSpec_GetField(q->sctx->spec, qn->miss.fieldName, qn->miss.len);
  if (!fs) {
    // Field does not exist
    return NULL;
  }
  if (!FieldSpec_IndexesMissing(fs)) {
    QueryError_SetErrorFmt(q->status, QUERY_EMISSING,
                           "'ismissing' requires field '%s' to be defined with '" SPEC_INDEXMISSING_STR "'",
                           qn->miss.fieldName);
    return NULL;
  }

  // Get the InvertedIndex corresponding to the queried field.
  InvertedIndex *missingII = dictFetchValue(q->sctx->spec->missingFieldDict, fs->name);

  if (!missingII) {
    // There are no missing values for this field.
    return NULL;
  }

  // Create a reader for the missing values InvertedIndex.
  FieldIndexFilterContext filterCtx = {.fieldIndex = fs->index, .predicate = FIELD_EXPIRATION_DEFAULT};
  IndexReader *ir = NewMissingIndexReader(missingII, q->sctx->spec, &filterCtx);

  return NewReadIterator(ir);
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
    case QN_PREFIX:
      return Query_EvalPrefixNode(q, n);
    case QN_LEXRANGE:
      return Query_EvalLexRangeNode(q, n);
    case QN_FUZZY:
      return Query_EvalFuzzyNode(q, n);
    case QN_NUMERIC:
      return Query_EvalNumericNode(q, n);
    case QN_OPTIONAL:
      return Query_EvalOptionalNode(q, n);
    case QN_GEO:
      return Query_EvalGeofilterNode(q, n, n->opts.weight);
    case QN_VECTOR:
      return Query_EvalVectorNode(q, n);
    case QN_IDS:
      return Query_EvalIdFilterNode(q, &n->fn);
    case QN_WILDCARD:
      return Query_EvalWildcardNode(q, n);
    case QN_WILDCARD_QUERY:
      return Query_EvalWildcardQueryNode(q,n);
    case QN_GEOMETRY:
      return Query_EvalGeometryNode(q, n);
    case QN_NULL:
      return NewEmptyIterator();
    case QN_MISSING:
      return Query_EvalMissingNode(q, n);
  }

  return NULL;
}

int QAST_Parse(QueryAST *dst, const RedisSearchCtx *sctx, const RSSearchOptions *sopts,
               const char *qstr, size_t len, unsigned int dialectVersion, QueryError *status) {
  if (!dst->query) {
    dst->query = rm_strndup(qstr, len);
    dst->nquery = len;
  }
  QueryParseCtx qpCtx = {// force multiline
                         .raw = dst->query,
                         .len = dst->nquery,
                         .sctx = (RedisSearchCtx *)sctx,
                         .opts = sopts,
                         .status = status,
#ifdef PARSER_DEBUG
                         .trace_log = NULL
#endif
  };
  if (dialectVersion >= 2)
    dst->root = RSQuery_ParseRaw_v2(&qpCtx);
  else
    dst->root = RSQuery_ParseRaw_v1(&qpCtx);

#ifdef PARSER_DEBUG
  if (qpCtx.trace_log != NULL) {
    fclose(qpCtx.trace_log);
  }
#endif
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
  dst->numParams = qpCtx.numParams;
  return REDISMODULE_OK;
}

IndexIterator *QAST_Iterate(QueryAST *qast, const RSSearchOptions *opts, RedisSearchCtx *sctx,
                            ConcurrentSearchCtx *conc, uint32_t reqflags, QueryError *status) {
  QueryEvalCtx qectx = {
      .conc = conc,
      .opts = opts,
      .numTokens = qast->numTokens,
      .docTable = &sctx->spec->docs,
      .sctx = sctx,
      .status = status,
      .metricRequestsP = &qast->metricRequests,
      .reqFlags = reqflags,
      .config = &qast->config,
  };
  IndexIterator *root = Query_EvalNode(&qectx, qast->root);
  if (!root) {
    // Return the dummy iterator
    root = NewEmptyIterator();
  }
  return root;
}

void QAST_Destroy(QueryAST *q) {
  QueryNode_Free(q->root);
  q->root = NULL;
  array_free(q->metricRequests);
  q->metricRequests = NULL;
  q->numTokens = 0;
  q->numParams = 0;
  rm_free(q->query);
  q->nquery = 0;
  q->query = NULL;
}

int QAST_Expand(QueryAST *q, const char *expander, RSSearchOptions *opts, RedisSearchCtx *sctx,
                QueryError *status) {
  if (!q->root) {
    return REDISMODULE_OK;
  }
  RSQueryExpanderCtx expCtx = {
      .qast = q, .language = opts->language, .handle = sctx, .status = status};

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
}

int QAST_EvalParams(QueryAST *q, RSSearchOptions *opts, QueryError *status) {
  if (!q || !q->root || q->numParams == 0)
    return REDISMODULE_OK;
  QueryNode_EvalParams(opts->params, q->root, status);
  return REDISMODULE_OK;
}

int QueryNode_EvalParams(dict *params, QueryNode *n, QueryError *status) {
  int withChildren = 1;
  int res = REDISMODULE_OK;
  switch(n->type) {
    case QN_VECTOR:
      res = VectorQuery_EvalParams(params, n, status);
      break;
    case QN_GEO:
    case QN_TOKEN:
    case QN_NUMERIC:
    case QN_TAG:
    case QN_PHRASE:
    case QN_NOT:
    case QN_PREFIX:
    case QN_LEXRANGE:
    case QN_FUZZY:
    case QN_OPTIONAL:
    case QN_IDS:
    case QN_WILDCARD:
    case QN_WILDCARD_QUERY:
    case QN_GEOMETRY:
      res = QueryNode_EvalParamsCommon(params, n, status);
      break;
    case QN_UNION:
      // no immediately owned params to resolve
      assert(n->params == NULL);
      break;
    case QN_NULL:
    case QN_MISSING:
      withChildren = 0;
      break;
  }
  // Handle children
  if (withChildren && res == REDISMODULE_OK) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
      res = QueryNode_EvalParams(params, n->children[ii], status);
      if (res == REDISMODULE_ERR)
        break;
    }
  }
  return res;
}

static int QueryNode_CheckAllowSlopAndInorder(QueryNode *qn, const IndexSpec *spec, bool atTopLevel, QueryError *status) {
  // Need to check when slop/inorder are locally overridden at query node level, or at query top-level
  if(atTopLevel || qn->opts.maxSlop >= 0 || (qn->opts.flags & QueryNode_OverriddenInOrder)) {
    // Check only fields that are used in this query node (either specific fields or all fields)
    return IndexSpec_CheckAllowSlopAndInorder(spec, qn->opts.fieldMask, status);
  } else {
    return 1;
  }
}

static inline bool QueryNode_DoesIndexEmpty(QueryNode *n, IndexSpec *spec, RSSearchOptions *opts) {
  if (opts->flags & QueryNode_IsTag) {
    return opts->flags & QueryNode_IndexesEmpty;
  }

  // TEXT field (probably)
  bool empty_text = n->opts.fieldMask == RS_FIELDMASK_ALL;
  if (!empty_text) {
    // Check if there is a field from the field mask that indexes empty. If not,
    // we throw an error.
    arrayof(FieldSpec *) fields = IndexSpec_GetFieldsByMask(spec, n->opts.fieldMask);
    if (array_len(fields) == 0) {
      // Not a TEXT field. We don't want to throw an error, for backward compatibility.
      array_free(fields);
      return true;
    }
    array_foreach(fields, fs, {
      if (FieldSpec_IndexesEmpty(fs)) {
        empty_text = true;
        break;
      }
    });
    array_free(fields);
  }
  return empty_text;
}

// If the token is of an empty string, and the searched field doesn't index
// empty strings, we should return an error
static inline bool QueryNode_ValidateToken(QueryNode *n, IndexSpec *spec, RSSearchOptions *opts, QueryError *status) {
  if (n->tn.len == 0 && n->tn.str && !strcmp(n->tn.str, "") && !QueryNode_DoesIndexEmpty(n, spec, opts)) {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "Use `%s` in field creation in order to index and query for empty strings", SPEC_INDEXEMPTY_STR);
    return false;
  }
  return true;
}

static int QueryNode_CheckIsValid(QueryNode *n, IndexSpec *spec, RSSearchOptions *opts, QueryError *status) {
  int withChildren = 1;
  int res = REDISMODULE_OK;
  switch(n->type) {
    case QN_PHRASE:
      {
        if (isSpecJson(spec) && (spec->flags & Index_HasUndefinedOrder)){
          bool atTopLevel = opts->slop >=0 || (opts->flags & Search_InOrder);
          if (!QueryNode_CheckAllowSlopAndInorder(n, spec, atTopLevel, status)) {
            res = REDISMODULE_ERR;
          }
        }
      }
      break;
    case QN_NULL:
    case QN_MISSING:
      withChildren = 0;
      break;
    case QN_TAG:
      {
        opts->flags |= QueryNode_IsTag;
        const FieldSpec *fs = IndexSpec_GetField(spec, n->tag.fieldName, n->tag.len);
        if (fs && FieldSpec_IndexesEmpty(fs)) {
          opts->flags |= QueryNode_IndexesEmpty;
        }
      }
      break;
    case QN_UNION:
    case QN_TOKEN:
      {
        if (spec->flags & Index_HasNonEmpty) {
          // We don't validate this if there is no TEXT\TAG field that does not
          // index empty values.
          QueryNode_ValidateToken(n, spec, opts, status);
        }
      }
      break;
    case QN_NUMERIC:
    case QN_NOT:
    case QN_OPTIONAL:
    case QN_GEO:
    case QN_PREFIX:
    case QN_IDS:
    case QN_WILDCARD:
    case QN_WILDCARD_QUERY:
    case QN_FUZZY:
    case QN_LEXRANGE:
    case QN_VECTOR:
    case QN_GEOMETRY:
      break;
  }
  // Handle children
  if (withChildren && res == REDISMODULE_OK) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
      res = QueryNode_CheckIsValid(n->children[ii], spec, opts, status);
      if (res == REDISMODULE_ERR)
        break;
    }
  }
  return res;
}

// Checks whether query nodes are valid
// Currently Phrase nodes are checked whether slop/inorder are allowed
int QAST_CheckIsValid(QueryAST *q, IndexSpec *spec, RSSearchOptions *opts, QueryError *status) {
  if (!q || !q->root ||
      !(spec->flags & Index_HasNonEmpty) &&
      (!isSpecJson(spec) || !(spec->flags & Index_HasUndefinedOrder))
  ) {
    return REDISMODULE_OK;
  }
  return QueryNode_CheckIsValid(q->root, spec, opts, status);
}

/* Set the field mask recursively on a query node. This is called by the parser to handle
 * situations like @foo:(bar baz|gaz), where a complex tree is being applied a field mask */
void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask) {
  if (!n) return;
  n->opts.fieldMask &= mask;
  for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
    QueryNode_SetFieldMask(n->children[ii], mask);
  }
}

void QueryNode_AddChildren(QueryNode *n, QueryNode **children, size_t nchildren) {
  if (n->type == QN_TAG) {
    for (size_t ii = 0; ii < nchildren; ++ii) {
      QueryNode *child = children[ii];
      if (child->type == QN_TOKEN || child->type == QN_PHRASE ||
          child->type == QN_PREFIX || child->type == QN_LEXRANGE ||
          child->type == QN_WILDCARD_QUERY) {
        n->children = array_ensure_append(n->children, children + ii, 1, QueryNode *);
        for(size_t jj = 0; jj < QueryNode_NumParams(child); ++jj) {
          Param *p = child->params + jj;
          p->type = PARAM_TERM_CASE;
        }
      }
    }
  } else {
    array_ensure_append(n->children, children, nchildren, QueryNode *);
  }
}

void QueryNode_AddChild(QueryNode *n, QueryNode *ch) {
  QueryNode_AddChildren(n, &ch, 1);
}

void QueryNode_ClearChildren(QueryNode *n, int shouldFree) {
  if (shouldFree) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
      QueryNode_Free(n->children[ii]);
    }
  }
  if (QueryNode_NumChildren(n)) {
    array_clear(n->children);
  }
}

int QueryNode_EvalParamsCommon(dict *params, QueryNode *node, QueryError *status) {
  if (node->params) {
    for (size_t i = 0; i < QueryNode_NumParams(node); i++) {
      int res = QueryParam_Resolve(&node->params[i], params, status);
      if (res < 0)
        return REDISMODULE_ERR;
      // If parameter's value is a number, don't expand the node.
      if (res == 2) {
        node->opts.flags |= QueryNode_Verbatim;
      }
    }
  }
  return REDISMODULE_OK;
}

static sds doPad(sds s, int len) {
  if (!len) return s;

  char buf[len * 2 + 1];
  memset(buf, ' ', len * 2);
  buf[len * 2] = 0;
  return sdscat(s, buf);
}

static sds QueryNode_DumpSds(sds s, const IndexSpec *spec, const QueryNode *qs, int depth);

static sds QueryNode_DumpChildren(sds s, const IndexSpec *spec, const QueryNode *qs, int depth);

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
          const char *f = IndexSpec_GetFieldNameByBit(spec, bit);
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
      for (size_t ii = 0; ii < QueryNode_NumChildren(qs); ++ii) {
        s = QueryNode_DumpSds(s, spec, qs->children[ii], depth + 1);
      }
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;
    case QN_TOKEN:
      s = sdscatprintf(s, "%s%s", (qs->tn.len > 0) ? qs->tn.str : "\"\"", qs->tn.expanded ? "(expanded)" : "");
      if (qs->opts.weight != 1) {
        s = sdscatprintf(s, " => {$weight: %g;}", qs->opts.weight);
      }
      s = sdscat(s, "\n");
      return s;

    case QN_PREFIX:
      if(qs->pfx.prefix && qs->pfx.suffix) {
        s = sdscatprintf(s, "INFIX{*%s*}", (char *)qs->pfx.tok.str);
      } else if (qs->pfx.suffix) {
        s = sdscatprintf(s, "SUFFIX{*%s}", (char *)qs->pfx.tok.str);
      } else {
        s = sdscatprintf(s, "PREFIX{%s*}", (char *)qs->pfx.tok.str);
      }
      break;

    case QN_LEXRANGE:
      s = sdscatprintf(s, "LEXRANGE{%s...%s}", qs->lxrng.begin ? qs->lxrng.begin : "",
                       qs->lxrng.end ? qs->lxrng.end : "");
      break;

    case QN_NOT:
      s = sdscat(s, "NOT{\n");
      s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;

    case QN_OPTIONAL:
      s = sdscat(s, "OPTIONAL{\n");
      s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;

    case QN_NUMERIC: {
      const NumericFilter *f = qs->nn.nf;
      s = sdscatprintf(s, "NUMERIC {%f %s @%s %s %f}", f->min, f->inclusiveMin ? "<=" : "<",
                       f->fieldName, f->inclusiveMax ? "<=" : "<", f->max);
    } break;
    case QN_UNION:
      s = sdscat(s, "UNION {\n");
      s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;
    case QN_TAG:
      s = sdscatprintf(s, "TAG:@%.*s {\n", (int)qs->tag.len, qs->tag.fieldName);
      s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;
    case QN_GEO:

      s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s}", qs->gn.gf->property, qs->gn.gf->lon,
                       qs->gn.gf->lat, qs->gn.gf->radius,
                       GeoDistance_ToString(qs->gn.gf->unitType));
      break;
    case QN_IDS:

      s = sdscat(s, "IDS {");
      for (int i = 0; i < qs->fn.len; i++) {
        s = sdscatprintf(s, "%llu,", (unsigned long long)qs->fn.ids[i]);
      }
      s = sdscat(s, "}");
      break;
    case QN_VECTOR:
      s = sdscat(s, "VECTOR {");
      if (QueryNode_NumChildren(qs) > 0) {
        s = sdscat(s, "\n");
        s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
        s = doPad(s, depth);
        s = sdscat(s, "} => {");
      }
      switch (qs->vn.vq->type) {
        case VECSIM_QT_KNN: {
          s = sdscatprintf(s, "K=%zu nearest vectors to ", qs->vn.vq->knn.k);
          // This loop finds the vector param name.
          for (size_t i = 0; i < array_len(qs->params); i++) {
            if (qs->params[i].type != PARAM_NONE &&
                qs->params[i].target == &qs->vn.vq->knn.vector) {
              s = sdscatprintf(s, "`$%s` ", qs->params[i].name);
              break;
            }
          }
          break;
        }
        case VECSIM_QT_RANGE: {
          s = sdscatprintf(s, "Vectors that are within %g distance radius from",
                           qs->vn.vq->range.radius);
          // This loop finds the vector param name.
          for (size_t i = 0; i < array_len(qs->params); i++) {
            if (qs->params[i].type != PARAM_NONE &&
                qs->params[i].target == &qs->vn.vq->range.vector) {
              s = sdscatprintf(s, " `$%s` ", qs->params[i].name);
              break;
            }
          }
          break;
        }
      } // switch (qs->vn.vq->type). Next is a common part for both types.
      s = sdscatprintf(s, "in vector index associated with field @%s", qs->vn.vq->property);
      for (size_t i = 0; i < array_len(qs->vn.vq->params.params); i++) {
        s = sdscatprintf(s, ", %s = ", qs->vn.vq->params.params[i].name);
        s = sdscatlen(s, qs->vn.vq->params.params[i].value, qs->vn.vq->params.params[i].valLen);
      }
      if (qs->vn.vq->scoreField) {
        s = sdscatprintf(s, ", yields distance as `%s`", qs->vn.vq->scoreField);
      }
      s = sdscat(s, "}"); // end of VECTOR
      break;
    case QN_WILDCARD:
      s = sdscat(s, "<WILDCARD>");
      break;
    case QN_FUZZY:
      s = sdscatprintf(s, "FUZZY{%s}", qs->fz.tok.str);
      break;
    case QN_WILDCARD_QUERY:
      s = sdscatprintf(s, "WILDCARD{%s}", qs->verb.tok.str);
      break;
    case QN_NULL:
      s = sdscat(s, "<empty>");
      break;
    case QN_GEOMETRY:
      s = sdscatprintf(s, "GEOSHAPE{%d %s}", qs->gmn.geomq->query_type, qs->gmn.geomq->str);
      break;
    case QN_MISSING:
      s = sdscatprintf(s, "ISMISSING{%.*s}", (int)qs->miss.len, qs->miss.fieldName);
      break;
  }

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

static sds QueryNode_DumpChildren(sds s, const IndexSpec *spec, const QueryNode *qs, int depth) {
  for (size_t ii = 0; ii < QueryNode_NumChildren(qs); ++ii) {
    s = QueryNode_DumpSds(s, spec, qs->children[ii], depth);
  }
  return s;
}

/* Return a string representation of the query parse tree. The string should be freed by the
 * caller
 * Assumes that the spec is guarded by the GIL or its own lock (read or write)
 */
char *QAST_DumpExplain(const QueryAST *q, const IndexSpec *spec) {
  // empty query
  if (!q || !q->root) {
    return rm_strdup("NULL");
  }

  sds s = QueryNode_DumpSds(sdsnew(""), spec, q->root, 0);
  char *ret = rm_strndup(s, sdslen(s));
  sdsfree(s);
  return ret;
}

// Debugging function to print the query parse tree
void QAST_Print(const QueryAST *ast, const IndexSpec *spec) {
  sds s = QueryNode_DumpSds(sdsnew(""), spec, ast->root, 0);
  sdsfree(s);
}

int QueryNode_ForEach(QueryNode *q, QueryNode_ForEachCallback callback, void *ctx, int reverse) {
#define INITIAL_ARRAY_NODE_SIZE 5
  QueryNode **nodes = array_new(QueryNode *, INITIAL_ARRAY_NODE_SIZE);
  array_append(nodes, q);
  int retVal = 1;
  while (array_len(nodes) > 0) {
    QueryNode *curr = array_pop(nodes);
    if (!callback(curr, q, ctx)) {
      retVal = 0;
      break;
    }
    if (reverse) {
      for (size_t ii = QueryNode_NumChildren(curr); ii; --ii) {
        array_append(nodes, curr->children[ii - 1]);
      }
    } else {
      for (size_t ii = 0; ii < QueryNode_NumChildren(curr); ++ii) {
        array_append(nodes, curr->children[ii]);
      }
    }
  }

  array_free(nodes);
  return retVal;
}

// Convert the query attribute into a raw vector param to be resolved by the vector iterator
// down the road. return 0 in case of an unrecognized parameter.
static int QueryVectorNode_ApplyAttribute(VectorQuery *vq, QueryAttribute *attr) {
  if (STR_EQCASE(attr->name, attr->namelen, VECSIM_EFRUNTIME) ||
      STR_EQCASE(attr->name, attr->namelen, VECSIM_EPSILON) ||
      STR_EQCASE(attr->name, attr->namelen, VECSIM_HYBRID_POLICY) ||
      STR_EQCASE(attr->name, attr->namelen, VECSIM_BATCH_SIZE)) {
    // Move ownership on the value string, so it won't get freed when releasing the QueryAttribute.
    // The name string was not copied by the parser (unlike the value) - so we copy and save it.
    VecSimRawParam param = (VecSimRawParam){ .name = rm_strndup(attr->name, attr->namelen),
                                            .nameLen = attr->namelen,
                                            .value = attr->value,
                                            .valLen = attr->vallen };
    attr->value = NULL;
    vq->params.params = array_ensure_append_1(vq->params.params, param);
    bool resolve_required = false;  // at this point, we have the actual value in hand, not the query param.
    vq->params.needResolve = array_ensure_append_1(vq->params.needResolve, resolve_required);
    return 1;
  }
  return 0;
}

static int QueryNode_ApplyAttribute(QueryNode *qn, QueryAttribute *attr, QueryError *status) {

#define MK_INVALID_VALUE()                                                         \
  QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "Invalid value (%.*s) for `%.*s`", \
                         (int)attr->vallen, attr->value, (int)attr->namelen, attr->name)

  int res = 0;

  if (attr->vallen == 0) {
    MK_INVALID_VALUE();
    return res;
  }

  // Apply slop: [-1 ... INF]
  if (STR_EQCASE(attr->name, attr->namelen, SLOP_ATTR)) {
    long long n;
    if (!ParseInteger(attr->value, &n) || n < -1) {
      MK_INVALID_VALUE();
      return res;
    }
    qn->opts.maxSlop = n;
    res = 1;

  } else if (STR_EQCASE(attr->name, attr->namelen, INORDER_ATTR)) {
    // Apply inorder: true|false
    int b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return res;
    }
    qn->opts.inOrder = b;
    qn->opts.flags |= QueryNode_OverriddenInOrder;
    res = 1;

  } else if (STR_EQCASE(attr->name, attr->namelen, WEIGHT_ATTR)) {
    // Apply weight: [0  ... INF]
    double d;
    if (!ParseDouble(attr->value, &d, 1) || d < 0) {
      MK_INVALID_VALUE();
      return res;
    }
    qn->opts.weight = d;
    res = 1;

  } else if (STR_EQCASE(attr->name, attr->namelen, PHONETIC_ATTR)) {
    // Apply phonetic: true|false
    int b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return res;
    }
    if (b) {
      qn->opts.phonetic = PHONETIC_ENABLED;  // means we specifically asked for phonetic matching
    } else {
      qn->opts.phonetic =
          PHONETIC_DISABLED;  // means we specifically asked no for phonetic matching
    }
    res = 1;
    // qn->opts.noPhonetic = PHONETIC_DEFAULT -> means no special asks regarding phonetics
    //                                          will be enable if field was declared phonetic

  } else if (STR_EQCASE(attr->name, attr->namelen, YIELD_DISTANCE_ATTR) && qn->opts.flags & QueryNode_YieldsDistance) {
    // Move ownership on the value string, so it won't get freed when releasing the QueryAttribute.
    qn->opts.distField = (char *)attr->value;
    attr->value = NULL;
    res = 1;

  } else if (qn->type == QN_VECTOR) {
    res = QueryVectorNode_ApplyAttribute(qn->vn.vq, attr);
  }

  if (!res) {
    QueryError_SetErrorFmt(status, QUERY_ENOOPTION, "Invalid attribute %.*s", (int)attr->namelen,
                           attr->name);
  }
  return res;
}

int QueryNode_ApplyAttributes(QueryNode *qn, QueryAttribute *attrs, size_t len, QueryError *status) {
  for (size_t i = 0; i < len; i++) {
    if (!QueryNode_ApplyAttribute(qn, &attrs[i], status)) {
      return 0;
    }
  }
  return 1;
}
