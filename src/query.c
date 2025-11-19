/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

#include "geo_index.h"
#include "query.h"
#include "config.h"
#include "query_error.h"
#include "redis_index.h"
#include "tokenize.h"
#include "triemap.h"
#include "util/logging.h"
#include "extension.h"
#include "ext/default.h"
#include "hiredis/sds.h"
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
#include "wildcard.h"
#include "geometry/geometry_api.h"
#include "iterators/inverted_index_iterator.h"
#include "iterators/wildcard_iterator.h"
#include "iterators/union_iterator.h"
#include "iterators/intersection_iterator.h"
#include "iterators/optional_iterator.h"
#include "iterators/not_iterator.h"
#include "iterators/idlist_iterator.h"
#include "iterators/empty_iterator.h"
#include "iterators/hybrid_reader.h"
#include "iterators/optimizer_reader.h"
#include "search_disk.h"

#ifndef STRINGIFY
#define __STRINGIFY(x) #x
#define STRINGIFY(x) __STRINGIFY(x)
#endif

#define EFFECTIVE_FIELDMASK(q_, qn_) ((qn_)->opts.fieldMask & (q)->opts->fieldmask)

static void QueryTokenNode_Free(QueryTokenNode *tn) {
  if (tn->str) rm_free(tn->str);
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
    case QN_GEOMETRY:
      QueryGeometryNode_Free(&n->gmn);
      break;
    case QN_MISSING:
    case QN_WILDCARD:
    case QN_IDS:
    case QN_TAG:
    case QN_UNION:
    case QN_NOT:
    case QN_OPTIONAL:
    case QN_NULL:
    case QN_PHRASE:
      break;
  }
  rm_free(n);
}

// Add a new metric request to the metricRequests array. Returns the index of the request
static int addMetricRequest(QueryEvalCtx *q, char *metric_name, bool isInternal) {
  MetricRequest mr = {metric_name, NULL, isInternal};
  array_ensure_append_1(*q->metricRequestsP, mr);
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
    .explicitWeight = false,
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
      s = rm_normalize(qt->s, qt->len);
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
    RS_ASSERT(qt->type == QT_PARAM_WILDCARD);
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
    char *s = rm_normalize(qt->s, qt->len);
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
    RS_ASSERT(qt->type == QT_PARAM_TERM);
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

QueryNode *NewTagNode(const FieldSpec *field) {
  QueryNode *ret = NewQueryNode(QN_TAG);
  ret->tag.fs = field;
  return ret;
}

QueryNode *NewNumericNode(QueryParam *p, const FieldSpec *fs) {
  QueryNode *ret = NewQueryNode(QN_NUMERIC);
  ret->nn.nf = p->nf;
  ret->params = p->params;
  ret->nn.nf->fieldSpec = fs;
  p->nf = NULL;
  p->params = NULL;
  rm_free(p);
  return ret;
}

QueryNode *NewGeofilterNode(QueryParam *p) {
  RS_ASSERT(p->type == QP_GEO_FILTER);
  QueryNode *ret = NewQueryNode(QN_GEO);
  // Move data and params pointers
  ret->gn.gf = p->gf;
  ret->params = p->params;
  p->gf = NULL;
  p->params = NULL;
  rm_free(p);
  return ret;
}

QueryNode *NewMissingNode(const FieldSpec *field) {
  QueryNode *ret = NewQueryNode(QN_MISSING);
  ret->miss.field = field;
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
      vq->knn.shardWindowRatio = DEFAULT_SHARD_WINDOW_RATIO;

      // Save K position so it can be modified later in the shard command.
      // NOTE: If k is given as a *parameter*:
      // 1. value->pos: position of "$"
      vq->knn.k_token_pos = value->pos;
      // 2. value->len: length of the parameter name (e.g. $k -> len=1, $k_meow -> len=6)
      // So we need to include the '$' in the token length.
      if (value->type == QT_PARAM_SIZE) {
        vq->knn.k_token_len = value->len + 1;
      } else { // k is literal
        vq->knn.k_token_len = value->len;
      }
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

void SetFilterNode(QueryAST *q, QueryNode *filterNode) {
  if (q->root == NULL || filterNode == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (q->root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator.
    q->root->children = array_ensure_prepend(q->root->children, &filterNode, 1, QueryNode *);
    q->numTokens++;
  // vector node of type KNN should always be in the root, so we have a special case here.
  } else if (q->root->type == QN_VECTOR && q->root->vn.vq->type == VECSIM_QT_KNN) {
    // for non-hybrid - add the filter node as the child of the vector node.
    if (QueryNode_NumChildren(q->root) == 0) {
      QueryNode_AddChild(q->root, filterNode);
    // otherwise, add a new phrase node as the parent of the current child of the hybrid vector node,
    // and set its children to be the previous child and the new filter node.
    } else {
      RS_LOG_ASSERT(QueryNode_NumChildren(q->root) == 1, "Vector query node can have at most one child");
      QueryNode *nr = NewPhraseNode(0);
      QueryNode_AddChild(nr, filterNode);
      QueryNode_AddChild(nr, q->root->children[0]);
      q->root->children[0] = nr;
    }
  } else {  // for other types, we need to create a new phrase node
    QueryNode *nr = NewPhraseNode(0);
    QueryNode_AddChild(nr, filterNode);
    QueryNode_AddChild(nr, q->root);
    q->numTokens++;
    q->root = nr;
  }
}

void QAST_SetGlobalFilters(QueryAST *ast, const QAST_GlobalFilterOptions *options) {
  if (options->empty) {
    SetFilterNode(ast, NewQueryNode(QN_NULL));
  }
  if (options->numeric) {
    QueryNode *n = NewQueryNode(QN_NUMERIC);
    n->nn.nf = options->numeric;
    SetFilterNode(ast, n);
  }
  if (options->geo) {
    QueryNode *n = NewQueryNode(QN_GEO);
    n->gn.gf = options->geo;
    SetFilterNode(ast, n);
  }
  if (options->keys) {
    QueryNode *n = NewQueryNode(QN_IDS);
    n->fn.keys = options->keys;
    n->fn.len = options->nkeys;
    SetFilterNode(ast, n);
  }
}

static void QueryNode_Expand(RSQueryTokenExpander expander, RSQueryExpanderCtx *expCtx,
                             QueryNode **pqn) {

  QueryNode *qn = *pqn;
  if ((qn->opts.flags & QueryNode_Verbatim) ||    // Do not expand verbatim nodes
      (qn->type == QN_PHRASE && qn->pn.exact) ||  // Do not expand exact phrases
      (qn->type == QN_TAG)) {                     // Tag nodes are handles by their node evaluator
    return;
  }

  // Check that there is at least one stemmable field in the query
  if (expCtx->handle && expCtx->handle->spec) {
    const IndexSpec *spec = expCtx->handle->spec;
    t_fieldMask fm = qn->opts.fieldMask;
    if ( fm != RS_FIELDMASK_ALL) {
      int expand = 0;
      t_fieldMask bit_mask = 1;
      while (fm) {
        if (fm & bit_mask) {
          const FieldSpec *fs = IndexSpec_GetFieldByBit(spec, bit_mask);
          if (fs && !FieldSpec_IsNoStem(fs)) {
            expand = 1;
            break;
          }
        }
        fm &= ~bit_mask;
        bit_mask <<= 1;
      }
      if (!expand) {
        return;
      }
    }
  }

  if (qn->type == QN_TOKEN && qn->tn.len > 0) {
    expCtx->currentNode = pqn;
    expander(expCtx, &qn->tn);
  } else {
    for (size_t ii = 0; ii < QueryNode_NumChildren(qn); ++ii) {
      QueryNode_Expand(expander, expCtx, &qn->children[ii]);
    }
  }
}

QueryIterator *Query_EvalTokenNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_TOKEN, "query node type should be token")
  RSQueryTerm *term = NewQueryTerm(&qn->tn, q->tokenId++);

  if (q->sctx->spec->diskSpec) {
    RS_LOG_ASSERT(q->sctx->spec->diskSpec, "Disk spec should be open");
    return SearchDisk_NewTermIterator(q->sctx->spec->diskSpec, term->str, EFFECTIVE_FIELDMASK(q, qn), qn->opts.weight);
  } else {
    return Redis_OpenReader(q->sctx, term, q->docTable, EFFECTIVE_FIELDMASK(q, qn), qn->opts.weight);
  }
}

static inline void addTerm(char *str, size_t tok_len, QueryEvalCtx *q,
  QueryNodeOptions *opts, QueryIterator ***its, size_t *itsSz, size_t *itsCap) {
  // Create a token for the reader
  RSToken tok = (RSToken){
      .expanded = 0,
      .flags = 0,
      .len = tok_len,
      .str = str
  };

  RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);
  QueryIterator *ir = NULL;

  if (q->sctx->spec->diskSpec) {
    RS_LOG_ASSERT(q->sctx->spec->diskSpec, "Disk spec should be open");
    ir = SearchDisk_NewTermIterator(q->sctx->spec->diskSpec, term->str, q->opts->fieldmask & opts->fieldMask, 1);
  } else {
    // Open an index reader
    ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs,
                                        q->opts->fieldmask & opts->fieldMask, 1);
  }

  if (!ir) {
    return;
  }

  (*its)[(*itsSz)++] = ir;
  if (*itsSz == *itsCap) {
    *itsCap *= 2;
    *its = rm_realloc(*its, (*itsCap) * sizeof(*its));
  }
}

static QueryIterator *iterateExpandedTerms(QueryEvalCtx *q, Trie *terms, const char *str,
                                           size_t len, int maxDist, int prefixMode,
                                           QueryNodeOptions *opts) {
  TrieIterator *it = Trie_Iterate(terms, str, len, maxDist, prefixMode);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  QueryIterator **its = rm_calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  char *target_str = NULL;
  size_t tok_len = 0;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  int hasNext;
  while ((hasNext = TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) &&
         (itsSz < q->config->maxPrefixExpansions)) {
    target_str = runesToStr(rstr, slen, &tok_len);
    addTerm(target_str, tok_len, q, opts, &its, &itsSz, &itsCap);
    rm_free(target_str);
  }
  TrieIterator_Free(it);

  if (hasNext && itsSz == q->config->maxPrefixExpansions) {
    QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
  }

  // Add an iterator over the inverted index of the empty string for fuzzy search
  if (!prefixMode && q->sctx->apiVersion >= 2 && len <= maxDist) {
    addTerm("", 0, q, opts, &its, &itsSz, &itsCap);
  }


  QueryNodeType type = prefixMode ? QN_PREFIX : QN_FUZZY;
  return NewUnionIterator(its, itsSz, true, opts->weight, type, str, q->config);
}

typedef struct {
  QueryIterator **its;
  size_t nits;
  size_t cap;
  QueryEvalCtx *q;
  QueryNodeOptions *opts;
  double weight;
} TrieCallbackCtx;

static int runeIterCb(const rune *r, size_t n, void *p, void *payload);
static int charIterCb(const char *s, size_t n, void *p, void *payload);

static const char *PrefixNode_GetTypeString(const QueryPrefixNode *pfx) {
  if (pfx->prefix && pfx->suffix) {
    return "INFIX";
  } else if (pfx->prefix) {
    return "PREFIX";
  } else {
    return "SUFFIX";
  }
}

#define TRIE_STR_TOO_LONG_MSG "query string is too long. Maximum allowed length is " STRINGIFY(MAX_RUNESTR_LEN)

/* Evaluate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them.
 * Used for Prefix, Contains and suffix nodes.
*/
static QueryIterator *Query_EvalPrefixNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_PREFIX, "query node type should be prefix");

  // we allow a minimum of 2 letters in the prefix by default (configurable)
  if (qn->pfx.tok.len < q->config->minTermPrefix) {
    return NULL;
  }

  IndexSpec *spec = q->sctx->spec;
  Trie *t = spec->terms;
  TrieCallbackCtx ctx = {.q = q, .opts = &qn->opts};

  if (!t) {
    return NULL;
  }

  size_t nstr;
  rune *str = qn->pfx.tok.str ? strToLowerRunes(qn->pfx.tok.str, qn->pfx.tok.len, &nstr) : NULL;
  if (!str) {
    QueryError_SetWithoutUserDataFmt(q->status, QUERY_ERROR_CODE_LIMIT, "%s " TRIE_STR_TOO_LONG_MSG, PrefixNode_GetTypeString(&qn->pfx));
    return NULL;
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
      QueryError_SetError(q->status, QUERY_ERROR_CODE_GENERIC, "Contains query on fields without WITHSUFFIXTRIE support");
    }
  } else {
    TrieNode_IterateContains(t->root, str, nstr, qn->pfx.prefix, qn->pfx.suffix,
                           runeIterCb, &ctx, &q->sctx->time.timeout);
  }

  rm_free(str);

  return NewUnionIterator(ctx.its, ctx.nits, true, qn->opts.weight, QN_PREFIX, qn->pfx.tok.str, q->config);
}

/* Evaluate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them.
 * Used for Prefix, Contains and suffix nodes.
*/
static QueryIterator *Query_EvalWildcardQueryNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_WILDCARD_QUERY, "query node type should be wildcard query");

  IndexSpec *spec = q->sctx->spec;
  Trie *t = spec->terms;
  TrieCallbackCtx ctx = {.q = q, .opts = &qn->opts};
  RSToken *token = &qn->verb.tok;

  if (!t || !token->str) {
    return NULL;
  }

  token->len = Wildcard_RemoveEscape(token->str, token->len);
  size_t nstr;
  rune *str = strToLowerRunes(token->str, token->len, &nstr);
  if (!str) {
    QueryError_SetError(q->status, QUERY_ERROR_CODE_LIMIT, "Wildcard " TRIE_STR_TOO_LONG_MSG);
    return NULL;
  }

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
        .timeout = &q->sctx->time.timeout,
      };
      if (Suffix_IterateWildcard(&sufCtx) == 0) {
        // if suffix trie cannot be used, use brute force
        fallbackBruteForce = true;
      }
    } else {
      QueryError_SetError(q->status, QUERY_ERROR_CODE_GENERIC, "Contains query on fields without WITHSUFFIXTRIE support");
    }
  }

  if (!spec->suffix || fallbackBruteForce) {
    TrieNode_IterateWildcard(t->root, str, nstr, runeIterCb, &ctx, &q->sctx->time.timeout);
  }

  rm_free(str);

  return NewUnionIterator(ctx.its, ctx.nits, true, qn->opts.weight, QN_WILDCARD_QUERY, qn->verb.tok.str, q->config);
}

static void rangeItersAddIterator(TrieCallbackCtx *ctx, QueryIterator *it) {
  ctx->its[ctx->nits++] = it;
  if (ctx->nits == ctx->cap) {
    ctx->cap *= 2;
    ctx->its = rm_realloc(ctx->its, ctx->cap * sizeof(*ctx->its));
  }
}

static void rangeIterCbStrs(const char *r, size_t n, void *p, void *invidx) {
  TrieCallbackCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  RSToken tok = {0};
  tok.str = (char *)r;
  tok.len = n;
  RSQueryTerm *term = NewQueryTerm(&tok, ctx->q->tokenId++);
  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = ctx->opts->fieldIndex};
  QueryIterator *ir = NewInvIndIterator_TermQuery(invidx, q->sctx, fieldMaskOrIndex, term, ctx->weight);
  if (!ir) {
    Term_Free(term);
    return;
  }

  rangeItersAddIterator(ctx, ir);
}

static int runeIterCb(const rune *r, size_t n, void *p, void *payload) {
  TrieCallbackCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  if (!RS_IsMock && ctx->nits >= q->config->maxPrefixExpansions) {
    QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
    return REDISEARCH_ERR;
  }
  RSToken tok = {0};
  tok.str = runesToStr(r, n, &tok.len);
  QueryIterator *ir = NULL;
  RSQueryTerm *term = NewQueryTerm(&tok, ctx->q->tokenId++);
  if (q->sctx->spec->diskSpec) {
    ir = SearchDisk_NewTermIterator(q->sctx->spec->diskSpec, term->str, q->opts->fieldmask & ctx->opts->fieldMask, 1);
  } else {
    ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs,
                                        q->opts->fieldmask & ctx->opts->fieldMask, 1);
  }
  rm_free(tok.str);
  if (ir) {
    rangeItersAddIterator(ctx, ir);
  }

  return REDISEARCH_OK;
}

static int charIterCb(const char *s, size_t n, void *p, void *payload) {
  TrieCallbackCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  if (ctx->nits >= q->config->maxPrefixExpansions) {
    QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
    return REDISEARCH_ERR;
  }
  RSToken tok = {.str = (char *)s, .len = n};
  RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);
  QueryIterator *ir = NULL;
  if (q->sctx->spec->diskSpec) {
    ir = SearchDisk_NewTermIterator(q->sctx->spec->diskSpec, term->str, q->opts->fieldmask & ctx->opts->fieldMask, 1);
  } else {
    ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs,
                                        q->opts->fieldmask & ctx->opts->fieldMask, 1);
  }
  if (ir) {
    rangeItersAddIterator(ctx, ir);
  }

  return REDISEARCH_OK;
}

static QueryIterator *Query_EvalLexRangeNode(QueryEvalCtx *q, QueryNode *lx) {
  RS_LOG_ASSERT(lx->type == QN_LEXRANGE, "query node type should be lexrange");

  Trie *t = q->sctx->spec->terms;
  TrieCallbackCtx ctx = {.q = q, .opts = &lx->opts};

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  rune *begin = NULL, *end = NULL;
  size_t nbegin, nend;
  if (lx->lxrng.begin) {
    begin = strToLowerRunes(lx->lxrng.begin, strlen(lx->lxrng.begin), &nbegin);
  }
  if (lx->lxrng.end) {
    end = strToLowerRunes(lx->lxrng.end, strlen(lx->lxrng.end), &nend);
  }

  TrieNode_IterateRange(t->root, begin, begin ? nbegin : -1, lx->lxrng.includeBegin, end,
                        end ? nend : -1, lx->lxrng.includeEnd, runeIterCb, &ctx);
  rm_free(begin);
  rm_free(end);

  return NewUnionIterator(ctx.its, ctx.nits, true, lx->opts.weight, QN_LEXRANGE, NULL, q->config);
}

static QueryIterator *Query_EvalFuzzyNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_FUZZY, "query node type should be fuzzy");

  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, qn->pfx.tok.str, strlen(qn->pfx.tok.str), qn->fz.maxDist, 0, &qn->opts);
}

static QueryIterator *Query_EvalPhraseNode(QueryEvalCtx *q, QueryNode *qn) {
  QueryPhraseNode *node = &qn->pn;
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (QueryNode_NumChildren(qn) == 1) {
    qn->children[0]->opts.fieldMask &= qn->opts.fieldMask;
    return Query_EvalNode(q, qn->children[0]);
  }

  // recursively eval the children
  QueryIterator **iters = rm_calloc(QueryNode_NumChildren(qn), sizeof(QueryIterator *));
  for (size_t ii = 0; ii < QueryNode_NumChildren(qn); ++ii) {
    qn->children[ii]->opts.fieldMask &= qn->opts.fieldMask;
    iters[ii] = Query_EvalNode(q, qn->children[ii]);
  }
  QueryIterator *ret;

  if (node->exact) {
    ret = NewIntersectionIterator(iters, QueryNode_NumChildren(qn), 0, true, qn->opts.weight);
  } else {
    // Let the query node override the slop/order parameters
    int slop = qn->opts.maxSlop;
    if (slop == -1) slop = q->opts->slop;

    // Let the query node override the inorder of the whole query
    bool inOrder = (q->opts->flags & Search_InOrder) || qn->opts.inOrder;

    ret = NewIntersectionIterator(iters, QueryNode_NumChildren(qn), slop, inOrder, qn->opts.weight);
  }
  return ret;
}

static QueryIterator *Query_EvalWildcardNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_WILDCARD, "query node type should be wildcard");
  RS_LOG_ASSERT(q->docTable, "DocTable is NULL");

  return NewWildcardIterator(q, qn->opts.weight);
}

static QueryIterator *Query_EvalNotNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_NOT, "query node type should be not")
  QueryIterator *child = NULL;
  bool currently_notSubtree = q->notSubtree;
  q->notSubtree = true;
  child = Query_EvalNode(q, qn->children[0]);
  q->notSubtree = currently_notSubtree;

  return NewNotIterator(child, q->docTable->maxDocId, qn->opts.weight, q->sctx->time.timeout, q);
}

static QueryIterator *Query_EvalOptionalNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_OPTIONAL, "query node type should be optional");
  RS_LOG_ASSERT(QueryNode_NumChildren(qn) == 1, "Optional node must have a single child");

  return NewOptionalIterator(Query_EvalNode(q, qn->children[0]), q, qn->opts.weight);
}

static QueryIterator *Query_EvalNumericNode(QueryEvalCtx *q, QueryNode *node) {
  RS_LOG_ASSERT(node->type == QN_NUMERIC, "query node type should be numeric")

  const FieldSpec *fs = node->nn.nf->fieldSpec;
  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = fs->index}, .predicate = FIELD_EXPIRATION_DEFAULT};
  return NewNumericFilterIterator(q->sctx, node->nn.nf, INDEXFLD_T_NUMERIC, q->config, &filterCtx);
}

static QueryIterator *Query_EvalGeofilterNode(QueryEvalCtx *q, QueryNode *node,
                                              double weight) {
  RS_LOG_ASSERT(node->type == QN_GEO, "query node type should be geo");

  if (!GeoFilter_Validate(node->gn.gf, q->status)) {
    return NULL;
  }

  return NewGeoRangeIterator(q->sctx, node->gn.gf, q->config);
}

static QueryIterator *Query_EvalGeometryNode(QueryEvalCtx *q, QueryNode *node) {
  RS_LOG_ASSERT(node->type == QN_GEOMETRY, "query node type should be geometry");

  const FieldSpec *fs = node->gmn.geomq->fs;

  // TODO: open with DONT_CREATE_INDEX once the query string is validated before we get here.
  // Currently, if  we use DONT_CREATE_INDEX, and the index was not initialized yet, and the query is invalid,
  // we return results as if the index was empty, instead of raising an error.
  const GeometryIndex *index = OpenGeometryIndex(q->sctx->spec, fs, CREATE_INDEX);
  const GeometryApi *api = GeometryApi_Get(index);
  const GeometryQuery *gq = node->gmn.geomq;
  RedisModuleString *errMsg;
  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = fs->index}, .predicate = FIELD_EXPIRATION_DEFAULT};
  QueryIterator *ret = api->query(q->sctx, &filterCtx, index, gq->query_type, gq->format, gq->str, gq->str_len, &errMsg);
  if (ret == NULL) {
    QueryError_SetWithUserDataFmt(q->status, QUERY_ERROR_CODE_BAD_VAL, "Error querying geoshape index", ": %s",
                           RedisModule_StringPtrLen(errMsg, NULL));
    RedisModule_FreeString(NULL, errMsg);
  }
  return ret;
}


static QueryIterator *Query_EvalVectorNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_VECTOR, "query node type should be vector");

  if (qn->opts.distField) {
    if (qn->vn.vq->scoreField) {
      // Since the KNN syntax allows specifying the distance field in two ways (...=>[KNN ... AS <dist_field>] and
      // ...=>[KNN ...]=>{$YIELD_DISTANCE_AS:<dist_field>), we validate that we got it only once.
      size_t len;
      const char *fieldName = HiddenString_GetUnsafe(qn->vn.vq->field->fieldName, &len);
      char default_score_field[len + 9];  // buffer for __<field>_score
      sprintf(default_score_field, "__%s_score", fieldName);
      // If the saved score field is NOT the default one, we return an error, otherwise, just override it.
      if (strcasecmp(qn->vn.vq->scoreField, default_score_field) != 0) {
        QueryError_SetWithUserDataFmt(q->status, QUERY_ERROR_CODE_DUP_FIELD,
                               "Distance field was specified twice for vector query", ": %s and %s",
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
  size_t idx;
  if (qn->vn.vq->scoreField) {
    idx = addMetricRequest(q, qn->vn.vq->scoreField, qn->opts.flags & QueryNode_HideVectorDistanceField);
  }

  QueryIterator *child_it = NULL;
  if (QueryNode_NumChildren(qn) > 0) {
    RS_ASSERT(QueryNode_NumChildren(qn) == 1);
    child_it = Query_EvalNode(q, qn->children[0]);
    // If child iterator is in valid or empty, the hybrid iterator is empty as well.
    if (child_it == NULL) {
      return NULL;
    }
  }
  QueryIterator *it = NewVectorIterator(q, qn->vn.vq, child_it);
  // If iterator was created successfully, and we have a metric to yield, update the
  // Only create MetricRequest entries for iterators that actually yield metrics
  if (it && qn->vn.vq->scoreField &&
      (it->type == HYBRID_ITERATOR || it->type == METRIC_ITERATOR)) {
    MetricRequest *request = array_ensure_at(q->metricRequestsP, idx, MetricRequest);

    // Create a handle that points to the iterator's ownKey field
    // Both HYBRID_ITERATOR and METRIC_ITERATOR have the same ownKey and keyHandle layout
    RLookupKeyHandle *handle = rm_malloc(sizeof(RLookupKeyHandle));
    handle->is_valid = true;

    if (it->type == HYBRID_ITERATOR) {
      HybridIterator *hybridIt = (HybridIterator *)it;
      handle->key_ptr = &hybridIt->ownKey;
      hybridIt->keyHandle = handle; // Set up back-reference
    } else { // Must be METRIC_ITERATOR due to the condition above
      MetricIterator *metricIt = (MetricIterator *)it;
      handle->key_ptr = &metricIt->ownKey;
      metricIt->keyHandle = handle; // Set up back-reference
    }

    request->key_handle = handle;
  }
  if (it == NULL && child_it != NULL) {
    child_it->Free(child_it);
  }
  return it;
}

static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;
  return (int)(*d1 - *d2);
}

static inline size_t deduplicateDocIdsFrom(t_docId *ids, size_t num, size_t start) {
  size_t j = start - 1;
  for (size_t i = start + 1; i < num; ++i) {
    if (ids[i] != ids[j]) {
      ids[++j] = ids[i];
    }
  }
  return j + 1;
}

static inline size_t deduplicateDocIds(t_docId *ids, size_t num) {
  for (size_t i = 1; i < num; ++i) {
    if (ids[i] == ids[i - 1]) {
      return deduplicateDocIdsFrom(ids, num, i);
    }
  }
  return num;
}

static QueryIterator *Query_EvalIdFilterNode(QueryEvalCtx *q, QueryIdFilterNode *node) {
  size_t num = 0;
  t_docId* it_ids = rm_malloc(sizeof(*it_ids) * node->len);
  for (size_t ii = 0; ii < node->len; ++ii) {
    t_docId did = DocTable_GetId(&q->sctx->spec->docs, node->keys[ii], sdslen(node->keys[ii]));
    if (did) {
      it_ids[num++] = did;
    }
  }
  if (num) {
    qsort(it_ids, num, sizeof(t_docId), cmp_docids);
    num = deduplicateDocIds(it_ids, num);
  }
  // Passing the ownership of the ids to the iterator.
  return NewIdListIterator(it_ids, num, 1);
}

static QueryIterator *Query_EvalUnionNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_UNION, "query node type should be union")

  // a union stage with one child is the same as the child, so we just return it
  if (QueryNode_NumChildren(qn) == 1) {
    return Query_EvalNode(q, qn->children[0]);
  }

  // recursively eval the children
  QueryIterator **iters = rm_malloc(QueryNode_NumChildren(qn) * sizeof(QueryIterator *));
  for (size_t i = 0; i < QueryNode_NumChildren(qn); ++i) {
    qn->children[i]->opts.fieldMask &= qn->opts.fieldMask;
    iters[i] = Query_EvalNode(q, qn->children[i]);
  }

  // We want to get results with all the matching children (`quickExit == false`), unless:
  // 1. We are a `Not` sub-tree, so we only care about the set of IDs
  // 2. The node's weight is 0, which means the sub-tree is not relevant for scoring.
  bool quickExit = q->notSubtree || qn->opts.weight == 0;
  QueryIterator *ret = NewUnionIterator(iters, QueryNode_NumChildren(qn), quickExit, qn->opts.weight, QN_UNION, NULL, q->config);
  return ret;
}

/**
 * Converts a given string to lowercase and handles escape sequences.
 *
 * This function processes the input string and converts it to lowercase
 * if `caseSensitive` is false.
 * If no memory allocation is needed for lowerconversion, the string is modified
 * in place.
 * If memory allocation is needed, the original string is freed and replaced
 * with the new lowercase string.
 * It also handles escape sequences by removing the backslash character if it
 * precedes a punctuation or whitespace character.
 *
 * @param pstr A pointer to the input string.
 * @param len A pointer to the length of the input string. The length is updated
 * to reflect any changes made to the string.
 * @param caseSensitive A flag indicating whether the conversion to lowercase
 * should be performed. If true, the string remains case-sensitive.
 */
static void tag_strtolower(char **pstr, size_t *len, int caseSensitive) {
  size_t length = *len;
  char *str = *pstr;
  char *origStr = str;
  char *p = str;

  while (*p) {
    if (*p == '\\' && (ispunct(*(p+1)) || isspace(*(p+1)))) {
      ++p;
      --length;
    }
    *str++ = *p++;
  }
  *str = '\0';

  if (!caseSensitive) {
    char *dst = unicode_tolower(origStr, &length);
    if (dst) {
        rm_free(origStr);
        *pstr = dst;
    } else {
      // No memory allocation, just ensure null termination
      origStr[length] = '\0';
    }
  }
  *len = length;
}

static QueryIterator *Query_EvalTagLexRangeNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                                double weight, bool caseSensitive) {
  TrieMap *t = idx->values;
  TrieCallbackCtx ctx = {.q = q, .opts = &qn->opts, .weight = weight};

  if (!t) {
    return NULL;
  }

  if(qn->lxrng.begin) {
    size_t beginLen = strlen(qn->lxrng.begin);
    tag_strtolower(&(qn->lxrng.begin), &beginLen, caseSensitive);
  }
  if(qn->lxrng.end) {
    size_t endLen = strlen(qn->lxrng.end);
    tag_strtolower(&(qn->lxrng.end), &endLen, caseSensitive);
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  const char *begin = qn->lxrng.begin, *end = qn->lxrng.end;
  int nbegin = begin ? strlen(begin) : -1, nend = end ? strlen(end) : -1;

  TrieMap_IterateRange(t, begin, nbegin, qn->lxrng.includeBegin, end, nend, qn->lxrng.includeEnd,
                       rangeIterCbStrs, &ctx);

  return NewUnionIterator(ctx.its, ctx.nits, true, qn->opts.weight, QN_LEXRANGE, NULL, q->config);
}

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static QueryIterator *Query_EvalTagPrefixNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn, double weight,
                                              int withSuffixTrie, t_fieldIndex fieldIndex,
                                              bool caseSensitive) {
  if (qn->type != QN_PREFIX) {
    return NULL;
  }
  RSToken *tok = &qn->pfx.tok;

  tag_strtolower(&(tok->str), &tok->len, caseSensitive);

  // we allow a minimum of 2 letters in the prefix by default (configurable)
  if (tok->len < q->config->minTermPrefix) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  size_t itsSz = 0, itsCap = 8;
  QueryIterator **its = rm_calloc(itsCap, sizeof(*its));

  if (!qn->pfx.suffix || !withSuffixTrie) {    // prefix query or no suffix triemap, use bruteforce
    tm_iter_mode iter_mode = TM_PREFIX_MODE;
    if (qn->pfx.suffix) {
      if (qn->pfx.prefix) { // contains mode
        iter_mode = TM_CONTAINS_MODE;
      } else {
        iter_mode = TM_SUFFIX_MODE;
      }
    }
    TrieMapIterator *it = TrieMap_IterateWithFilter(idx->values, tok->str, tok->len, iter_mode);
    if (!it) {
      rm_free(its);
      return NULL;
    }
    TrieMapIterator_SetTimeout(it, q->sctx->time.timeout);


    // an upper limit on the number of expansions is enforced to avoid stuff like "*"
    char *s;
    tm_len_t sl;
    void *ptr;

    // Find all completions of the prefix
    int hasNext;
    while ((hasNext = TrieMapIterator_Next(it, &s, &sl, &ptr)) &&
           (itsSz < q->config->maxPrefixExpansions)) {
      QueryIterator *ret = TagIndex_OpenReader(idx, q->sctx, s, sl, 1, fieldIndex);
      if (!ret) continue;

      // Add the reader to the iterator array
      its[itsSz++] = ret;
      if (itsSz == itsCap) {
        itsCap *= 2;
        its = rm_realloc(its, itsCap * sizeof(*its));
      }
    }

    if (hasNext && itsSz == q->config->maxPrefixExpansions) {
      QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
    }

    TrieMapIterator_Free(it);
  } else {    // TAG field has suffix triemap
    arrayof(char**) arr = GetList_SuffixTrieMap(idx->suffix, tok->str, tok->len,
                                                qn->pfx.prefix, q->sctx->time.timeout);
    if (!arr) {
      rm_free(its);
      return NULL;
    }
    for (int i = 0; i < array_len(arr) && itsSz < q->config->maxPrefixExpansions; ++i) {
      size_t iarrlen = array_len(arr);
      for (int j = 0; j < array_len(arr[i]); ++j) {
        size_t jarrlen = array_len(arr[i]);
        if (itsSz >= q->config->maxPrefixExpansions) {
          QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
          break;
        }
        QueryIterator *ret = TagIndex_OpenReader(idx, q->sctx, arr[i][j], strlen(arr[i][j]), 1, fieldIndex);
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

  return NewUnionIterator(its, itsSz, true, weight, QN_PREFIX, qn->pfx.tok.str, q->config);
}

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static QueryIterator *Query_EvalTagWildcardNode(QueryEvalCtx *q, TagIndex *idx,
                     QueryNode *qn, double weight,
                     t_fieldIndex fieldIndex, bool caseSensitive) {
  RS_ASSERT(qn->type == QN_WILDCARD_QUERY);
  if (!idx || !idx->values) return NULL;

  RSToken *tok = &qn->verb.tok;

  tag_strtolower(&(tok->str), &tok->len, caseSensitive);

  tok->len = Wildcard_RemoveEscape(tok->str, tok->len);

  size_t itsSz = 0, itsCap = 8;
  QueryIterator **its = rm_malloc(itsCap * sizeof(*its));

  bool fallbackBruteForce = false;
  if (idx->suffix) {
    // with suffix
    arrayof(char*) arr = GetList_SuffixTrieMap_Wildcard(idx->suffix, tok->str, tok->len,
                                                        q->sctx->time.timeout, q->config->maxPrefixExpansions);
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
          QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
          break;
        }
        QueryIterator *ret = TagIndex_OpenReader(idx, q->sctx, arr[i], strlen(arr[i]), 1, fieldIndex);
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
    TrieMapIterator *it = TrieMap_IterateWithFilter(idx->values, tok->str, tok->len, TM_WILDCARD_MODE);
    TrieMapIterator_SetTimeout(it, q->sctx->time.timeout);

    char *s;
    tm_len_t sl;
    void *ptr;

    // Find all completions of the prefix
    int hasNext;
    while ((hasNext = TrieMapIterator_Next(it, &s, &sl, &ptr)) &&
           (itsSz < q->config->maxPrefixExpansions)) {
      QueryIterator *ret = TagIndex_OpenReader(idx, q->sctx, s, sl, 1, fieldIndex);
      if (!ret) continue;

      // Add the reader to the iterator array
      its[itsSz++] = ret;
      if (itsSz == itsCap) {
        itsCap *= 2;
        its = rm_realloc(its, itsCap * sizeof(*its));
      }
    }

    if (hasNext && itsSz == q->config->maxPrefixExpansions) {
      QueryError_SetReachedMaxPrefixExpansionsWarning(q->status);
    }

    TrieMapIterator_Free(it);
  }

  return NewUnionIterator(its, itsSz, true, weight, QN_WILDCARD_QUERY, qn->pfx.tok.str, q->config);
}

static QueryIterator *query_EvalSingleTagNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *n,
                                              double weight, const FieldSpec *fs) {
  QueryIterator *ret = NULL;
  int caseSensitive = fs->tagOpts.tagFlags & TagField_CaseSensitive;

  // For hybrid queries, use weight 0.0 to disable tag scoring
  // Use IsHybrid-like logic adapted for QueryEvalCtx
  bool is_hybrid = (q->reqFlags & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY) ||
                  (q->reqFlags & QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY);
  double effective_weight = is_hybrid ? 0.0 : weight;

  switch (n->type) {
    case QN_TOKEN: {
      tag_strtolower(&(n->tn.str), &n->tn.len, caseSensitive);
      ret = TagIndex_OpenReader(idx, q->sctx, n->tn.str, n->tn.len, effective_weight, fs->index);
      break;
    }
    case QN_PREFIX:
      return Query_EvalTagPrefixNode(q, idx, n, effective_weight,
                         FieldSpec_HasSuffixTrie(fs), fs->index, caseSensitive);

    case QN_WILDCARD_QUERY:
      return Query_EvalTagWildcardNode(q, idx, n, effective_weight, fs->index,
                                       caseSensitive);

    case QN_LEXRANGE:
      return Query_EvalTagLexRangeNode(q, idx, n, effective_weight, caseSensitive);

    case QN_PHRASE: {
      char *terms[QueryNode_NumChildren(n)];
      for (size_t i = 0; i < QueryNode_NumChildren(n); ++i) {
        if (n->children[i]->type == QN_TOKEN) {
          tag_strtolower(&(n->children[i]->tn.str), &n->children[i]->tn.len, caseSensitive);
          terms[i] = n->children[i]->tn.str;
        } else {
          terms[i] = "";
        }
      }

      sds s = sdsjoin(terms, QueryNode_NumChildren(n), " ");

      ret = TagIndex_OpenReader(idx, q->sctx, s, sdslen(s), effective_weight, fs->index);
      sdsfree(s);
      break;
    }

    default:
      return NULL;
  }

  return ret;
}

static QueryIterator *Query_EvalTagNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_ASSERT(qn->type == QN_TAG);
  QueryTagNode *node = &qn->tag;
  RedisModuleString *kstr = IndexSpec_GetFormattedKey(q->sctx->spec, node->fs, INDEXFLD_T_TAG);
  TagIndex *idx = TagIndex_Open(q->sctx->spec, kstr, DONT_CREATE_INDEX);

  if (!idx) {
    // There are no documents to traverse.
    return NULL;
  }
  if (QueryNode_NumChildren(qn) == 1) {
    // a union stage with one child is the same as the child, so we just return it
    return query_EvalSingleTagNode(q, idx, qn->children[0], qn->opts.weight, node->fs);
  }

  // recursively eval the children
  QueryIterator **iters = rm_malloc(QueryNode_NumChildren(qn) * sizeof(QueryIterator *));
  for (size_t i = 0; i < QueryNode_NumChildren(qn); i++) {
    iters[i] = query_EvalSingleTagNode(q, idx, qn->children[i], qn->opts.weight, node->fs);
  }
  // We want to get results with all the matching children (`quickExit == false`), unless:
  // 1. We are a `Not` sub-tree, so we only care about the set of IDs
  // 2. The node's weight is 0, which means the sub-tree is not relevant for scoring.
  bool quickExit = q->notSubtree || qn->opts.weight == 0;
  return NewUnionIterator(iters, QueryNode_NumChildren(qn), quickExit, qn->opts.weight, QN_TAG, NULL, q->config);
}

static QueryIterator *Query_EvalMissingNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_MISSING, "query qn type should be missing")
  const FieldSpec *fs = qn->miss.field;

  // Get the InvertedIndex corresponding to the queried field.
  InvertedIndex *missingII = dictFetchValue(q->sctx->spec->missingFieldDict, fs->fieldName);

  if (!missingII) {
    // There are no missing values for this field.
    return NULL;
  }

  // Create an iterator for the missing values InvertedIndex.
  return NewInvIndIterator_MissingQuery(missingII, q->sctx, fs->index);
}

QueryIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n) {
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

QueryIterator *QAST_Iterate(QueryAST *qast, const RSSearchOptions *opts, RedisSearchCtx *sctx,
                            uint32_t reqflags, QueryError *status) {
  QueryEvalCtx qectx = {
      .opts = opts,
      .numTokens = qast->numTokens,
      .docTable = &sctx->spec->docs,
      .sctx = sctx,
      .status = status,
      .metricRequestsP = &qast->metricRequests,
      .reqFlags = reqflags,
      .config = &qast->config,
      .notSubtree = false,
  };
  QueryIterator *root = Query_EvalNode(&qectx, qast->root);
  if (!root) {
    // Return the dummy iterator
    root = NewEmptyIterator();
  }
  return root;
}

void QAST_Destroy(QueryAST *q) {
  QueryNode_Free(q->root);
  q->root = NULL;

  // Free the key handles in metric requests
  if (q->metricRequests) {
    for (size_t i = 0; i < array_len(q->metricRequests); i++) {
      if (q->metricRequests[i].key_handle) {
        rm_free(q->metricRequests[i].key_handle);
      }
    }
  }

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

int QAST_EvalParams(QueryAST *q, RSSearchOptions *opts, unsigned int dialectVersion, QueryError *status) {
  if (!q || !q->root || q->numParams == 0)
    return REDISMODULE_OK;
  QueryNode_EvalParams(opts->params, q->root, dialectVersion, status);
  return REDISMODULE_OK;
}

int QueryNode_EvalParams(dict *params, QueryNode *n, unsigned int dialectVersion, QueryError *status) {
  int withChildren = 1;
  int res = REDISMODULE_OK;
  switch(n->type) {
    case QN_VECTOR:
      res = VectorQuery_EvalParams(params, n, dialectVersion, status);
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
      res = QueryNode_EvalParamsCommon(params, n, dialectVersion, status);
      break;
    case QN_UNION:
      // no immediately owned params to resolve
      RS_ASSERT(n->params == NULL);
      break;
    case QN_NULL:
    case QN_MISSING:
      withChildren = 0;
      break;
  }
  // Handle children
  if (withChildren && res == REDISMODULE_OK) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
      res = QueryNode_EvalParams(params, n->children[ii], dialectVersion, status);
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
    QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, "Use `" SPEC_INDEXEMPTY_STR "` in field creation in order to index and query for empty strings");
    return false;
  }
  return true;
}

static int QueryNode_CheckIsValid(QueryNode *n, IndexSpec *spec, RSSearchOptions *opts,
  QueryError *status, QAST_ValidationFlags validationFlags) {
  // Check if this is the main vector node in a hybrid vector subquery
  QAST_ValidationFlags effectiveFlags = validationFlags;
  if ((n->opts.flags & QueryNode_HybridVectorSubqueryNode) && (n->type == QN_VECTOR)) {
    // This is the main vector node in hybrid vector subquery - allow it despite restrictions
    effectiveFlags &= ~(QAST_NO_WEIGHT | QAST_NO_VECTOR);
  }

  // Check for weight attribute restrictions
  if ((effectiveFlags & QAST_NO_WEIGHT) && n->opts.explicitWeight) {
    QueryError_SetError(status, QUERY_ERROR_CODE_WEIGHT_NOT_ALLOWED, NULL);
    return REDISMODULE_ERR;
  }

  bool withChildren = true;
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
      withChildren = false;
      break;
    case QN_TAG:
      {
        opts->flags |= QueryNode_IsTag;
        const FieldSpec *fs = n->tag.fs;
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
    case QN_NUMERIC: {
        if (n->nn.nf->min > n->nn.nf->max) {
          QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Invalid numeric range (min > max)", ": @%s:[%f %f]",
                                 HiddenString_GetUnsafe(n->nn.nf->fieldSpec->fieldName, NULL), n->nn.nf->min, n->nn.nf->max);
          res = REDISMODULE_ERR;
        }
      }
      break;
    case QN_VECTOR:
      if (effectiveFlags & QAST_NO_VECTOR) {
        QueryError_SetError(status, QUERY_ERROR_CODE_VECTOR_NOT_ALLOWED, NULL);
        res = REDISMODULE_ERR;
      }
      break;
    case QN_NOT:
    case QN_OPTIONAL:
    case QN_GEO:
    case QN_PREFIX:
    case QN_IDS:
    case QN_WILDCARD:
    case QN_WILDCARD_QUERY:
    case QN_FUZZY:
    case QN_LEXRANGE:
    case QN_GEOMETRY:
      break;
  }

  // Handle children
  if (withChildren && res == REDISMODULE_OK) {
    for (size_t ii = 0; ii < QueryNode_NumChildren(n); ++ii) {
      res = QueryNode_CheckIsValid(n->children[ii], spec, opts, status, validationFlags);
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

  return QueryNode_CheckIsValid(q->root, spec, opts, status, q->validationFlags);
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

int QueryNode_EvalParamsCommon(dict *params, QueryNode *node, unsigned int dialectVersion, QueryError *status) {
  if (node->params) {
    for (size_t i = 0; i < QueryNode_NumParams(node); i++) {
      int res = QueryParam_Resolve(&node->params[i], params, dialectVersion, status);
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
      s = sdscatprintf(s, "%s{%s%s%s}", PrefixNode_GetTypeString(&qs->pfx),
                                        qs->pfx.suffix ? "*" : "",
                                        qs->pfx.tok.str,
                                        qs->pfx.prefix ? "*" : "");
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
      s = sdscatprintf(s, "NUMERIC {%f %s @%s %s %f}", f->min, f->minInclusive ? "<=" : "<",
                       HiddenString_GetUnsafe(f->fieldSpec->fieldName, NULL), f->maxInclusive ? "<=" : "<", f->max);
    } break;
    case QN_UNION:
      s = sdscat(s, "UNION {\n");
      s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;
    case QN_TAG:
      s = sdscatprintf(s, "TAG:@%s {\n", HiddenString_GetUnsafe(qs->tag.fs->fieldName, NULL));
      s = QueryNode_DumpChildren(s, spec, qs, depth + 1);
      s = doPad(s, depth);
      s = sdscat(s, "}");
      break;
    case QN_GEO:
      s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s}", HiddenString_GetUnsafe(qs->gn.gf->fieldSpec->fieldName, NULL), qs->gn.gf->lon,
                       qs->gn.gf->lat, qs->gn.gf->radius,
                       GeoDistance_ToString(qs->gn.gf->unitType));
      break;
    case QN_IDS:

      s = sdscat(s, "IDS {");
      for (int i = 0; i < qs->fn.len; i++) {
        t_docId id = DocTable_GetId(&spec->docs, qs->fn.keys[i], sdslen(qs->fn.keys[i]));
        if (id != 0) {
          s = sdscatprintf(s, "%lu,", id);
        }
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
      s = sdscatprintf(s, "in vector index associated with field @%s", HiddenString_GetUnsafe(qs->vn.vq->field->fieldName, NULL));
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
      s = sdscatprintf(s, "ISMISSING{%s}", HiddenString_GetUnsafe(qs->miss.field->fieldName, NULL));
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

static int ValidateShardKRatio(const char *value, double *ratio, QueryError *status) {
  if (!ParseDouble(value, ratio, 1)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
      "Invalid shard k ratio value", " '%s'", value);
    return 0;
  }

  if (*ratio <= MIN_SHARD_WINDOW_RATIO || *ratio > MAX_SHARD_WINDOW_RATIO) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
      "Invalid shard k ratio value: Shard k ratio must be greater than %g and at most %g (got %g)",
      MIN_SHARD_WINDOW_RATIO, MAX_SHARD_WINDOW_RATIO, *ratio);
    return 0;
  }

  return 1;
}

// Convert the query attribute into a raw vector param to be resolved by the vector iterator
// down the road. return 0 in case of an unrecognized parameter.
static int QueryVectorNode_ApplyAttribute(VectorQuery *vq, QueryAttribute *attr, QueryError *status) {
  if (STR_EQCASE(attr->name, attr->namelen, SHARD_K_RATIO_ATTR)) {
    double ratio;
    if (!ValidateShardKRatio(attr->value, &ratio, status)) {
      return 0;
    }
    vq->knn.shardWindowRatio = ratio;
    return 1;
  } else if (STR_EQCASE(attr->name, attr->namelen, VECSIM_EFRUNTIME) ||
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

#define MK_INVALID_VALUE()                                                             \
  QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_SYNTAX, "Invalid value", " (%.*s) for `%.*s`", \
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
    qn->opts.explicitWeight = true;
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
    res = QueryVectorNode_ApplyAttribute(qn->vn.vq, attr, status);
  }

  if (!res) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_NO_OPTION, "Invalid attribute", " %.*s", (int)attr->namelen,
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
