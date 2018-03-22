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
#include "concurrent_ctx.h"

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
      if (n->gn.gf) {
        GeoFilter_Free(n->gn.gf);
      }

    case QN_WILDCARD:
    case QN_IDS:
      break;

    case QN_TAG:
      QueryTagNode_Free(&n->tag);
  }
  free(n);
}

static QueryNode *NewQueryNode(QueryNodeType type) {
  QueryNode *s = calloc(1, sizeof(QueryNode));
  s->type = type;
  s->fieldMask = RS_FIELDMASK_ALL;
  s->flags = 0;
  return s;
}

QueryNode *NewTokenNodeExpanded(QueryParseCtx *q, const char *s, size_t len, RSTokenFlags flags) {
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
  ret->not.child = n;
  return ret;
}

QueryNode *NewOptionalNode(QueryNode *n) {
  QueryNode *ret = NewQueryNode(QN_OPTIONAL);
  ret->not.child = n;
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

static void QueryNode_Expand(RSQueryTokenExpander expander, RSQueryExpanderCtx *expCtx,
                             QueryNode **pqn) {

  QueryNode *qn = *pqn;
  // Do not expand verbatim nodes
  if (qn->flags & QueryNode_Verbatim) {
    return;
  }

  if (qn->type == QN_TOKEN) {
    expCtx->currentNode = pqn;
    expander(expCtx, &qn->tn);

  } else if (qn->type == QN_PHRASE && !qn->pn.exact) {  // do not expand exact phrases
    for (int i = 0; i < qn->pn.numChildren; i++) {
      QueryNode_Expand(expander, expCtx, &qn->pn.children[i]);
    }
  } else if (qn->type == QN_UNION) {
    for (int i = 0; i < qn->un.numChildren; i++) {
      QueryNode_Expand(expander, expCtx, &qn->un.children[i]);
    }
  }
}

void Query_Expand(QueryParseCtx *q, const char *expander) {
  if (!q->root) return;

  RSQueryExpanderCtx expCtx = {.query = q,
                               .language = q->opts.language ? q->opts.language : DEFAULT_LANGUAGE};

  ExtQueryExpanderCtx *xpc =
      Extensions_GetQueryExpander(&expCtx, expander ? expander : DEFAULT_EXPANDER_NAME);
  if (xpc && xpc->exp) {
    QueryNode_Expand(xpc->exp, &expCtx, &q->root);
    if (xpc->ff) xpc->ff(expCtx.privdata);
  }
}

IndexIterator *Query_EvalTokenNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_TOKEN) {
    return NULL;
  }
  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index
  int isSingleWord = q->numTokens == 1 && q->opts->fieldMask == RS_FIELDMASK_ALL;

  RSQueryTerm *term = NewQueryTerm(&qn->tn, q->tokenId++);

  IndexReader *ir = Redis_OpenReader(q->sctx, term, q->docTable, isSingleWord,
                                     q->opts->fieldMask & qn->fieldMask, q->conc);
  if (ir == NULL) {
    Term_Free(term);
    return NULL;
  }

  return NewReadIterator(ir);
}

/* Ealuate a prefix node by expanding all its possible matches and creating one big UNION on all of
 * them */
static IndexIterator *Query_EvalPrefixNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_PREFX) {
    return NULL;
  }

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (qn->pfx.len < RSGlobalConfig.minTermPrefix) {
    return NULL;
  }
  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  TrieIterator *it = Trie_IteratePrefix(terms, qn->pfx.str, qn->pfx.len, 0);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"

  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist) &&
         itsSz < RSGlobalConfig.maxPrefixExpansions) {

    // Create a token for the reader
    RSToken tok = (RSToken){
        .expanded = 0,
        .flags = 0,
        .len = 0,
    };
    tok.str = runesToStr(rstr, slen, &tok.len);
    RSQueryTerm *term = NewQueryTerm(&tok, q->tokenId++);

    // Open an index reader
    IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                       q->opts->fieldMask & qn->fieldMask, q->conc);

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
  return NewUnionIterator(its, itsSz, q->docTable, 1);
}

static IndexIterator *Query_EvalPhraseNode(QueryEvalCtx *q, QueryNode *qn) {
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
    ret = NewIntersecIterator(iters, node->numChildren, q->docTable,
                              q->opts->fieldMask & qn->fieldMask, 0, 1);
  } else {
    ret = NewIntersecIterator(iters, node->numChildren, q->docTable,
                              q->opts->fieldMask & qn->fieldMask, q->opts->slop,
                              q->opts->flags & Search_InOrder);
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
  QueryNotNode *node = &qn->not;

  return NewNotIterator(node->child ? Query_EvalNode(q, node->child) : NULL, q->docTable->maxDocId);
}

static IndexIterator *Query_EvalOptionalNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_OPTIONAL) {
    return NULL;
  }
  QueryOptionalNode *node = &qn->opt;

  return NewOptionalIterator(node->child ? Query_EvalNode(q, node->child) : NULL,
                             q->docTable->maxDocId);
}

static IndexIterator *Query_EvalNumericNode(QueryEvalCtx *q, QueryNumericNode *node) {

  FieldSpec *fs =
      IndexSpec_GetField(q->sctx->spec, node->nf->fieldName, strlen(node->nf->fieldName));
  if (!fs || fs->type != FIELD_NUMERIC) {
    return NULL;
  }

  return NewNumericFilterIterator(q->sctx, node->nf, q->conc);
}

static IndexIterator *Query_EvalGeofilterNode(QueryEvalCtx *q, QueryGeofilterNode *node) {

  FieldSpec *fs = IndexSpec_GetField(q->sctx->spec, node->gf->property, strlen(node->gf->property));
  if (fs == NULL || fs->type != FIELD_GEO) {
    return NULL;
  }

  GeoIndex gi = {.ctx = q->sctx, .sp = fs};
  return NewGeoRangeIterator(&gi, node->gf);
}

static IndexIterator *Query_EvalIdFilterNode(QueryEvalCtx *q, QueryIdFilterNode *node) {

  return NewIdFilterIterator(node->f);
}

static IndexIterator *Query_EvalUnionNode(QueryEvalCtx *q, QueryNode *qn) {
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

  if (n == 1) {
    IndexIterator *ret = iters[0];
    free(iters);
    return ret;
  }

  IndexIterator *ret = NewUnionIterator(iters, n, q->docTable, 0);
  return ret;
}

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static IndexIterator *Query_EvalTagPrefixNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                              RedisModuleKey *k, RedisModuleString *kn) {
  if (qn->type != QN_PREFX) {
    return NULL;
  }

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (qn->pfx.len < RSGlobalConfig.minTermPrefix) {
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
  while (TrieMapIterator_Next(it, &s, &sl, &ptr) && itsSz < RSGlobalConfig.maxPrefixExpansions) {
    IndexIterator *ret = TagIndex_OpenReader(idx, q->docTable, s, sl, q->conc, k, kn);
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
  return NewUnionIterator(its, itsSz, q->docTable, 1);
}

static IndexIterator *query_EvalSingleTagNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *n,
                                              RedisModuleKey *k, RedisModuleString *kn) {

  switch (n->type) {
    case QN_TOKEN:
      /*TagIndex *idx, DocTable *dt, const char *value, size_t len,
                                         ConcurrentSearchCtx *csx, RedisModuleKey *k,
                                         RedisModuleString *keyName);*/
      return TagIndex_OpenReader(idx, q->docTable, n->tn.str, n->tn.len, q->conc, k, kn);
    case QN_PREFX:
      return Query_EvalTagPrefixNode(q, idx, n, k, kn);

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

      IndexIterator *ret = TagIndex_OpenReader(idx, q->docTable, s, sdslen(s), q->conc, k, kn);
      sdsfree(s);
      return ret;
    }

    default:
      return NULL;
  }
}

static IndexIterator *Query_EvalTagNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_TAG) {
    return NULL;
  }
  QueryTagNode *node = &qn->tag;
  RedisModuleKey *k = NULL;
  RedisModuleString *str = TagIndex_FormatName(q->sctx, node->fieldName);
  TagIndex *idx = TagIndex_Open(q->sctx->redisCtx, str, 0, &k);
  if (!idx) return NULL;
  // a union stage with one child is the same as the child, so we just return it
  if (node->numChildren == 1) {
    return query_EvalSingleTagNode(q, idx, node->children[0], k, str);
  }

  // recursively eval the children
  IndexIterator **iters = calloc(node->numChildren, sizeof(IndexIterator *));
  int n = 0;
  for (int i = 0; i < node->numChildren; i++) {
    IndexIterator *it = query_EvalSingleTagNode(q, idx, node->children[i], k, str);
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

/* Set the field mask recursively on a query node. This is called by the parser to handle situations
 * like @foo:(bar baz|gaz), where a complex tree is being applied a field mask */
void QueryNode_SetFieldMask(QueryNode *n, t_fieldMask mask) {
  if (!n) return;
  n->fieldMask &= mask;
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
      QueryNode_SetFieldMask(n->not.child, mask);
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
  // QueryNode_Print(NULL, parent, 0);
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

static void assignSearchOpts(RSSearchOptions *tgt, const RSSearchOptions *src,
                             RedisSearchCtx *ctx) {
  if (src) {
    *tgt = *src;
  } else {
    *tgt = RS_DEFAULT_SEARCHOPTS;
  }
  if (tgt->flags & Search_NoStopwrods) {
    tgt->stopwords = EmptyStopWordList();
  } else {
    tgt->stopwords =
        (ctx && ctx->spec && ctx->spec->stopwords) ? ctx->spec->stopwords : DefaultStopWordList();
  }
}

QueryParseCtx *NewQueryParseCtx(RedisSearchCtx *sctx, const char *raw, size_t len,
                                RSSearchOptions *opts) {

  QueryParseCtx *ctx = malloc(sizeof(*ctx));
  ctx->len = len;
  ctx->raw = strdup(raw);
  ctx->numTokens = 0;
  ctx->ok = 1;
  ctx->root = NULL;
  ctx->sctx = sctx;
  ctx->tokenId = 1;
  ctx->errorMsg = NULL;
  assignSearchOpts(&ctx->opts, opts, sctx);

  return ctx;
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

static sds QueryNode_DumpSds(sds s, QueryParseCtx *q, QueryNode *qs, int depth) {
  s = doPad(s, depth);

  if (qs->fieldMask == 0) {
    s = sdscat(s, "@NULL:");
  }

  if (qs->fieldMask && qs->fieldMask != RS_FIELDMASK_ALL && qs->type != QN_NUMERIC &&
      qs->type != QN_GEO && qs->type != QN_IDS) {
    if (!q || !q->sctx->spec) {
      s = sdscatprintf(s, "@%" PRIu64, (uint64_t)qs->fieldMask);
    } else {
      s = sdscat(s, "@");
      t_fieldMask fm = qs->fieldMask;
      int i = 0, n = 0;
      while (fm) {
        t_fieldMask bit = (fm & 1) << i;
        if (bit) {
          char *f = GetFieldNameByBit(q->sctx->spec, bit);
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
      s = sdscatprintf(s, "%s%s\n", (char *)qs->tn.str, qs->tn.expanded ? "(expanded)" : "");
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
    case QN_TAG:
      s = sdscatprintf(s, "TAG:@%.*s {\n", (int)qs->tag.len, qs->tag.fieldName);
      for (int i = 0; i < qs->tag.numChildren; i++) {
        s = QueryNode_DumpSds(s, q, qs->tag.children[i], depth + 1);
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

  free(q->raw);
  free(q);
}
