
#include "geo_index.h"
#include "index.h"
#include "query.h"
#include "config.h"
#include "query_parser/parser.h"
#include "redis_index.h"
#include "tokenize.h"
#include "tag_index.h"
#include "err.h"
#include "concurrent_ctx.h"
#include "numeric_index.h"
#include "numeric_filter.h"
#include "module.h"

#include "extension.h"
#include "ext/default.h"

#include "rmutil/sds.h"

#include "util/logging.h"
#include "util/strconv.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define EFFECTIVE_FIELDMASK(q_, qn_) ((qn_)->opts.fieldMask & (q)->opts->fieldmask)

//---------------------------------------------------------------------------------------------

QueryTokenNode::~QueryTokenNode() {
  if (str) rm_free(str);
}

//---------------------------------------------------------------------------------------------

QueryTagNode::~QueryTagNode() {
  rm_free((char *)fieldName);
}

//---------------------------------------------------------------------------------------------

QueryLexRangeNode::~QueryLexRangeNode() {
  if (begin) rm_free(begin);
  if (end) rm_free(end);
}

//---------------------------------------------------------------------------------------------

// void _queryNumericNode_Free(QueryNumericNode *nn) { free(nn->nf); }

QueryNode::~QueryNode() {
  if (children) {
    for (size_t i = 0; i < NumChildren(); ++i) {
      delete children[i];
    }
    array_free(children);
    children = NULL;
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::ctor(QueryNodeType t) {
  type = t;
  opts = new QueryNodeOptions(RS_FIELDMASK_ALL, 0, -1, false, 1);
}

//---------------------------------------------------------------------------------------------

QueryTokenNode *QueryAST::NewTokenNodeExpanded(const char *s, size_t len, RSTokenFlags flags) {
  QueryTokenNode *ret = new QueryTokenNode(NULL, (char *)s, len, 1, flags);
  numTokens++;
  return ret;
}

//---------------------------------------------------------------------------------------------

void QueryAST::setFilterNode(QueryNode *n) {
  if (root == NULL || n == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator.
    root->children = array_ensure_prepend(root->children, &n, 1, QueryNode *);
    numTokens++;
  } else {  // for other types, we need to create a new phrase node
    QueryNode *nr = new QueryPhraseNode(0);
    nr->AddChild(n);
    nr->AddChild(root);
    numTokens++;
    root = nr;
  }
}

//---------------------------------------------------------------------------------------------

// Used only to support legacy FILTER keyword. Should not be used by newer code
void QueryAST::SetGlobalFilters(const NumericFilter *numeric) {
  QueryNumericNode *n(numeric);
  setFilterNode(n);
}

// Used only to support legacy GEOFILTER keyword. Should not be used by newer code
void QueryAST::SetGlobalFilters(const GeoFilter *geo) {
  QueryGeofilterNode *n(geo);
  setFilterNode(n);
}

// List of IDs to limit to, and the length of that array
void QueryAST::SetGlobalFilters(t_docId *ids, size_t nids) {
  QueryIdFilterNode *n(ids, nids);
  setFilterNode(n);
}

//---------------------------------------------------------------------------------------------

void QueryNode::Expand(RSQueryTokenExpander expander, RSQueryExpanderCtx *expCtx) {
  QueryNode *qn = this;
  // Do not expand verbatim nodes
  if (qn->opts.flags & QueryNode_Verbatim) {
    return;
  }

  int expandChildren = 0;

  if (qn->type == QN_TOKEN) {
    expCtx->currentNode = this;
    expander(expCtx, &qn);
  } else if (qn->type == QN_UNION ||
             (qn->type == QN_PHRASE && !qn->exact)) {  // do not expand exact phrases
    expandChildren = 1;
  }
  if (expandChildren) {
    for (size_t i = 0; i < qn->NumChildren(); ++i) {
      qn->children[i]->Expand(expander, expCtx);
    }
  }
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryTokenNode::Eval(Query *q) {
  // if (qn->type != QN_TOKEN) {
  //   return NULL;
  // }

  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index
  int isSingleWord = q->numTokens == 1 && q->opts->fieldmask == RS_FIELDMASK_ALL;

  RSQueryTerm *term = new RSQueryTerm(tok, q->tokenId++);

  // printf("Opening reader.. `%s` FieldMask: %llx\n", term->str, EFFECTIVE_FIELDMASK(q, qn));

  IndexReader *ir = Redis_OpenReader(q->sctx, term, q->docTable, isSingleWord,
                                     EFFECTIVE_FIELDMASK(q, qn), q->conc, qn->opts.weight);
  if (ir == NULL) {
    delete term;
    return NULL;
  }

  return ir->NewReadIterator();
}

//---------------------------------------------------------------------------------------------

static IndexIterator *iterateExpandedTerms(Query *q, Trie *terms, const char *str,
                                           size_t len, int maxDist, int prefixMode,
                                           QueryNodeOptions *opts) {
  TrieIterator *it = terms->Iterate(str, len, maxDist, prefixMode);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  size_t maxExpansions = q->sctx->spec->maxPrefixExpansions;
  while (it->Next(&rstr, &slen, NULL, &score, &dist) &&
         (itsSz < maxExpansions || maxExpansions == -1)) {

    // Create a token for the reader
    RSToken tok(NULL, 0, 0, 0);
    tok.str = runesToStr(rstr, slen, &tok.len);
    if (q->sctx && q->sctx->redisCtx) {
      RedisModule_Log(q->sctx->redisCtx, "debug", "Found fuzzy expansion: %s %f", tok.str, score);
    }

    RSQueryTerm *term = new RSQueryTerm(&tok, q->tokenId++);

    // Open an index reader
    IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                       q->opts->fieldmask & opts->fieldMask, q->conc, 1);

    if (!ir) {
      delete term;
      continue;
    }

    // Add the reader to the iterator array
    its[itsSz++] = NewReadIterator(ir);
    if (itsSz == itsCap) {
      itsCap *= 2;
      its = rm_realloc(its, itsCap * sizeof(*its));
    }
  }

  delete it->ctx;
  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    rm_free(its);
    return NULL;
  }
  return new UnionIterator(its, itsSz, q->docTable, 1, opts->weight);
}

//---------------------------------------------------------------------------------------------

/* Ealuate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them */
IndexIterator *QueryPrefixNode::Eval(Query *q) {
  // RS_LOG_ASSERT(type == QN_PREFX, "query node type should be prefix");

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (len < RSGlobalConfig.minTermPrefix) {
    return NULL;
  }
  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, str, len, 0, 1, &opts);
}

//---------------------------------------------------------------------------------------------

struct LexRangeCtx {
  IndexIterator **its;
  size_t nits;
  size_t cap;
  Query *q;
  QueryNodeOptions *opts;
  double weight;

  LexRangeCtx(Query q, QueryNodeOptions *opts, double weight = 0) :
  q(&q), opts(opts), weight(weight) {}
};

static void rangeItersAddIterator(LexRangeCtx *ctx, IndexReader *ir) {
  ctx->its[ctx->nits++] = ir->NewReadIterator();
  if (ctx->nits == ctx->cap) {
    ctx->cap *= 2;
    ctx->its = rm_realloc(ctx->its, ctx->cap * sizeof(*ctx->its));
  }
}

//---------------------------------------------------------------------------------------------

static void rangeIterCbStrs(const char *r, size_t n, void *p, void *invidx) {
  LexRangeCtx *ctx = p;
  Query *q = ctx->q;
  RSToken tok;
  tok.str = (char *)r;
  tok.len = n;
  RSQueryTerm *term = new RSQueryTerm(&tok, ctx->q->tokenId++);
  IndexReader *ir = new TermIndexReader(invidx, q->sctx->spec, RS_FIELDMASK_ALL, term, ctx->weight);
  if (!ir) {
    delete term;
    return;
  }

  rangeItersAddIterator(ctx, ir);
}

//---------------------------------------------------------------------------------------------

static void rangeIterCb(const rune *r, size_t n, void *p) {
  LexRangeCtx *ctx = p;
  Query *q = ctx->q;
  RSToken tok;
  tok.str = runesToStr(r, n, &tok.len);
  RSQueryTerm *term = new RSQueryTerm(&tok, ctx->q->tokenId++);
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                     q->opts->fieldmask & ctx->opts->fieldMask, q->conc, 1);
  rm_free(tok.str);
  if (!ir) {
    delete term;
    return;
  }

  rangeItersAddIterator(ctx, ir);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryLexRangeNode::Eval(Query *q) {
  Trie *t = q->sctx->spec->terms;
  LexRangeCtx ctx(q, &opts);

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  rune *rbegin = NULL, *rend = NULL;
  size_t nbegin, nend;
  if (begin) {
    begin = strToFoldedRunes(begin, &nbegin);
  }
  if (end) {
    end = strToFoldedRunes(end, &nend);
  }

  t->root->IterateRange(rbegin, rbegin ? nbegin : -1, includeBegin, rend,
                        rend ? nend : -1, includeEnd, rangeIterCb, &ctx);
  if (!ctx.its || ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return new UnionIterator(ctx.its, ctx.nits, q->docTable, 1, opts.weight);
  }
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryFuzzyNode::Eval(Query *q) {
  // RS_LOG_ASSERT(type == QN_FUZZY, "query node type should be fuzzy");

  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, pfx.str, pfx.len, maxDist, 0, &opts); //@@ Why can it be Fuzzy and Prefix?
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryPhraseNode::Eval(Query *q) {
  // if (qn->type != QN_PHRASE) {
  //   // printf("Not a phrase node!\n");
  //   return NULL;
  // }

  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (NumChildren() == 1) {
    children[0]->opts.fieldMask &= opts.fieldMask;
    return children[0]->EvalNode(q);
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(NumChildren(), sizeof(IndexIterator *));
  for (size_t ii = 0; ii < NumChildren(); ++ii) {
    children[ii]->opts.fieldMask &= opts.fieldMask;
    iters[ii] = children[ii]->EvalNode(q);
  }
  IndexIterator *ret;

  if (exact) {
    ret = NewIntersecIterator(iters, NumChildren(), q->docTable,
                              EFFECTIVE_FIELDMASK(q, this), 0, 1, opts.weight);
  } else {
    // Let the query node override the slop/order parameters
    int slop = opts.maxSlop;
    if (slop == -1) slop = q->opts->slop;

    // Let the query node override the inorder of the whole query
    int inOrder = q->opts->flags & Search_InOrder;
    if (opts.inOrder) inOrder = 1;

    // If in order was specified and not slop, set slop to maximum possible value.
    // Otherwise we can't check if the results are in order
    if (inOrder && slop == -1) {
      slop = __INT_MAX__;
    }

    ret = NewIntersecIterator(iters, NumChildren(), q->docTable,
                              EFFECTIVE_FIELDMASK(q, this), slop, inOrder, opts.weight);
  }
  return ret;
}

IndexIterator *QueryWildcardNode::EvalNode(Query *q) {
  if (!q->docTable) {
    return NULL;
  }

  return NewWildcardIterator(q->docTable->maxDocId);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryNotNode::Eval(Query *q) {
  // if (qn->type != QN_NOT) {
  //   return NULL;
  // }

  return NewNotIterator(NumChildren() ? children[0]->EvalNode(q) : NULL,
                        q->docTable->maxDocId, opts.weight);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryOptionalNode::Eval(Query *q) {
  // if (type != QN_OPTIONAL) {
    // return NULL;
  // }
  // QueryOptionalNode *node = &qn->opt;

  return NewOptionalIterator(NumChildren() ? children[0]->EvalNode(q) : NULL,
                             q->docTable->maxDocId, opts.weight);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryNumericNode::Eval(Query *q) {
  const FieldSpec *fs =
      q->sctx->spec->GetField(nf->fieldName, strlen(nf->fieldName));
  if (!fs || !fs->IsFieldType(INDEXFLD_T_NUMERIC)) {
    return NULL;
  }

  return NewNumericFilterIterator(q->sctx, nf, q->conc);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryGeofilterNode::Eval(Query *q, double weight) {
  const FieldSpec *fs =
      q->sctx->spec->GetField(gf->property, strlen(gf->property));
  if (!fs || !fs->IsFieldType(INDEXFLD_T_GEO)) {
    return NULL;
  }

  GeoIndex gi(q->sctx, fs);
  return NewGeoRangeIterator(&gi, gf, weight);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryIdFilterNode::Eval(Query *q) {
  return NewIdListIterator(ids, len, 1);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryUnionNode::Eval(Query *q) {
  // if (qn->type != QN_UNION) {
  //   return NULL;
  // }

  // a union stage with one child is the same as the child, so we just return it
  if (NumChildren() == 1) {
    return children[0]->EvalNode(q);
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(NumChildren(), sizeof(IndexIterator *));
  int n = 0;
  for (size_t i = 0; i < NumChildren(); ++i) {
    children[i]->opts.fieldMask &= opts.fieldMask;
    IndexIterator *it = children[i]->EvalNode(q);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    return NULL;
  }

  if (n == 1) {
    IndexIterator *ret = iters[0];
    return ret;
  }

  return new UnionIterator(iters, n, q->docTable, 0, opts.weight);
}

//---------------------------------------------------------------------------------------------

typedef IndexIterator **IndexIteratorArray;

IndexIterator *QueryLexRangeNode::EvalTagLexRangeNode(Query *q, TagIndex *idx, IndexIteratorArray *iterout,
                                                      double weight) {
  TrieMap *t = idx->values;
  LexRangeCtx ctx(q, &opts, weight);

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  const char *begin_ = begin, *end_ = end;
  int nbegin = begin_ ? strlen(begin_) : -1, nend = end_ ? strlen(end_) : -1;

  t->IterateRange(begin_, nbegin, includeBegin, end_, nend, includeEnd,
                  rangeIterCbStrs, &ctx);
  if (ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return new UnionIterator(ctx.its, ctx.nits, q->docTable, 1, opts.weight);
  }
}

//---------------------------------------------------------------------------------------------

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
IndexIterator *QueryPrefixNode::EvalTagPrefixNode(Query *q, TagIndex *idx,
                                                  IndexIteratorArray *iterout, double weight) {
  // if (type != QN_PREFX) {
  //   return NULL;
  // }

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (pfx.len < q->sctx->spec->minPrefix) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  TrieMapIterator *it = idx->values->Iterate(pfx.str, pfx.len);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  char *s;
  tm_len_t sl;
  void *ptr;

  // Find all completions of the prefix
  size_t maxExpansions = q->sctx->spec->maxPrefixExpansions;
  while (it->Next(&s, &sl, &ptr) &&
         (itsSz < maxExpansions || maxExpansions == -1)) {
    IndexIterator *ret = idx->OpenReader(q->sctx->spec, s, sl, 1);
    if (!ret) continue;

    // Add the reader to the iterator array
    its[itsSz++] = ret;
    if (itsSz == itsCap) {
      itsCap *= 2;
      its = rm_realloc(its, itsCap * sizeof(*its));
    }
  }

  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    rm_free(its);
    return NULL;
  }

  *iterout = array_ensure_append(*iterout, its, itsSz, IndexIterator *);
  return new UnionIterator(its, itsSz, q->docTable, 1, weight);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryNode::EvalSingleTagNode(Query *q, TagIndex *idx, IndexIteratorArray *iterout,
                                            double weight) {
  IndexIterator *ret = NULL;
  switch (type) {
    case QN_TOKEN: {
      // ret = idx->OpenReader(q->sctx->spec, tn.str, tn.len, weight);
      ret = EvalSingle(q, idx, iterout, weight);
      break;
    }
    case QN_PREFX:
      return EvalTagPrefixNode(q, idx, iterout, weight);

    case QN_LEXRANGE:
      return EvalTagLexRangeNode(q, idx, iterout, weight);

    case QN_PHRASE: {
      // char *terms[NumChildren()];
      // for (size_t i = 0; i < NumChildren(); ++i) {
      //   if (children[i]->type == QN_TOKEN) {
      //     terms[i] = children[i]->tn.str;
      //   } else {
      //     terms[i] = "";
      //   }
      // }

      // sds s = sdsjoin(terms, NumChildren(), " ");

      // ret = idx->OpenReader(q->sctx->spec, s, sdslen(s), weight);
      // sdsfree(s);

      ret = EvalSingle(q, idx, iterout, weight);
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

//---------------------------------------------------------------------------------------------

IndexIterator *QueryTagNode::Eval(Query *q) {
  // if (qn->type != QN_TAG) {
  //   return NULL;
  // }
  QueryTagNode *node = &tag;
  RedisModuleKey *k = NULL;
  const FieldSpec *fs =
      q->sctx->spec->GetFieldCase(node->fieldName, strlen(node->fieldName));
  if (!fs) {
    return NULL;
  }
  RedisModuleString *kstr = q->sctx->spec->GetFormattedKey(fs, INDEXFLD_T_TAG);
  TagIndex *idx = q->sctxOpen(kstr, 0, &k);

  IndexIterator **total_its = NULL;
  IndexIterator *ret = NULL;

  if (!idx) {
    goto done;
  }
  // a union stage with one child is the same as the child, so we just return it
  if (NumChildren() == 1) {
    ret = children[0]->EvalSingleTagNode(q, idx, &total_its, opts.weight);
    if (ret) {
      if (q->conc) {
        idx->RegisterConcurrentIterators(q->conc, k, kstr, (array_t *)total_its);
        k = NULL;  // we passed ownershit
      } else {
        array_free(total_its);
      }
    }
    goto done;
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(NumChildren(), sizeof(IndexIterator *));
  size_t n = 0;
  for (size_t i = 0; i < NumChildren(); i++) {
    IndexIterator *it =
        children[i]->EvalSingleTagNode(q, idx, &total_its, opts.weight);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    goto done;
  }

  if (total_its) {
    if (q->conc) {
      idx->RegisterConcurrentIterators(q->conc, k, kstr, (array_t *)total_its);
      k = NULL;  // we passed ownershit
    } else {
      array_free(total_its);
    }
  }

  ret = new UnionIterator(iters, n, q->docTable, 0, opts.weight);

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryParse::QueryParse(char *query, size_t nquery, const RedisSearchCtx &sctx_,
                       const RSSearchOptions &opts_, QueryError *status_) {
  raw =  query;
  len = nquery;
  sctx = (RedisSearchCtx *)&sctx_;
  opts = &opts_;
  status = status_;
}

//---------------------------------------------------------------------------------------------

/**
 * Parse the query string into an AST.
 * @param dst the AST structure to populate
 * @param sctx the context - this is never written to or retained
 * @param sopts options modifying parsing behavior
 * @param qstr the query string
 * @param len the length of the query string
 * @param status error details set here.
 */

QueryAST::QueryAST(const RedisSearchCtx &sctx, const RSSearchOptions &opts,
                   const char *q, size_t n, QueryError *status) {
  query = rm_strndup(q, n);
  nquery = n;

  QueryParse qp(query, nquery, sctx, opts, status);

  root = qp.ParseRaw();
  // printf("Parsed %.*s. Error (Y/N): %d. Root: %p\n", (int)n, q, status->HasError(), root);
  if (!root) {
    if (status->HasError()) {
      throw Error(status);
    }
    root = new QueryNode();
  }
  if (status->HasError()) {
    if (root) {
      delete root;
    }
    throw Error(status);
  }
  numTokens = qp.numTokens;
}

//---------------------------------------------------------------------------------------------

Query::Query(QueryAST &ast, const RSSearchOptions *opts_, RedisSearchCtx *sctx_,
             ConcurrentSearchCtx *conc_) {
  conc = conc_;
  opts = opts_;
  numTokens = ast.numTokens;
  docTable = &sctx_->spec->docs;
  sctx = sctx_;
}

//---------------------------------------------------------------------------------------------

/**
 * Open the result iterator on the filters. Returns the iterator for the root node.
 *
 * @param ast the parsed tree
 * @param opts options
 * @param sctx the search context. Note that this may be retained by the iterators
 *  for the remainder of the query.
 * @param conc Used to save state on the query
 * @return an iterator.
 */

IndexIterator *QueryAST::Iterate(const RSSearchOptions &opts, RedisSearchCtx &sctx,
                                 ConcurrentSearchCtx &conc) const {
  Query query(*this, &opts, &sctx, conc);
  IndexIterator *iter = query.Eval(root);
  if (!iter) {
    // Return the dummy iterator
    iter = NewEmptyIterator();
  }
  return iter;
}

//---------------------------------------------------------------------------------------------

QueryAST::~QueryAST() {
  delete root;
  numTokens = 0;
  rm_free(query);
  nquery = 0;
  query = NULL;
}

//---------------------------------------------------------------------------------------------

/**
 * Expand the query using a pre-registered expander. Query expansion possibly
 * modifies or adds additional search terms to the query.
 * @param q the query
 * @param expander the name of the expander
 * @param opts query options, passed to the expander function
 * @param status error detail
 * @return REDISMODULE_OK, or REDISMODULE_ERR with more detail in `status`
 */

int QueryAST::Expand(const char *expander, RSSearchOptions *opts, RedisSearchCtx &sctx,
                     QueryError *status) {
  if (!root) {
    return REDISMODULE_OK;
  }
  RSQueryExpanderCtx expCtx = {
      qast: = q, language: opts->language, handle: &sctx, status: status};

  ExtQueryExpander *xpc =
      Extensions::GetQueryExpander(&expCtx, expander ? expander : DEFAULT_EXPANDER_NAME);
  if (xpc && xpc->exp) {
    root->Expand(xpc->exp, &expCtx);
    if (xpc->ff) {
      xpc->ff(expCtx.privdata);
    }
  }
  if (status->HasError()) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

// Set the field mask recursively on a query node. This is called by the parser to handle
// situations like @foo:(bar baz|gaz), where a complex tree is being applied a field mask.
void QueryNode::SetFieldMask(t_fieldMask mask) {
  if (!this) return;
  opts.fieldMask &= mask;
  for (size_t ii = 0; ii < NumChildren(); ++ii) {
    children[ii]->SetFieldMask(mask);
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::AddChildren(QueryNode **children_, size_t nchildren) {
  if (type == QN_TAG) {
    for (size_t ii = 0; ii < nchildren; ++ii) {
      if (children_[ii]->type == QN_TOKEN || children_[ii]->type == QN_PHRASE ||
          children_[ii]->type == QN_PREFX || children_[ii]->type == QN_LEXRANGE) {
        children = array_ensure_append(children, children_ + ii, 1, QueryNode *);
      }
    }
  } else {
    array_ensure_append(children, children_, nchildren, QueryNode *);
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::AddChild(QueryNode *ch) {
  AddChildren(&ch, 1);
}

//---------------------------------------------------------------------------------------------

void QueryNode::ClearChildren(bool shouldFree) {
  if (shouldFree) {
    for (size_t ii = 0; ii < NumChildren(); ++ii) {
      delete children[ii];
    }
  }
  if (NumChildren()) {
    array_clear(children);
  }
}

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

sds QueryNode::DumpSds(sds s, const IndexSpec *spec, int depth) const {
  s = doPad(s, depth);

  if (opts.fieldMask == 0) {
    s = sdscat(s, "@NULL:");
  }

  if (opts.fieldMask && opts.fieldMask != RS_FIELDMASK_ALL && type != QN_NUMERIC &&
      type != QN_GEO && type != QN_IDS) {
    if (!spec) {
      s = sdscatprintf(s, "@%" PRIu64, (uint64_t)opts.fieldMask);
    } else {
      s = sdscat(s, "@");
      t_fieldMask fm = opts.fieldMask;
      int i = 0, n = 0;
      while (fm) {
        t_fieldMask bit = (fm & 1) << i;
        if (bit) {
          const char *f = spec->GetFieldNameByBit(bit);
          s = sdscatprintf(s, "%s%s", n ? "|" : "", f ? f : "n/a");
          n++;
        }
        fm = fm >> 1;
        i++;
      }
    }
    s = sdscat(s, ":");
  }

  s = dumpsds(s, spec, depth);

  s = sdscat(s, "}");
  // print attributes if not the default
  if (opts.weight != 1 || opts.maxSlop != -1 || opts.inOrder) {
    s = sdscat(s, " => {");
    if (opts.weight != 1) {
      s = sdscatprintf(s, " $weight: %g;", opts.weight);
    }
    if (opts.maxSlop != -1) {
      s = sdscatprintf(s, " $slop: %d;", opts.maxSlop);
    }
    if (opts.inOrder || opts.maxSlop != -1) {
      s = sdscatprintf(s, " $inorder: %s;", opts.inOrder ? "true" : "false");
    }
    s = sdscat(s, " }");
  }
  s = sdscat(s, "\n");
  return s;
}

//---------------------------------------------------------------------------------------------

sds QueryNode::DumpChildren(sds s, const IndexSpec *spec, int depth) const {
  for (size_t ii = 0; ii < NumChildren(); ++ii) {
    s = children[ii]->DumpSds(s, spec, depth);
  }
  return s;
}

//---------------------------------------------------------------------------------------------

/* Return a string representation of the query parse tree. The string should be freed by the
 * caller
 */
char *QueryAST::DumpExplain(const IndexSpec *spec) const {
  // empty query
  if (!root) {
    return rm_strdup("NULL");
  }

  sds s = root->DumpSds(sdsnew(""), spec, 0);
  char *ret = rm_strndup(s, sdslen(s));
  sdsfree(s);
  return ret;
}

//---------------------------------------------------------------------------------------------

void QueryAST::Print(const IndexSpec *spec) const {
  sds s = root->DumpSds(sdsnew(""), spec, 0);
  printf("%s\n", s);
  sdsfree(s);
}

//---------------------------------------------------------------------------------------------

int QueryNode::ForEach(ForEachCallback callback, void *ctx, bool reverse) {
#define INITIAL_ARRAY_NODE_SIZE 5
  QueryNode **nodes = array_new(QueryNode *, INITIAL_ARRAY_NODE_SIZE);
  nodes = array_append(nodes, this);
  int retVal = 1;
  while (array_len(nodes) > 0) {
    QueryNode *curr = array_pop(nodes);
    if (!callback(curr, this, ctx)) {
      retVal = 0;
      break;
    }
    if (reverse) {
      for (size_t ii = curr->NumChildren(); ii; --ii) {
        nodes = array_append(nodes, curr->children[ii - 1]);
      }
    } else {
      for (size_t ii = 0; ii < curr->NumChildren(); ++ii) {
        nodes = array_append(nodes, curr->children[ii]);
      }
    }
  }

  array_free(nodes);
  return retVal;
}

//---------------------------------------------------------------------------------------------

bool QueryNode::ApplyAttribute(QueryAttribute *attr, QueryError *status) {

#define MK_INVALID_VALUE()                                                         \
  status->SetErrorFmt(QUERY_ESYNTAX, "Invalid value (%.*s) for `%.*s`", \
                         (int)attr->vallen, attr->value, (int)attr->namelen, attr->name)

  // Apply slop: [-1 ... INF]
  if (STR_EQCASE(attr->name, attr->namelen, "slop")) {
    long long n;
    if (!ParseInteger(attr->value, &n) || n < -1) {
      MK_INVALID_VALUE();
      return false;
    }
    opts.maxSlop = n;

  } else if (STR_EQCASE(attr->name, attr->namelen, "inorder")) {
    // Apply inorder: true|false
    bool b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return false;
    }
    opts.inOrder = b;

  } else if (STR_EQCASE(attr->name, attr->namelen, "weight")) {
    // Apply weight: [0  ... INF]
    double d;
    if (!ParseDouble(attr->value, &d) || d < 0) {
      MK_INVALID_VALUE();
      return false;
    }
    opts.weight = d;

  } else if (STR_EQCASE(attr->name, attr->namelen, "phonetic")) {
    // Apply phonetic: true|false
    bool b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return false;
    }
    if (b) {
      opts.phonetic = PHONETIC_ENABLED;  // means we specifically asked for phonetic matching
    } else {
      opts.phonetic =
          PHONETIC_DESABLED;  // means we specifically asked no for phonetic matching
    }
    // opts.noPhonetic = PHONETIC_DEFAULT -> means no special asks regarding phonetics
    //                                          will be enable if field was declared phonetic

  } else {
    status->SetErrorFmt(QUERY_ENOOPTION, "Invalid attribute %.*s", (int)attr->namelen,
                           attr->name);
    return false;
  }

  return true;
}

//---------------------------------------------------------------------------------------------

bool QueryNode::ApplyAttributes(QueryAttribute *attrs, size_t len, QueryError *status) {
  for (size_t i = 0; i < len; i++) {
    if (!ApplyAttribute(&attrs[i], status)) {
      return false;
    }
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
