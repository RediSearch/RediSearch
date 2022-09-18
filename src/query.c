
#include "geo_index.h"
#include "index.h"
#include "query.h"
#include "config.h"
#include "query_parser/parser.h"
#include "redis_index.h"
#include "tokenize.h"
#include "tag_index.h"
#include "id_list.h"
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

RSToken::RSToken(const rune *r, size_t n) {
  str = runesToStr(r, n);
}

//---------------------------------------------------------------------------------------------

RSToken::RSToken(const Runes &r) {
  str = r.toUTF8();
}

///////////////////////////////////////////////////////////////////////////////////////////////

#define EFFECTIVE_FIELDMASK(q_, qn_) ((qn_)->opts.fieldMask & (q)->opts->fieldmask)

//---------------------------------------------------------------------------------------------

QueryLexRangeNode::~QueryLexRangeNode() {
  if (begin) rm_free(begin);
  if (end) rm_free(end);
}

//---------------------------------------------------------------------------------------------

// void _queryNumericNode_Free(QueryNumericNode *nn) { free(nn->nf); }

QueryNode::~QueryNode() {
  for (auto chi: children) {
    delete chi;
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::ctor(QueryNodeType t) {
  type = t;
}

///////////////////////////////////////////////////////////////////////////////////////////////

QueryTokenNode *QueryAST::NewTokenNodeExpanded(std::string_view s, RSTokenFlags flags) {
  QueryTokenNode *ret = new QueryTokenNode(NULL, s, 1, flags);
  numTokens++;
  return ret;
}

//---------------------------------------------------------------------------------------------

void QueryAST::setFilterNode(QueryNode *n) {
  if (root == NULL || n == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator
    root->children.insert(root->children.begin(), n);
  } else {
    // for other types, we need to create a new phrase node
    QueryNode *nr = new QueryPhraseNode(0);
    nr->AddChild(n);
    nr->AddChild(root);
    root = nr;
  }
  ++numTokens;
}

//---------------------------------------------------------------------------------------------

// Used only to support legacy FILTER keyword. Should not be used by newer code
void QueryAST::SetGlobalFilters(const NumericFilter *numeric) {
  setFilterNode(new QueryNumericNode{numeric});
}

// Used only to support legacy GEOFILTER keyword. Should not be used by newer code
void QueryAST::SetGlobalFilters(const GeoFilter *geo) {
  setFilterNode(new QueryGeofilterNode{geo});
}

// List of IDs to limit to, and the length of that array
void QueryAST::SetGlobalFilters(Vector<t_docId> &ids) {
  setFilterNode(new QueryIdFilterNode{ids});
}

//---------------------------------------------------------------------------------------------

void QueryNode::Expand(QueryExpander *expander) {
  // Do not expand verbatim nodes
  if (opts.flags & QueryNode_Verbatim) {
    return;
  }

  if (!expandChildren()) return;
  for (auto chi: children) {
    chi->Expand(expander);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryTokenNode::EvalNode(Query *q) {
  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index
  int isSingleWord = q->numTokens == 1 && q->opts->fieldmask == RS_FIELDMASK_ALL;
  RSQueryTerm *term = new RSQueryTerm(tok, q->tokenId++);

  IndexReader *ir = Redis_OpenReader(q->sctx, term, q->docTable, isSingleWord,
                                     EFFECTIVE_FIELDMASK(q, this), q->conc, opts.weight);
  if (ir == NULL) {
    delete term;
    return NULL;
  }

  return ir->NewReadIterator();
}

//---------------------------------------------------------------------------------------------

void QueryTokenNode::Expand(QueryExpander *expander) {
  // Do not expand verbatim nodes
  if (opts.flags & QueryNode_Verbatim) return;

  expander->currentNode = this;
  expander->Expand(&tok);
}

///////////////////////////////////////////////////////////////////////////////////////////////

static IndexIterator *iterateExpandedTerms(Query *q, Trie *terms, std::string_view str,
                                           int maxDist, int prefixMode, QueryNodeOptions *opts) {
  TrieIterator it = terms->Iterate(str.data(), maxDist, prefixMode); //@@@TODO: str.data() is not safe
  IndexIterators its;

  Runes runes;
  float score = 0;
  int dist = 0;
  RSPayload payload;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  size_t maxExpansions = q->sctx->spec->maxPrefixExpansions;
  while (it.Next(runes, payload, score, &dist) && (its.size() < maxExpansions || maxExpansions == -1)) {
    // Create a token for the reader
    RSToken tok(runes);
    if (q->sctx && q->sctx->redisCtx) {
      RedisModule_Log(q->sctx->redisCtx, "debug", "Found fuzzy expansion: %s %f", tok.str.data(), score);
    }

    RSQueryTerm *term = new RSQueryTerm(tok, q->tokenId++);

    // Open an index reader
    IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                       q->opts->fieldmask & opts->fieldMask, q->conc, 1);

    if (!ir) {
      delete term;
      continue;
    }

    // Add the reader to the iterator array
    its.push_back(ir->NewReadIterator());
  }

  return new UnionIterator(its, q->docTable, 1, opts->weight);
}

//---------------------------------------------------------------------------------------------

// Ealuate a prefix node by expanding all its possible matches and creating one big
// UNION on all of them.

IndexIterator *QueryPrefixNode::EvalNode(Query *q) {
  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (tok.length() < RSGlobalConfig.minTermPrefix) {
    return NULL;
  }

  Trie *terms = q->sctx->spec->terms;
  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, tok.str, 0, true, &opts);
}

//---------------------------------------------------------------------------------------------

void LexRange::rangeItersAddIterator(IndexReader *ir) {
  its.push_back(ir->NewReadIterator());
}

//---------------------------------------------------------------------------------------------

static void rangeIterCbStrs(const char *r, size_t n, void *p, void *invidx) {
  LexRange *ctx = p;
  Query *q = ctx->q;
  RSToken tok{r, n};
  RSQueryTerm *term = new RSQueryTerm(tok, ctx->q->tokenId++);
  IndexReader *ir = new TermIndexReader(invidx, q->sctx->spec, RS_FIELDMASK_ALL, term, ctx->weight);
  if (!ir) {
    delete term;
    return;
  }

  ctx->rangeItersAddIterator(ir);
}

//---------------------------------------------------------------------------------------------

static void rangeIterCb(const rune *r, size_t n, void *p)  {
  LexRange *ctx = p;
  Query *q = ctx->q;
  RSToken tok{r, n};
  RSQueryTerm *term = new RSQueryTerm(tok, q->tokenId++);
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                     q->opts->fieldmask & ctx->opts->fieldMask, q->conc, 1);
  if (!ir) {
    delete term;
    return;
  }

  ctx->rangeItersAddIterator(ir);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryLexRangeNode::EvalNode(Query *q) {
  Trie *t = q->sctx->spec->terms;
  if (!t) {
    return NULL;
  }

  LexRange range(q, &opts);
  Runes rbegin(begin), rend(end);

  t->root->IterateRange(rbegin, includeBegin, rend, includeEnd, rangeIterCb, &range);
  /*t->root->IterateRange(rbegin, includeBegin, rend, includeEnd, [&](const char *r, size_t n) {
      LexRange *ctx = p;
      Query *q = range.q;
      RSToken tok{r, n};
      RSQueryTerm *term = new RSQueryTerm(tok, range.q->tokenId++);
      IndexReader *ir = new TermIndexReader(invidx, q->sctx->spec, RS_FIELDMASK_ALL, term, range.weight);
      if (!ir) {
        delete term;
        return;
      }
      range.rangeItersAddIterator(ir);
    });*/
  if (range.its.empty()) {
    return NULL;
  }
  return new UnionIterator(range.its, q->docTable, 1, opts.weight);
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryFuzzyNode::EvalNode(Query *q) {
  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, tok.str, maxDist, false, &opts);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryPhraseNode::EvalNode(Query *q) {
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (NumChildren() == 1) {
    children[0]->opts.fieldMask &= opts.fieldMask;
    return children[0]->EvalNode(q);
  }

  // recursively eval the children
  IndexIterators iters;// = rm_calloc(NumChildren(), sizeof(IndexIterator *));
  for (size_t i = 0; i < NumChildren(); ++i) {
    children[i]->opts.fieldMask &= opts.fieldMask;
    iters.push_back(children[i]->EvalNode(q));
  }

  IndexIterator *ret;
  if (exact) {
    ret = new IntersectIterator(iters, q->docTable, EFFECTIVE_FIELDMASK(q, this), 0, 1, opts.weight);
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

    ret = new IntersectIterator(iters, q->docTable, EFFECTIVE_FIELDMASK(q, this), slop, inOrder, opts.weight);
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryWildcardNode::EvalNode(Query *q) {
  if (!q->docTable) return NULL;
  return new WildcardIterator{q->docTable->maxDocId};
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryPhraseNode::EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight) {
  char *terms[NumChildren()];
  for (size_t i = 0; i < NumChildren(); ++i) {
    if (children[i]->type == QN_TOKEN) {
      TermResult *res = dynamic_cast<TermResult*>(children[i]);
      terms[i] = res->term->str.c_str();
    } else {
      terms[i] = "";
    }
  }

  sds s = sdsjoin(terms, NumChildren(), " ");
  return idx->OpenReader(q->sctx->spec, std::string_view{s, sdslen(s)}, weight);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryNotNode::EvalNode(Query *q) {
  return new NotIterator(NumChildren() ? children[0]->EvalNode(q) : NULL,
                         q->docTable->maxDocId, opts.weight);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryOptionalNode::EvalNode(Query *q) {
  return new OptionalIterator(NumChildren() ? children[0]->EvalNode(q) : NULL,
                              q->docTable->maxDocId, opts.weight);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryNumericNode::EvalNode(Query *q) {
  const FieldSpec *fs =
      q->sctx->spec->GetField(nf->fieldName);
  if (!fs || !fs->IsFieldType(INDEXFLD_T_NUMERIC)) {
    return NULL;
  }

  return NewNumericFilterIterator(q->sctx, nf, q->conc);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryGeofilterNode::Eval(Query *q, double weight) {
  const FieldSpec *fs = q->sctx->spec->GetField(gf->property);
  if (!fs || !fs->IsFieldType(INDEXFLD_T_GEO)) {
    return NULL;
  }

  GeoIndex gi(q->sctx, *fs);
  return gi.NewGeoRangeIterator(*gf, weight);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryIdFilterNode::EvalNode(Query *q) {
  return new IdListIterator(ids, 1);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryUnionNode::EvalNode(Query *q) {
  // a union stage with one child is the same as the child, so we just return it
  if (children.size() == 1) {
    return children.front()->EvalNode(q);
  }

  // recursively eval the children
  IndexIterators iters(children.size());
  for (auto chi: children) {
    chi->opts.fieldMask &= opts.fieldMask;
    IndexIterator *it = chi->EvalNode(q);
    if (it) {
      iters.push_back(it);
    }
  }
  if (iters.empty()) {
    return NULL;
  }

  if (iters.size() == 1) {
    return iters[0];
  }

  return new UnionIterator(iters, q->docTable, 0, opts.weight);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryLexRangeNode::EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight) {
  TrieMap *t = idx->values;
  if (!t) {
    return NULL;
  }

  LexRange range(q, &opts, weight);
  const char *begin_ = begin, *end_ = end;
  int nbegin = begin_ ? strlen(begin_) : -1, nend = end_ ? strlen(end_) : -1;

  t->IterateRange(begin_, nbegin, includeBegin, end_, nend, includeEnd, rangeIterCbStrs, &range);
  if (range.its.empty()) {
    return NULL;
  } else {
    return new UnionIterator(range.its, q->docTable, 1, opts.weight);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Evaluate a tag prefix by expanding it with a lookup on the tag index

IndexIterator *QueryPrefixNode::EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight) {
  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (tok.length() < q->sctx->spec->minPrefix) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  TrieMapIterator *it = idx->values->Iterate(tok.str);
  if (!it) return NULL;

  IndexIterators its;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  char *s;
  tm_len_t sl;
  void *ptr;

  // Find all completions of the prefix
  size_t maxExpansions = q->sctx->spec->maxPrefixExpansions;
  while (it->Next(&s, &sl, &ptr) && (its.size() < maxExpansions || maxExpansions == -1)) {
    IndexIterator *ret = idx->OpenReader(q->sctx->spec, std::string_view{s, sl}, true);
    if (!ret) continue;

    // Add the reader to the iterator array
    its.push_back(ret);
  }

  iterout.insert(iterout.end(), its.begin(), its.end()); // concatenate the vectors
  return new UnionIterator(its, q->docTable, 1, weight);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryNode::EvalSingleTagNode(Query *q, TagIndex *idx, IndexIterators iterout,
                                            double weight) {
  IndexIterator *ret = EvalSingle(q, idx, iterout, weight);
  if (ret) {
    iterout.push_back(ret);
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator *QueryTagNode::EvalNode(Query *q) {
  RedisModuleKey *k = NULL;
  size_t n = 0;
  const FieldSpec *fs = q->sctx->spec->GetFieldCase(fieldName);
  if (!fs) {
    return NULL;
  }

  RedisModuleString *kstr = q->sctx->spec->GetFormattedKey(*fs, INDEXFLD_T_TAG);
  TagIndex *idx = TagIndex::Open(q->sctx, kstr, 0, &k);
  IndexIterator *ret = NULL;
  IndexIterators total_its;
  IndexIterators iters;

  if (!idx) {
    goto done;
  }
  // a union stage with one child is the same as the child, so we just return it
  if (NumChildren() == 1) {
    ret = children[0]->EvalSingleTagNode(q, idx, total_its, opts.weight);
    if (ret) {
      if (q->conc) {
        idx->RegisterConcurrentIterators(q->conc, k, kstr, total_its);
        k = NULL;  // we passed ownership
      }
    }
    goto done;
  }

  // recursively eval the children
  for (auto chi: children) {
    IndexIterator *it = chi->EvalSingleTagNode(q, idx, total_its, opts.weight);
    if (it) {
      iters.push_back(it);
    }
  }
  if (iters.empty()) {
    goto done;
  }

  if (!total_its.empty()) {
    if (q->conc) {
      idx->RegisterConcurrentIterators(q->conc, k, kstr, total_its);
      k = NULL;  // we passed ownershit
    }
  }

  ret = new UnionIterator(iters, q->docTable, 0, opts.weight);

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

QueryParse::QueryParse(char *query, size_t nquery, const RedisSearchCtx &sctx_,
                       const RSSearchOptions &opts_, QueryError *status_) {
  parser = NULL;
  raw =  query;
  len = nquery;
  numTokens = 0;
  sctx = (RedisSearchCtx *)&sctx_;
  root = NULL;
  opts = &opts_;
  status = status_;
}

///////////////////////////////////////////////////////////////////////////////////////////////

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
                   std::string_view query, QueryError *status) : query(query) {
  QueryParse qp(query.data(), query.length(), sctx, opts, status);

  root = qp.ParseRaw();
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

///////////////////////////////////////////////////////////////////////////////////////////////

Query::Query(const QueryAST &ast, const RSSearchOptions *opts, RedisSearchCtx *sctx, ConcurrentSearch *conc) :
  conc(conc), opts(opts), numTokens(ast.numTokens), docTable(&sctx->spec->docs), sctx(sctx) {}

//---------------------------------------------------------------------------------------------

IndexIterator *Query::Eval(QueryNode *node) {
  return node->EvalNode(this);
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
                                 ConcurrentSearch *conc) const {
  Query query{*this, &opts, &sctx, conc};
  IndexIterator *iter = query.Eval(root);
  if (!iter) {
    iter = new EmptyIterator();
  }
  return iter;
}

//---------------------------------------------------------------------------------------------

QueryAST::~QueryAST() {
  delete root;
}

//---------------------------------------------------------------------------------------------

/**
 * Expand the query using a pre-registered expander.
 * Query expansion possibly modifies or adds additional search terms to the query.
 * @param q the query
 * @param expander the name of the expander
 * @param opts query options, passed to the expander function
 * @param status error detail
 * @return REDISMODULE_OK, or REDISMODULE_ERR with more detail in `status`
 */

int QueryAST::Expand(const char *expanderName, RSSearchOptions *opts, RedisSearchCtx &sctx,
                     QueryError *status) {
  if (!root) return REDISMODULE_OK;

  QueryExpander::Factory factory = g_ext.GetQueryExpander(expanderName ? expanderName : DEFAULT_EXPANDER_NAME);
  if (factory) {
    QueryExpander *expander = factory(this, sctx, opts->language, status);
    root->Expand(expander);
    delete expander;
  }
  return status->HasError() ? REDISMODULE_ERR : REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Set the field mask recursively on a query node. This is called by the parser to handle
// situations like @foo:(bar baz|gaz), where a complex tree is being applied a field mask.

void QueryNode::SetFieldMask(t_fieldMask mask) {
  opts.fieldMask &= mask;
  for (auto chi: children) {
    chi->SetFieldMask(mask);
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::AddChildren(QueryNodes &children_) {
  if (type == QN_TAG) {
    for (auto child : children_) {
      if (child->type == QN_TOKEN || child->type == QN_PHRASE ||
          child->type == QN_PREFX || child->type == QN_LEXRANGE) {
        children.push_back(child);
      }
    }
  } else {
    children.insert(children.begin(), children_.begin(), children_.end());
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::AddChild(QueryNode *ch) {
  children.push_back(ch);
}

//---------------------------------------------------------------------------------------------

void QueryNode::ClearChildren(bool shouldFree) { //@@ Do we need the param here?
  if (shouldFree) {
    children.clear();
  }
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

///////////////////////////////////////////////////////////////////////////////////////////////

// Return a string representation of the query parse tree.
// The string should be freed by the caller.

String QueryAST::DumpExplain(const IndexSpec *spec) const {
  // empty query
  if (!root) {
    return String{"NULL"};
  }

  sds s = root->DumpSds(sdsnew(""), spec, 0);
  String ret{s, sdslen(s)};
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
    if (!callback(curr, ctx)) {
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

bool QueryNode::ApplyAttribute(QueryAttribute &attr, QueryError *status) {
  static std::string_view slop{"slop"}, inorder("inorder"), weight{"weight"}, phonetic{"phonetic"};

#define MK_INVALID_VALUE() \
  status->SetErrorFmt(QUERY_ESYNTAX, "Invalid value (%.*s) for `%.*s`", \
                      (int)attr.value.length(), attr.value.data(), (int)attr.name.length(), attr.name.data())

  // Apply slop: [-1 ... INF]
  if (str_caseeq(attr.name, slop)) {
    long long n;
    if (!ParseInteger(attr.value.c_str(), n) || n < -1) {
      MK_INVALID_VALUE();
      return false;
    }
    opts.maxSlop = n;

  } else if (str_caseeq(attr.name, inorder)) {
    // Apply inorder: true|false
    bool b;
    if (!ParseBoolean(attr.value.c_str(), b)) {
      MK_INVALID_VALUE();
      return false;
    }
    opts.inOrder = b;

  } else if (str_caseeq(attr.name, weight)) {
    // Apply weight: [0  ... INF]
    double d;
    if (!ParseDouble(attr.value.c_str(), d) || d < 0) {
      MK_INVALID_VALUE();
      return false;
    }
    opts.weight = d;

  } else if (str_caseeq(attr.name, "phonetic")) {
    // Apply phonetic: true|false
    bool b;
    if (!ParseBoolean(attr.value.c_str(), b)) {
      MK_INVALID_VALUE();
      return false;
    }
    if (b) {
      opts.phonetic = PHONETIC_ENABLED; // means we specifically asked for phonetic matching
    } else {
      opts.phonetic = PHONETIC_DESABLED; // means we specifically asked no for phonetic matching
    }
    // opts.noPhonetic = PHONETIC_DEFAULT -> means no special asks regarding phonetics
    //                                          will be enable if field was declared phonetic

  } else {
    status->SetErrorFmt(QUERY_ENOOPTION, "Invalid attribute %.*s", (int)attr.name.length(), attr.name.data());
    return false;
  }

  return true;
}

//---------------------------------------------------------------------------------------------

bool QueryNode::ApplyAttributes(QueryAttributes *attrs, QueryError *status) {
  if (!attrs) return true;
  for (auto attr: *attrs) {
    if (!ApplyAttribute(*attr, status)) {
      return false;
    }
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
