#pragma once

#include "redisearch.h"
#include "query_error.h"
#include "query_internal.h"
#include "tag_index.h"
#include "geo_index.h"
#include "numeric_filter.h"

#include <stdlib.h>
#include <string>
#include <string_view>

///////////////////////////////////////////////////////////////////////////////////////////////

struct idFilter;

//---------------------------------------------------------------------------------------------

// The types of query nodes

enum QueryNodeType {
  QN_PHRASE = 1,  // Phrase (AND) node, exact or not
  QN_UNION,       // Union (OR) Node
  QN_TOKEN,       // Single token node
  QN_NUMERIC,     // Numeric filter node
  QN_NOT,         // NOT operator node
  QN_OPTIONAL,    // OPTIONAL (should match) node
  QN_GEO,         // OPTIONAL (should match) node
  QN_PREFX,       // Prefix selection node
  QN_IDS,         // Id Filter node
  QN_WILDCARD,    // Wildcard node, used only in conjunction with negative root node to allow negative queries
  QN_TAG,         // Tag node, a list of tags for a specific tag field
  QN_FUZZY,       // Fuzzy term - expand with levenshtein distance
  QN_LEXRANGE,    // Lexical range
  QN_nullptr         // Null term - take no action
};

//---------------------------------------------------------------------------------------------

enum QueryNodeFlags {
  QueryNode_Verbatim = 0x01,
};

//---------------------------------------------------------------------------------------------

#define PHONETIC_ENABLED 1
#define PHONETIC_DESABLED 2
#define PHONETIC_DEFAULT 0

//---------------------------------------------------------------------------------------------

// Various modifiers and options that can apply to the entire query or any sub-query of it

struct QueryNodeOptions {
  QueryNodeFlags flags;
  t_fieldMask fieldMask;
  int maxSlop;
  bool inOrder;
  double weight;
  int phonetic;

  QueryNodeOptions(t_fieldMask fieldMask = RS_FIELDMASK_ALL,
                   QueryNodeFlags flags = 0,
                   int maxSlop = -1,
                   bool inOrder = false,
                   double weight = 1) :
    fieldMask(fieldMask), flags(flags), maxSlop(maxSlop), inOrder(inOrder), weight(weight) {}
};

//---------------------------------------------------------------------------------------------

// Query attribute is a dynamic attribute that can be applied to any query node.
// Currently supported are weight, slop, and inorder

struct QueryAttribute {
  QueryAttribute(const char *name, size_t namelen, const char *value, size_t vallen) :
    name(name, namelen), value(value, vallen) {}

  std::string_view name;
  const String value;
};

struct QueryAttributes : Vector<QueryAttribute*> {
  ~QueryAttributes() {
    for (auto attr: *this) {
      delete attr;
    }
  }
};

//---------------------------------------------------------------------------------------------

using QueryNodes = Vector<QueryNode *>;

// QueryNode reqresents a node in a query tree

struct QueryNode : Object {
  // void ctor(QueryNodeType t);

  QueryNode() : type{QN_nullptr} { }
  QueryNode(QueryNodeType t) : type{t} { }
  QueryNode(QueryNodeType t, QueryNode *node)
    : type{t}
  {
    children.push_back(node);
  }
  QueryNode(QueryNodeType t, QueryNodes *chi) //@@ QueryNodes &&chi ?
    : type{t}
  {
    if (chi) {
      children.swap(*chi);
    }
  }
  virtual ~QueryNode();

  // The node type, for resolving the union access
  QueryNodeType type;
  QueryNodeOptions opts;
  QueryNodes children;

  bool ApplyAttribute(QueryAttribute &attr, QueryError *status);
  bool ApplyAttributes(QueryAttributes *attrs, QueryError *status);

  void AddChildren(QueryNodes &children);
  void AddChild(QueryNode *child);
  void ClearChildren(bool shouldFree);

  size_t NumChildren() const { return children.size(); }
  QueryNode *Child(int i) { return NumChildren() > i ? children[i] : nullptr; }

  virtual void Expand(QueryExpander &expander);
  virtual bool expandChildren() const { return false; }

  typedef bool (*ForEachCallback)(QueryNode *node, void *ctx);
  int ForEach(ForEachCallback callback, void *ctx, bool reverse);

  void SetFieldMask(t_fieldMask mask);

  sds DumpSds(sds s, const IndexSpec *spec, int depth) const;
  sds DumpChildren(sds s, const IndexSpec *spec, int depth) const;
  virtual sds dumpsds(sds s, const IndexSpec *spec, int depth) { return sdscat(s, "<empty>"); }

  virtual IndexIterator *EvalNode(Query *q) { return new EmptyIterator(); }
  virtual IndexIterator *EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight) {
    return nullptr;
  }

  IndexIterator *EvalSingleTagNode(Query *q, TagIndex *idx, IndexIterators iterout, double weight);
};

//---------------------------------------------------------------------------------------------

// A prhase node represents a list of nodes with intersection between them, or a phrase in the case
// of several token nodes.

struct QueryPhraseNode : QueryNode {
  bool exact;

  QueryPhraseNode(bool exact) : QueryNode(QN_PHRASE), exact(exact) {}

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "%s {\n", exact ? "EXACT" : "INTERSECT");
    for (auto &child: children) {
      s = child->DumpSds(s, spec, depth + 1);
    }

    s = doPad(s, depth);
    return s;
  }

  bool expandChildren() const { return !exact; }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight);
};

//---------------------------------------------------------------------------------------------

// Query node used when the query is effectively null but not invalid.
// This might happen as a result of a query containing only stopwords.

struct QueryWildcardNode : QueryNode {
  QueryWildcardNode() : QueryNode(QN_WILDCARD) {}

  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "<WILDCARD>");
    return s;
  }
};

//---------------------------------------------------------------------------------------------

struct QueryTagNode : QueryNode {
  String fieldName;

  QueryTagNode(const std::string_view &fieldName) : QueryNode(QN_TAG), fieldName(fieldName) {}

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "TAG:@%.*s {\n", (int)fieldName.length(), fieldName.data());
    s = DumpChildren(s, spec, depth + 1);
    s = doPad(s, depth);
    return s;
  }

  IndexIterator *EvalNode(Query *q);
};

//---------------------------------------------------------------------------------------------

// A token node is a terminal, single term/token node.
// An expansion of synonyms is represented by a Union node with several token nodes.
// A token can have private metadata written by expanders or tokenizers.
// Later this gets passed to scoring functions in a Term object. See RSIndexRecord.

struct QueryTokenNode : QueryNode {
  RSToken tok;

  QueryTokenNode(QueryParse *query, std::string_view str, uint8_t expanded = 0, RSTokenFlags flags = 0) :
    QueryNode(QN_TOKEN), tok(str, expanded, flags) {
    if (query) query->numTokens++;
  }

  virtual void Expand(QueryExpander &expander);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "%s%s", (char *)tok.str.data(), tok.expanded ? "(expanded)" : "");
    if (opts.weight != 1) {
      s = sdscatprintf(s, " => {$weight: %g;}", opts.weight);
    }
    s = sdscat(s, "\n");
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight) {
    return idx->OpenReader(q->sctx->spec, tok.str, weight);
  }
};

//---------------------------------------------------------------------------------------------

struct QueryPrefixNode : QueryNode {
  RSToken tok;

  QueryPrefixNode(QueryParse *q, const std::string_view &str) :
    QueryNode(QN_PREFX), tok(str, 0, 0) {
    if (q) q->numTokens++;
  }

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "PREFIX{%s*", +tok);
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight);
};

//---------------------------------------------------------------------------------------------

struct QueryFuzzyNode : QueryNode {
  RSToken tok;
  int maxDist;

  QueryFuzzyNode(QueryParse *q, const std::string_view &str, int maxDist) :
      QueryNode(QN_FUZZY), tok(str, 0, 0), maxDist(maxDist) {
    if (q) q->numTokens++;
  }

  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "FUZZY{%s}\n", +tok);
    return s;
  }
};

//---------------------------------------------------------------------------------------------

// A node with a numeric filter

struct QueryNumericNode : QueryNode {
  const NumericFilter *nf;

  QueryNumericNode(const NumericFilter *nf) : QueryNode(QN_NUMERIC), nf(nf) {}

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "NUMERIC {%f %s @%s %s %f", nf->min, nf->inclusiveMin ? "<=" : "<",
                     nf->fieldName, nf->inclusiveMax ? "<=" : "<", nf->max);
    return s;
  }

  IndexIterator *EvalNode(Query *q);
};

//---------------------------------------------------------------------------------------------

struct QueryGeofilterNode : QueryNode {
  const struct GeoFilter *gf;

  QueryGeofilterNode(const GeoFilter *gf) : QueryNode(QN_GEO), gf(gf) {}

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s", gf->property, gf->lon,
                     gf->lat, gf->radius, gf->unitType.ToString());
    return s;
  }

  IndexIterator *Eval(Query *q, double weight);
};

//---------------------------------------------------------------------------------------------

struct QueryIdFilterNode : QueryNode {
  Vector<t_docId> ids;
  size_t len;

  QueryIdFilterNode(Vector<t_docId> &ids) : QueryNode(QN_IDS), ids(ids) {}

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "IDS { ");
    for (int i = 0; i < len; i++) {
      s = sdscatprintf(s, "%llu,", (unsigned long long)ids[i]);
    }
    return s;
  }

  IndexIterator *EvalNode(Query *q);
};

//---------------------------------------------------------------------------------------------

struct QueryLexRangeNode : QueryNode {
  char *begin;
  bool includeBegin;
  char *end;
  bool includeEnd;

  QueryLexRangeNode() : QueryNode(QN_LEXRANGE), begin(nullptr), includeBegin(false), end(nullptr), includeEnd(false) {}
  ~QueryLexRangeNode();

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "LEXRANGE{%s...%s", begin ? begin : "", end ? end : "");
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalSingle(Query *q, TagIndex *idx, IndexIterators iterout, double weight);
};

//---------------------------------------------------------------------------------------------

struct QueryUnionNode : QueryNode {
  QueryUnionNode() : QueryNode(QN_UNION) {}

  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "UNION {\n");
    s = DumpChildren(s, spec, depth + 1);
    s = doPad(s, depth);
    return s;
  }

  bool expandChildren() { return true; }
};

//---------------------------------------------------------------------------------------------

struct QueryNotNode : QueryNode {
  QueryNotNode() : QueryNode(QN_NOT) {}
  QueryNotNode(QueryNode *child) : QueryNode(QN_NOT, child) {}

  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "NOT{\n");
    s = DumpChildren(s, spec, depth + 1);
    s = doPad(s, depth);
    return s;
  }
};

//---------------------------------------------------------------------------------------------

struct QueryOptionalNode : QueryNode {
  QueryOptionalNode() : QueryNode(QN_OPTIONAL) {}
  QueryOptionalNode(QueryNode *child) : QueryNode(QN_OPTIONAL, child) {}

  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "OPTIONAL{\n");
    s = DumpChildren(s, spec, depth + 1);
    s = doPad(s, depth);
    return s;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////
