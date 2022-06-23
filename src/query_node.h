#pragma once

#include "redisearch.h"
#include "query_error.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct numericFilter;
struct geoFilter;
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
  QN_NULL         // Null term - take no action
};

//---------------------------------------------------------------------------------------------

enum QueryNodeFlags {
  QueryNode_Verbatim = 0x01,
};

//---------------------------------------------------------------------------------------------

#define PHONETIC_ENABLED 1
#define PHONETIC_DESABLED 2
#define PHONETIC_DEFAULT 0

// Various modifiers and options that can apply to the entire query or any sub-query of it

struct QueryNodeOptions {
  QueryNodeFlags flags;
  t_fieldMask fieldMask;
  int maxSlop;
  bool inOrder;
  double weight;
  int phonetic;

  QueryNodeOptions(t_fieldMask fieldMask, QueryNodeFlags flags, int maxSlop, bool inOrder, double weight) :
  fieldMask(fieldMask), flags(flags), maxSlop(maxSlop), inOrder(inOrder), weight(weight) {}
};

//---------------------------------------------------------------------------------------------

// Query attribute is a dynamic attribute that can be applied to any query node.
// Currently supported are weight, slop, and inorder

struct QueryAttribute {
  const char *name;
  size_t namelen;
  const char *value;
  size_t vallen;
};

//---------------------------------------------------------------------------------------------

// QueryNode reqresents any query node in the query tree.
// It has a type to resolve which node it is, and a union of all possible nodes.

struct QueryNode {
  //@@ make derive classes
  /*union {
    QueryPhraseNode pn;
    QueryTokenNode tn;
    QueryUnionNode un;
    QueryNumericNode nn;
    QueryGeofilterNode gn;
    QueryIdFilterNode fn;
    QueryNotNode inverted;
    QueryOptionalNode opt;
    QueryPrefixNode pfx;
    QueryTagNode tag;
    QueryFuzzyNode fz;
    QueryLexRangeNode lxrng;
  };*/

  void ctor(QueryNodeType t);
  QueryNode(QueryNodeType t) { ctor(t); }
  QueryNode(QueryNodeType t, QueryNode **children_, size_t n) {
    ctor(t);
    children = array_ensure_append(children, children_, n, QueryNode *);
  }
  virtual ~QueryNode();

  // The node type, for resolving the union access
  QueryNodeType type;
  QueryNodeOptions opts;
  struct QueryNode **children;

  bool ApplyAttribute(QueryAttribute *attr, QueryError *status);
  bool ApplyAttributes(QueryAttribute *attrs, size_t len, QueryError *status);

  void AddChildren(QueryNode **children_, size_t n);
  void AddChild(QueryNode *child);
  void ClearChildren(bool shouldFree);

  size_t NumChildren() const { return children ? array_len(children) : 0; }
  QueryNode *GetChild(int ix) { return NumChildren() > ix ? children[ix] : NULL; }

  void Expand(RSQueryTokenExpander expander, RSQueryExpanderCtx *expCtx);

  typedef int (*ForEachCallback)(QueryNode *node, QueryNode *q, void *ctx);
  int ForEach(ForEachCallback callback, void *ctx, bool reverse)

  void SetFieldMask(t_fieldMask mask);

  sds DumpSds(sds s, const IndexSpec *spec, int depth) const;
  sds DumpChildren(sds s, const IndexSpec *spec, int depth) const;
  virtual sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "<empty>");
    return s;
  }

  virtual IndexIterator *EvalNode(Query *q) {
    return NewEmptyIterator();
  }

  IndexIterator *EvalSingleTagNode(Query *q, TagIndex *idx, IndexIteratorArray *iterout, double weight);
};

//---------------------------------------------------------------------------------------------

// A prhase node represents a list of nodes with intersection between them, or a phrase in the case
// of several token nodes.

struct QueryPhraseNode : QueryNode {
  int exact;

  QueryPhraseNode(int exact_) {
    exact = exact_;
  }

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "%s {\n", exact ? "EXACT" : "INTERSECT");
    for (size_t ii = 0; ii < NumChildren(); ++ii) {
      s = children[ii]->DumpSds(s, spec, depth + 1);
    }

    s = doPad(s, depth);
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalSingle(Query *q, TagIndex *idx,
                            IndexIteratorArray *iterout, double weight) {
    char *terms[NumChildren()];
    for (size_t i = 0; i < NumChildren(); ++i) {
      if (children[i]->type == QN_TOKEN) {
        terms[i] = children[i]->str;
      } else {
        terms[i] = "";
      }
    }

    sds s = sdsjoin(terms, NumChildren(), " ");
    return idx->OpenReader(q->sctx->spec, s, sdslen(s), weight);
  }
};

//---------------------------------------------------------------------------------------------

// Query node used when the query is effectively null but not invalid.
// This might happen as a result of a query containing only stopwords.

struct QueryWildcardNode : QueryNode {
  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "<WILDCARD>");
    return s;
  }
};

//---------------------------------------------------------------------------------------------

struct QueryTagNode : QueryNode {
  const char *fieldName;
  size_t len;

  QueryTagNode(const char *field, size_t len) : fieldName(field), len(len) {}

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "TAG:@%.*s {\n", (int)len, fieldName);
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

// typedef RSToken QueryTokenNode;
// typedef RSToken QueryPrefixNode;

struct QueryTokenNode : QueryNode {
  RSToken tok;

  QueryTokenNode(QueryParse *q, const char *s, size_t len_) {
    if (len_ == (size_t)-1) {
      len_ = strlen(s);
    }

    q->numTokens++;

    str = (char *)s;
    len = len_;
    expanded = 0;
    flags = 0;
  }

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "%s%s", (char *)str, expanded ? "(expanded)" : "");
    if (opts.weight != 1) {
      s = sdscatprintf(s, " => {$weight: %g;}", opts.weight);
    }
    s = sdscat(s, "\n");
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalSingle(Query *q, TagIndex *idx,
                            IndexIteratorArray *iterout, double weight) {
    return idx->OpenReader(q->sctx->spec, str, len, weight);
  }
};

//---------------------------------------------------------------------------------------------

struct QueryPrefixNode : QueryNode {
  RSToken tok;

  QueryPrefixNode(QueryParse *q, const char *s, size_t len_) {
    q->numTokens++;

    str = (char *)s;
    len = len_;
    expanded = 0;
    flags = 0;
  }

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "PREFIX{%s*", (char *)str);
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalTagPrefixNode(Query *q, TagIndex *idx,
                                   IndexIteratorArray *iterout, double weight);

};

//---------------------------------------------------------------------------------------------

struct QueryFuzzyNode : QueryNode {
  RSToken tok;
  int maxDist;

  QueryFuzzyNode(QueryParse *q, const char *s, size_t len_, int maxDist_) {
    q->numTokens++;

    tok = {
            (RSToken) {
              .str = (char *)s,
              .len = len_,
              .expanded = 0,
              .flags = 0,
          },
        .maxDist = maxDist_,
    };
  }

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "FUZZY{%s}\n", tok.str);
    return s;
  }


};

//---------------------------------------------------------------------------------------------

// A node with a numeric filter
struct NumericFilter;

struct QueryNumericNode : QueryNode {
  NumericFilter *nf;

  QueryNumericNode(const NumericFilter *flt) {
    nf = flt;
  }

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

  QueryGeofilterNode(const GeoFilter *flt) {
    gf = flt;
  }

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s", gf->property, gf->lon,
                     gf->lat, gf->radius, gf->unitType.ToString());
    return s;
  }
};

//---------------------------------------------------------------------------------------------

struct QueryIdFilterNode : QueryNode {
  t_docId *ids;
  size_t len;

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "IDS { ");
    for (int i = 0; i < fn.len; i++) {
      s = sdscatprintf(s, "%llu,", (unsigned long long)fn.ids[i]);
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

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscatprintf(s, "LEXRANGE{%s...%s", begin ? begin : "", end ? end : "");
    return s;
  }

  IndexIterator *EvalNode(Query *q);
  IndexIterator *EvalTagLexRangeNode(Query *q, TagIndex *idx, IndexIteratorArray *iterout,
                                     double weight);
};

//---------------------------------------------------------------------------------------------

// typedef QueryNullNode QueryUnionNode, QueryNotNode, QueryOptionalNode;//@@ How to seperate it to classes?

struct QueryUnionNode : QueryNode {
  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "UNION {\n");
    s = DumpChildren(s, spec, depth + 1);
    s = doPad(s, depth);
    return s;
  }
};

//---------------------------------------------------------------------------------------------

struct QueryNotNode : QueryNode {
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
  IndexIterator *EvalNode(Query *q);

  sds dumpsds(sds s, const IndexSpec *spec, int depth) {
    s = sdscat(s, "OPTIONAL{\n");
    s = DumpChildren(s, spec, depth + 1);
    s = doPad(s, depth);
    return s;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////
