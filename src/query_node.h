#ifndef __QUERY_NODE_H__
#define __QUERY_NODE_H__
#include <stdlib.h>
#include "redisearch.h"
#include "query_error.h"

struct RSQueryNode;
struct numericFilter;
struct geoFilter;
struct idFilter;

/* The types of query nodes */
typedef enum {
  /* Phrase (AND) node, exact or not */
  QN_PHRASE = 1,
  /* Union (OR) Node */
  QN_UNION,
  /* Single token node */
  QN_TOKEN,
  /* Numeric filter node */
  QN_NUMERIC,

  /* NOT operator node */
  QN_NOT,

  /* OPTIONAL (should match) node */
  QN_OPTIONAL,

  /* Geo filter node */
  QN_GEO,

  /* Prefix selection node */
  QN_PREFIX,

  /* Id Filter node */
  QN_IDS,

  /* Wildcard node, used only in conjunction with negative root node to allow negative queries */
  QN_WILDCARD,

  /* Tag node, a list of tags for a specific tag field */
  QN_TAG,

  /* Fuzzy term - expand with levenshtein distance */
  QN_FUZZY,

  /* Lexical range */
  QN_LEXRANGE,

  /* Vector */
  QN_VECTOR,

  /* Null term - take no action */
  QN_NULL
} QueryNodeType;

/* A prhase node represents a list of nodes with intersection between them, or a phrase in the case
 * of several token nodes. */
typedef struct {
  int exact;
} QueryPhraseNode;

/**
 * Query node used when the query is effectively null but not invalid. This
 * might happen as a result of a query containing only stopwords.
 */
typedef struct {
  int dummy;
} QueryNullNode;

typedef struct {
  const char *fieldName;
  size_t len;
} QueryTagNode;

/* A token node is a terminal, single term/token node. An expansion of synonyms is represented by a
 * Union node with several token nodes. A token can have private metadata written by expanders or
 * tokenizers. Later this gets passed to scoring functions in a Term object. See RSIndexRecord */
typedef RSToken QueryTokenNode;

typedef RSToken QueryPrefixNode;

typedef struct {
  RSToken tok;
  int maxDist;
} QueryFuzzyNode;

/* A node with a numeric filter */
typedef struct {
  struct NumericFilter *nf;
} QueryNumericNode;

typedef struct {
  const struct GeoFilter *gf;
} QueryGeofilterNode;

typedef struct {
  struct VectorFilter *vf;
} QueryVectorNode;

typedef struct {
  t_docId *ids;
  size_t len;
} QueryIdFilterNode;

typedef struct {
  char *begin;
  bool includeBegin;
  char *end;
  bool includeEnd;
} QueryLexRangeNode;

typedef enum {
  QueryNode_Verbatim = 0x01,
} QueryNodeFlags;

/* Query attribute is a dynamic attribute that can be applied to any query node.
 * Currently supported are weight, slop, and inorder
 */
typedef struct {
  const char *name;
  size_t namelen;
  const char *value;
  size_t vallen;
} QueryAttribute;

#define PHONETIC_ENABLED 1
#define PHONETIC_DISABLED 2
#define PHONETIC_DEFAULT 0

/* Various modifiers and options that can apply to the entire query or any sub-query of it */
typedef struct {
  QueryNodeFlags flags;
  t_fieldMask fieldMask;
  int maxSlop;
  int inOrder;
  double weight;
  int phonetic;
} QueryNodeOptions;

typedef QueryNullNode QueryUnionNode, QueryNotNode, QueryOptionalNode;

/* QueryNode reqresents any query node in the query tree. It has a type to resolve which node it
 * is, and a union of all possible nodes  */
typedef struct RSQueryNode {
  union {
    QueryVectorNode vn;
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
  };

  /* The node type, for resolving the union access */
  QueryNodeType type;
  QueryNodeOptions opts;
  struct RSQueryNode **children;
} QueryNode;

int QueryNode_ApplyAttributes(QueryNode *qn, QueryAttribute *attr, size_t len, QueryError *status);

void QueryNode_AddChildren(QueryNode *parent, QueryNode **children, size_t n);
void QueryNode_AddChild(QueryNode *parent, QueryNode *child);
void QueryNode_ClearChildren(QueryNode *parent, int shouldFree);

#define QueryNode_NumChildren(qn) ((qn)->children ? array_len((qn)->children) : 0)
#define QueryNode_GetChild(qn, ix) (QueryNode_NumChildren(qn) > ix ? (qn)->children[ix] : NULL)

typedef int (*QueryNode_ForEachCallback)(QueryNode *node, QueryNode *q, void *ctx);
int QueryNode_ForEach(QueryNode *q, QueryNode_ForEachCallback callback, void *ctx, int reverse);

#endif
