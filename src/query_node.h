#ifndef __QUERY_NODE_H__
#define __QUERY_NODE_H__
#include <stdlib.h>
#include "redisearch.h"
//#include "numeric_index.h"

struct RSQueryNode;
struct numericFilter;
struct geoFilter;
struct idFilter;

/* The types of query nodes */
typedef enum {
  /* Phrase (AND) node, exact or not */
  QN_PHRASE,
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
  QN_PREFX,

  /* Id Filter node */
  QN_IDS,

  /* Wildcard node, used only in conjunction with negative root node to allow negative queries */
  QN_WILDCARD,

  /* Tag node, a list of tags for a specific tag field */
  QN_TAG,

  /* Fuzzy term - expand with levenshtein distance */
  QN_FUZZY,
} QueryNodeType;

/* A prhase node represents a list of nodes with intersection between them, or a phrase in the case
 * of several token nodes. */
typedef struct {
  struct RSQueryNode **children;
  int numChildren;
  int exact;

} QueryPhraseNode;

/* A Union node represents a set of child nodes where the index unions the result between them */
typedef struct {
  struct RSQueryNode **children;
  int numChildren;
} QueryUnionNode;

typedef struct {
  const char *fieldName;
  size_t len;

  struct RSQueryNode **children;
  int numChildren;
} QueryTagNode;

typedef struct { struct RSQueryNode *child; } QueryNotNode;

typedef struct { struct RSQueryNode *child; } QueryOptionalNode;

/* A token node is a terminal, single term/token node. An expansion of synonyms is represented by a
 * Union node with several token nodes. A token can have private metadata written by expanders or
 * tokenizers. Later this gets passed to scoring functions in a Term object. See RSIndexRecord */
typedef RSToken QueryTokenNode;

typedef RSToken QueryPrefixNode;

typedef struct {
  RSToken tok;
  int maxDist;
} QueryFuzzyNode;

typedef struct {
} QueryWildcardNode;

/* A node with a numeric filter */
typedef struct { struct numericFilter *nf; } QueryNumericNode;

typedef struct { struct geoFilter *gf; } QueryGeofilterNode;

typedef struct { struct idFilter *f; } QueryIdFilterNode;

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
#define PHONETIC_DEFAULT PHONETIC_DISABLED

/* Various modifiers and options that can apply to the entire query or any sub-query of it */
typedef struct {
  QueryNodeFlags flags;
  t_fieldMask fieldMask;
  int maxSlop;
  int inOrder;
  double weight;
  int phonetic;
} QueryNodeOptions;

/* QueryNode reqresents any query node in the query tree. It has a type to resolve which node it
 * is, and a union of all possible nodes  */
typedef struct RSQueryNode {
  union {
    QueryPhraseNode pn;
    QueryTokenNode tn;
    QueryUnionNode un;
    QueryNumericNode nn;
    QueryGeofilterNode gn;
    QueryIdFilterNode fn;
    QueryNotNode not;
    QueryOptionalNode opt;
    QueryPrefixNode pfx;
    QueryWildcardNode wc;
    QueryTagNode tag;
    QueryFuzzyNode fz;
  };

  /* The node type, for resolving the union access */
  QueryNodeType type;
  QueryNodeOptions opts;
} QueryNode;

int QueryNode_ApplyAttributes(QueryNode *qn, QueryAttribute *attr, size_t len, char **err);

/* Add a child to a phrase node */
void QueryPhraseNode_AddChild(QueryNode *parent, QueryNode *child);

/* Add a child to a union node  */
void QueryUnionNode_AddChild(QueryNode *parent, QueryNode *child);

void QueryTagNode_AddChildren(QueryNode *parent, QueryNode **children, size_t num);
#endif
