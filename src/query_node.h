#ifndef __QUERY_NODE_H__
#define __QUERY_NODE_H__
#include <stdlib.h>
//#include "numeric_index.h"

struct queryNode;
struct numericFilter;

/* The types of query nodes */
typedef enum {
  /* Phrase (AND) node, exact or not */
  QN_PHRASE,
  /* Union (OR) Node */
  QN_UNION,
  /* Single token node */
  QN_TOKEN,
  /* Numeric filter node */
  QN_NUMERIC
} QueryNodeType;

/* Private data saved into query nodes by query expanders, that can later be used by scoring and
 * filter functions. The id is there to make sure the private data is the same type when writing and
 * reading it */
typedef struct {
  char id;
  void *data;
} QueryNodePrivateData;

/* A prhase node represents a list of nodes with intersection between them, or a phrase in the case
 * of several token nodes. */
typedef struct {
  struct queryNode **children;
  int numChildren;
  int exact;
} QueryPhraseNode;

/* A Union node represents a set of child nodes where the index unions the result between them */
typedef struct {
  struct queryNode **children;
  int numChildren;
} QueryUnionNode;

/* A token node is a terminal, single term/token node. An expansion of synonyms is represented by a
 * Union node with several token nodes */
typedef struct {
  char *str;
  size_t len;
} QueryTokenNode;

/* A node with a numeric filter */
typedef struct { struct numericFilter *nf; } QueryNumericNode;

/* QueryNode reqresents any query node in the query tree. It has a type to resolve which node it is,
 * and a union of all possible nodes. It has private data that may be written by expanders */
typedef struct queryNode {
  union {
    QueryPhraseNode pn;
    QueryTokenNode tn;
    QueryUnionNode un;
    QueryNumericNode nn;
  };

  /* Private data that can be written by expanders and read by filters/scorers */
  QueryNodePrivateData *privdata;

  /* The node type, for resolving the union access */
  QueryNodeType type;
} QueryNode;

/* Add a child to a phrase node */
void QueryPhraseNode_AddChild(QueryPhraseNode *parent, QueryNode *child);

/* Add a child to a union node  */
void QueryUnionNode_AddChild(QueryUnionNode *parent, QueryNode *child);

#endif
