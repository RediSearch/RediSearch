#ifndef __QUERY_NODE_H__
#define __QUERY_NODE_H__
#include <stdlib.h>
//#include "numeric_index.h"

struct queryNode;
struct numericFilter;

typedef enum { QN_PHRASE, QN_UNION, QN_TOKEN, QN_NUMERIC } QueryNodeType;

typedef struct {
  char id;
  void *data;
} QueryNodePrivateData;

typedef struct {
  struct queryNode **children;
  int numChildren;
  int exact;
} QueryPhraseNode;

typedef struct {
  struct queryNode **children;
  int numChildren;
} QueryUnionNode;

typedef struct {
  char *str;
  size_t len;
} QueryTokenNode;

typedef struct { struct numericFilter *nf; } QueryNumericNode;

typedef struct queryNode {
  union {
    QueryPhraseNode pn;
    QueryTokenNode tn;
    QueryUnionNode un;
    QueryNumericNode nn;
  };
  QueryNodePrivateData *privdata;

  QueryNodeType type;
} QueryNode;

void QueryPhraseNode_AddChild(QueryPhraseNode *parent, QueryNode *child);
void QueryUnionNode_AddChild(QueryUnionNode *parent, QueryNode *child);

#endif
