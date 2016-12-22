#ifdef __EXPANDER_H__
//#define __EXPANDER_H__
#include <stdlib.h>
#include "types.h"
#include "varint.h"

struct queryNode;

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

typedef struct queryNode {
  union {
    QueryPhraseNode pn;
    QueryTokenNode tn;
    QueryUnionNode un;
  };
  int t;
} QueryNode;

typedef struct QueryContext QueryContext;
typedef QueryNode *(*QueryExpander)(QueryContext *q, QueryNode *);

#endif