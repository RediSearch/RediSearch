#ifndef __EXPANDER_H__
#define __EXPANDER_H__
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

typedef struct {
  char *str;
  double idf;
  void *metadata;
} Term;

typedef struct {
  t_docId docId;
  Term *term;
  double tf;
  u_char flags;
  VarintVector *offsets;
  // PayLoad payload;
} IndexRecord;

typedef struct {
  t_docId docId;
  double totalTF;
  u_char flags;
  IndexRecord records[8];
  int numRecords;
  int type;
} IndexResult;

void IndexResult_PutRecord(IndexRecord *record);


typedef double (*ScoreFunction)(IndexResult *);
#endif