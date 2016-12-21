#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "types.h"
#include "varint.h"

typedef VarintVector OffsetVector;

#define MAX_INTERSECT_WORDS 8

typedef struct {
  char *str;
  double idf;
  void *metadata;
} Term;

Term *NewTerm(char *str);
void Term_Free(Term *t);

typedef struct {
  t_docId docId;
  Term *term;
  float tf;
  u_char flags;
  OffsetVector offsets;
  // PayLoad payload;
} IndexRecord;

typedef struct {
  t_docId docId;
  double totalTF;
  u_char flags;
  int numRecords;
  IndexRecord records[MAX_INTERSECT_WORDS];
} IndexResult;

void IndexResult_PutRecord(IndexResult *r, IndexRecord *record);
void IndexResult_Print(IndexResult *r);
void IndexResult_Free(IndexResult *r);
int IndexResult_MinOffsetDelta(IndexResult *r);

#endif