#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "types.h"
#include "varint.h"

typedef VarintVector OffsetVector;

#define MAX_INTERSECT_WORDS 8
#define DEFAULT_RECORDLIST_SIZE 4

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
  int recordsCap;
  IndexRecord *records;
} IndexResult;

/** Reset the state of an existing index hit. This can be used to
recycle index hits during reads */
void IndexResult_Init(IndexResult *h);
/** Init a new index hit. This is not a heap allocation and doesn't neeed to be
 * freed */
IndexResult NewIndexResult();

void IndexResult_PutRecord(IndexResult *r, IndexRecord *record);
void IndexResult_Add(IndexResult *dst, IndexResult *src);
void IndexResult_Print(IndexResult *r);
void IndexResult_Free(IndexResult *r);
int IndexResult_MinOffsetDelta(IndexResult *r);

#endif