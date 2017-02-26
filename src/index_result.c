#include "index_result.h"
#include "varint.h"
#include "rmalloc.h"
#include <math.h>
#include <sys/param.h>

inline void IndexResult_PutRecord(IndexResult *r, IndexRecord *record) {
  if (r->numRecords == r->recordsCap) {
    // printf("expanding record cap from %d\n", r->recordsCap);
    r->recordsCap = r->recordsCap ? r->recordsCap * 2 : DEFAULT_RECORDLIST_SIZE;
    r->records = rm_realloc(r->records, r->recordsCap * sizeof(IndexRecord));
  }
  r->records[r->numRecords++] = *record;
  r->docId = record->docId;
  r->flags |= record->flags;
  r->totalTF += record->tf;
}

void IndexResult_Add(IndexResult *dst, IndexResult *src) {
  for (int i = 0; i < src->numRecords; i++) {
    IndexResult_PutRecord(dst, &src->records[i]);
  }
}

void IndexResult_Print(IndexResult *r) {

  printf("docId: %d, totalTF: %f, flags %x. Terms:\n", r->docId, r->totalTF, r->flags);

  for (int i = 0; i < r->numRecords; i++) {
    printf("\t%s, %d tf %f, flags %x\n", r->records[i].term->str, r->records[i].docId,
           r->records[i].tf, r->records[i].flags);
  }
  printf("----------\n");
}

Term *NewTerm(char *str) {
  Term *ret = rm_malloc(sizeof(Term));
  ret->idf = 1;
  ret->metadata = NULL;
  ret->str = str;
  return ret;
}

void Term_Free(Term *t) {

  rm_free(t);
}

void IndexResult_Init(IndexResult *h) {

  h->docId = 0;
  h->numRecords = 0;
  h->flags = 0;
  h->totalTF = 0;
  // h->hasMetadata = 0;
}

IndexResult NewIndexResult() {
  IndexResult h;
  h.recordsCap = DEFAULT_RECORDLIST_SIZE;
  h.records = rm_calloc(h.recordsCap, sizeof(IndexRecord));
  IndexResult_Init(&h);
  return h;
}

void IndexResult_Free(IndexResult *r) {
  if (r->records) {
    rm_free(r->records);
    r->records = NULL;
  }
}

#define __absdelta(x, y) (x > y ? x - y : y - x)
/**
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list
*/
int IndexResult_MinOffsetDelta(IndexResult *r) {
  if (r->numRecords <= 1) {
    return 1;
  }

  int dist = 0;
  int num = r->numRecords;

  for (int i = 1; i < num; i++) {

    VarintVectorIterator v1 = VarIntVector_iter(&r->records[i - 1].offsets);
    VarintVectorIterator v2 = VarIntVector_iter(&r->records[i].offsets);
    int p1 = VV_Next(&v1);
    int p2 = VV_Next(&v2);
    int cd = __absdelta(p2, p1);
    while (cd > 1 && p1 != -1 && p2 != -1) {
      cd = MIN(__absdelta(p2, p1), cd);
      if (p2 > p1) {
        p1 = VV_Next(&v1);
      } else {
        p2 = VV_Next(&v2);
      }
    }

    dist += cd * cd;
  }

  // we return 1 if ditance could not be calculate, to avoid division by zero
  return dist ? dist : r->numRecords - 1;
}