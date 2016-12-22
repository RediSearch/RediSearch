#include "index_result.h"
#include "varint.h"
#include <math.h>
#include <sys/param.h>

void IndexResult_PutRecord(IndexResult *r, IndexRecord *record) {
  if (r->numRecords < MAX_INTERSECT_WORDS) {
    r->records[r->numRecords++] = *record;
    r->docId = record->docId;
    r->flags |= record->flags;
    r->totalTF += record->tf;
  }
}

void IndexResult_Print(IndexResult *r) {

  printf("docId: %d, totalTF: %f, flags %x. Terms:\n", r->docId, r->totalTF,
         r->flags);

  for (int i = 0; i < r->numRecords; i++) {
    printf("\t%s, tf %f, flags %x\n", r->records[i].term->str, r->records[i].tf,
           r->records[i].flags);
  }
  printf("----------\n");
}

Term *NewTerm(char *str) {
  Term *ret = malloc(sizeof(Term));
  ret->idf = 1;
  ret->metadata = NULL;
  ret->str = str;
  return ret;
}

void Term_Free(Term *t) { free(t); }

void IndexResult_Init(IndexResult *h) {
  h->docId = 0;
  h->numRecords = 0;
  h->flags = 0;
  h->totalTF = 0;
  // h->hasMetadata = 0;
}

IndexResult NewIndexResult() {
  IndexResult h;
  IndexResult_Init(&h);
  return h;
}

void IndexResult_Free(IndexResult *r) {}
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
    BufferSeek(&r->records[i-1].offsets, 0);
    BufferSeek(&r->records[i].offsets, 0);
    VarintVectorIterator v1 = VarIntVector_iter(&r->records[i - 1].offsets);
    VarintVectorIterator v2 = VarIntVector_iter(&r->records[i].offsets);
    int p1 = VV_Next(&v1);
    int p2 = VV_Next(&v2);

    int cd = abs(p2 - p1);
    while (cd > 1 && p1 != -1  && p2 != -1)   {
        cd = MIN(abs(p2 - p1), cd);
        if (p2 > p1) {
          p1 = VV_Next(&v1);
        } else {
          p2 = VV_Next(&v2);
        }
    }
    //printf("docId %d dist %d: %d\n", r->docId, i, cd);
    dist += cd * cd;

  }
  //printf("total dist: %d\n", dist);
  return dist;
}