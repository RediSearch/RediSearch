#include "index_result.h"
#include "varint.h"

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

  int minDist = 0;
  int dist = 0;
  int num = r->numRecords;

  VarintVectorIterator iters[num];
  int vals[num];
  int i;
  for (i = 0; i < num; i++) {
    BufferSeek(&r->records[i].offsets, 0);
    iters[i] = VarIntVector_iter(&r->records[i].offsets);
    vals[i] = VV_Next(&iters[i]);
    if (i >= 1) {
      dist += abs(vals[i] - vals[i - 1]);
    }
  }

  minDist = dist;

  while (minDist >= num) {
    // find the smallest iterator and advance it
    int minIdx = -1;

    for (i = 0; i < num; i++) {
      if (VV_HasNext(&iters[i]) && (minIdx == -1 || vals[i] < vals[minIdx])) {
        minIdx = i;
      }
    }
    // all lists are at their end
    if (minIdx == -1)
      break;

    dist -= minIdx > 0 ? abs(vals[minIdx] - vals[minIdx - 1]) : 0;
    dist -= minIdx < num - 1 ? abs(vals[minIdx + 1] - vals[minIdx]) : 0;

    vals[minIdx] = VV_Next(&iters[minIdx]);
    dist += minIdx > 0 ? abs(vals[minIdx] - vals[minIdx - 1]) : 0;
    dist += minIdx < num - 1 ? abs(vals[minIdx + 1] - vals[minIdx]) : 0;

    minDist = dist < minDist ? dist : minDist;
  }

  return minDist;
}
