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

  printf("docId: %d, finalScore: %f, flags %x. Terms:\n", r->docId, r->finalScore, r->flags);

  for (int i = 0; i < r->numRecords; i++) {
    printf("\t%s, %d tf %d, flags %x\n", r->records[i].term->str, r->records[i].docId,
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
  h->finalScore = 0;
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

int __indexResult_withinRangeInOrder(VarintVectorIterator *iters, int *positions, int num,
                                     int maxSlop) {
  while (1) {

    // we start from the beginning, and a span of 0
    int span = 0;
    for (int i = 0; i < num; i++) {
      // take the current position and the position of the previous iterator.
      // For the first iterator we always advance once
      int pos = i ? positions[i] : VV_Next(&iters[i]);
      int lastPos = i ? positions[i - 1] : 0;

      // read while we are not in order
      while (pos != -1 && pos < lastPos) {
        pos = VV_Next(&iters[i]);
        // printf("Reading: i=%d, pos=%d, lastPos %d\n", i, pos, lastPos);
      }
      // we've read through the entire list and it's not in order relative to the last pos
      if (pos == -1) {
        return 0;
      }

      // add the diff from the last pos to the total span
      if (i > 0) {
        span += (pos - lastPos - 1);

        // if we are already out of slop - just quit
        if (span > maxSlop) {
          break;
        }
      }
      positions[i] = pos;
    }

    if (span <= maxSlop) {
      return 1;
    }
  }

  return 0;
}

inline int _arrayMin(int *arr, int len, int *pos) {
  int m = arr[0];
  *pos = 0;
  for (int i = 1; i < len; i++) {
    if (arr[i] < m) {
      m = arr[i];
      *pos = i;
    }
  }
  return m;
}
inline int _arrayMax(int *arr, int len, int *pos) {
  int m = arr[0];
  *pos = 0;
  for (int i = 1; i < len; i++) {
    if (arr[i] >= m) {
      m = arr[i];
      *pos = i;
    }
  }
  return m;
}

int __indexResult_withinRangeUnordered(VarintVectorIterator *iters, int *positions, int num,
                                       int maxSlop) {
  for (int i = 0; i < num; i++) {
    positions[i] = VV_Next(&iters[i]);
  }
  int minPos, maxPos, min, max;
  max = _arrayMax(positions, num, &maxPos);

  while (1) {

    // we start from the beginning, and a span of 0
    min = _arrayMin(positions, num, &minPos);
    if (min != max) {
      int span = max - min - (num - 1);
      // printf("maxslop %d min %d, max %d, minPos %d, maxPos %d, span %d\n", maxSlop, min, max,
      //        minPos, maxPos, span);
      if (span <= maxSlop) {
        return 1;
      }
    }

    positions[minPos] = VV_Next(&iters[minPos]);
    if (positions[minPos] > max) {
      maxPos = minPos;
      max = positions[maxPos];
    } else if (positions[minPos] == -1) {
      break;
    }
  }

  return 0;
}

/** Test the result offset vectors to see if they fall within a max "slop" or distance between the
 * terms. That is the total number of non matched offsets between the terms is no bigger than
 * maxSlop.
 * e.g. for an exact match, the slop allowed is 0.
  */
int IndexResult_IsWithinRange(IndexResult *r, int maxSlop, int inOrder) {

  int num = r->numRecords;
  if (num <= 1) {
    return 1;
  }

  // Fill a list of iterators and the last read positions
  VarintVectorIterator iters[num];
  int positions[num];
  for (int i = 0; i < num; i++) {
    iters[i] = VarIntVector_iter(&r->records[i].offsets);
    positions[i] = 0;
  }

  if (inOrder)
    return __indexResult_withinRangeInOrder(iters, positions, num, maxSlop);
  else
    return __indexResult_withinRangeUnordered(iters, positions, num, maxSlop);
}
