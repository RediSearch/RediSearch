#include "offset_vector.h"
#include "index_result.h"
#include "varint.h"
#include "rmalloc.h"
#include <math.h>
#include <sys/param.h>

inline void IndexResult_PutRecord(RSIndexResult *r, RSIndexRecord *record) {
  if (r->numRecords == r->recordsCap) {
    // printf("expanding record cap from %d\n", r->recordsCap);
    r->recordsCap = r->recordsCap ? r->recordsCap * 2 : DEFAULT_RECORDLIST_SIZE;
    r->records = rm_realloc(r->records, r->recordsCap * sizeof(RSIndexRecord));
  }
  r->records[r->numRecords++] = *record;
  r->docId = record->docId;
  r->fieldMask |= record->fieldMask;
  r->totalTF += record->freq;
}

void IndexResult_Add(RSIndexResult *dst, RSIndexResult *src) {
  for (int i = 0; i < src->numRecords; i++) {
    IndexResult_PutRecord(dst, &src->records[i]);
  }
}

void IndexResult_Print(RSIndexResult *r) {

  printf("docId: %d, finalScore: %f, flags %x. Terms:\n", r->docId, r->finalScore, r->fieldMask);

  for (int i = 0; i < r->numRecords; i++) {
    printf("\t%s, %d tf %d, flags %x\n", r->records[i].term->str, r->records[i].docId,
           r->records[i].freq, r->records[i].fieldMask);
  }
  printf("----------\n");
}

RSQueryTerm *NewTerm(char *str) {
  RSQueryTerm *ret = rm_malloc(sizeof(RSQueryTerm));
  ret->idf = 1;
  ret->str = str;
  ret->len = strlen(str);
  ret->flags = 0;
  return ret;
}

void Term_Free(RSQueryTerm *t) {

  rm_free(t);
}

void IndexResult_Init(RSIndexResult *h) {

  h->docId = 0;
  h->numRecords = 0;
  h->fieldMask = 0;
  h->totalTF = 0;
  h->finalScore = 0;

  // h->hasMetadata = 0;
}

RSIndexResult NewIndexResult() {
  RSIndexResult h;
  h.recordsCap = DEFAULT_RECORDLIST_SIZE;
  h.records = rm_calloc(h.recordsCap, sizeof(RSIndexRecord));
  IndexResult_Init(&h);
  return h;
}

void IndexResult_Free(RSIndexResult *r) {
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
int IndexResult_MinOffsetDelta(RSIndexResult *r) {
  if (r->numRecords <= 1) {
    return 1;
  }

  int dist = 0;
  int num = r->numRecords;

  RSOffsetIterator *v1 = NULL, *v2 = NULL;
  for (int i = 1; i < num; i++) {
    // if this is not the first iteration, we take v1 from v2 and rewind it

    v1 = RSOffsetVector_Iterate(&r->records[i - 1].offsets);
    v2 = RSOffsetVector_Iterate(&r->records[i].offsets);

    uint32_t p1 = RSOffsetIterator_Next(v1);
    uint32_t p2 = RSOffsetIterator_Next(v2);
    int cd = __absdelta(p2, p1);
    while (cd > 1 && p1 != RS_OFFSETVECTOR_EOF && p2 != RS_OFFSETVECTOR_EOF) {
      cd = MIN(__absdelta(p2, p1), cd);
      if (p2 > p1) {
        p1 = RSOffsetIterator_Next(v1);
      } else {
        p2 = RSOffsetIterator_Next(v2);
      }
    }

    RSOffsetIterator_Free(v1);
    RSOffsetIterator_Free(v2);

    dist += cd * cd;
  }

  // we return 1 if ditance could not be calculate, to avoid division by zero
  return dist ? dist : r->numRecords - 1;
}

int __indexResult_withinRangeInOrder(RSOffsetIterator **iters, uint32_t *positions, int num,
                                     int maxSlop) {
  while (1) {

    // we start from the beginning, and a span of 0
    int span = 0;
    for (int i = 0; i < num; i++) {
      // take the current position and the position of the previous iterator.
      // For the first iterator we always advance once
      uint32_t pos = i ? positions[i] : RSOffsetIterator_Next(iters[i]);
      uint32_t lastPos = i ? positions[i - 1] : 0;

      // read while we are not in order
      while (pos != RS_OFFSETVECTOR_EOF && pos < lastPos) {
        pos = RSOffsetIterator_Next(iters[i]);
        // printf("Reading: i=%d, pos=%d, lastPos %d\n", i, pos, lastPos);
      }
      // printf("i=%d, pos=%d, lastPos %d\n", i, pos, lastPos);

      // we've read through the entire list and it's not in order relative to the last pos
      if (pos == RS_OFFSETVECTOR_EOF) {
        return 0;
      }

      // add the diff from the last pos to the total span
      if (i > 0) {
        span += ((int)pos - (int)lastPos - 1);

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

uint32_t _arrayMin(uint32_t *arr, int len, int *pos);
uint32_t _arrayMax(uint32_t *arr, int len, int *pos);

inline uint32_t _arrayMin(uint32_t *arr, int len, int *pos) {
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

inline uint32_t _arrayMax(uint32_t *arr, int len, int *pos) {
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

int __indexResult_withinRangeUnordered(RSOffsetIterator **iters, uint32_t *positions, int num,
                                       int maxSlop) {
  for (int i = 0; i < num; i++) {
    positions[i] = RSOffsetIterator_Next(iters[i]);
  }
  uint32_t minPos, maxPos, min, max;
  max = _arrayMax(positions, num, &maxPos);

  while (1) {

    // we start from the beginning, and a span of 0
    min = _arrayMin(positions, num, &minPos);
    if (min != max) {
      int span = (int)max - (int)min - (num - 1);
      // printf("maxslop %d min %d, max %d, minPos %d, maxPos %d, span %d\n", maxSlop, min, max,
      //        minPos, maxPos, span);
      if (span <= maxSlop) {
        return 1;
      }
    }

    positions[minPos] = RSOffsetIterator_Next(iters[minPos]);
    if (positions[minPos] != RS_OFFSETVECTOR_EOF && positions[minPos] > max) {
      maxPos = minPos;
      max = positions[maxPos];
    } else if (positions[minPos] == RS_OFFSETVECTOR_EOF) {
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
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder) {

  int num = r->numRecords;
  if (num <= 1) {
    return 1;
  }

  // Fill a list of iterators and the last read positions
  RSOffsetIterator *iters[num];
  uint32_t positions[num];
  for (int i = 0; i < num; i++) {
    iters[i] = RSOffsetVector_Iterate(&r->records[i].offsets);
    positions[i] = 0;
  }

  if (inOrder)
    return __indexResult_withinRangeInOrder(iters, positions, num, maxSlop);
  else
    return __indexResult_withinRangeUnordered(iters, positions, num, maxSlop);
}
