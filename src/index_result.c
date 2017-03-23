#include "offset_vector.h"
#include "index_result.h"
#include "varint.h"
#include "rmalloc.h"
#include <math.h>
#include <sys/param.h>

RSIndexResult *NewIndexResult() {
  RSIndexResult *res = rm_new(RSIndexResult);
  memset(res, 0, sizeof(RSIndexResult));
  return res;
}

RSIndexResult *__newAggregateResult(size_t cap, RSResultType t) {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){
      .type = t,
      .docId = 0,
      .freq = 0,
      .fieldMask = 0,
      .agg = (RSAggregateResult){.numChildren = 0,
                                 .childrenCap = cap,
                                 .children = rm_calloc(cap, sizeof(RSIndexResult *))}};
  return res;
}

RSIndexResult *NewIntersectResult(size_t cap) {
  return __newAggregateResult(cap, RSResultType_Intersection);
}
RSIndexResult *NewUnionResult(size_t cap) {
  return __newAggregateResult(cap, RSResultType_Union);
}

RSIndexResult *NewTokenRecord(RSQueryTerm *term) {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){.type = RSResultType_Term,
                         .docId = 0,
                         .fieldMask = 0,
                         .freq = 0,
                         .term = (RSTermRecord){
                             .term = term, .offsets = (RSOffsetVector){},
                         }};
  return res;
}

void AggregateResult_AddChild(RSIndexResult *parent, RSIndexResult *child) {
  RSAggregateResult *agg = &parent->agg;
  if (agg->numChildren >= agg->childrenCap) {
    agg->childrenCap = agg->childrenCap ? agg->childrenCap * 2 : 1;
    agg->children = rm_realloc(agg->children, agg->childrenCap * sizeof(RSIndexResult *));
  }
  agg->children[agg->numChildren++] = child;
  parent->freq += child->freq;
  parent->docId = child->docId;
  parent->fieldMask |= child->fieldMask;
}

void IndexResult_Print(RSIndexResult *r) {

  // printf("docId: %d, finalScore: %f, flags %x. Terms:\n", r->docId, r->finalScore, r->fieldMask);

  // for (int i = 0; i < r->numRecords; i++) {
  //   printf("\t%s, %d tf %d, flags %x\n", r->records[i].term->str, r->records[i].docId,
  //          r->records[i].freq, r->records[i].fieldMask);
  // }
  // printf("----------\n");
}

RSQueryTerm *NewTerm(RSToken *tok) {
  RSQueryTerm *ret = rm_malloc(sizeof(RSQueryTerm));
  ret->idf = 1;
  ret->str = tok->str;
  ret->len = tok->len;
  ret->flags = tok->flags;
  return ret;
}

void Term_Free(RSQueryTerm *t) {

  rm_free(t);
}

void IndexResult_Init(RSIndexResult *h) {

  h->docId = 0;
  h->fieldMask = 0;
  h->freq = 0;

  if (h->type == RSResultType_Intersection || h->type == RSResultType_Union) {
    h->agg.numChildren = 0;
  }
}

void AggregateResult_Reset(RSAggregateResult *r) {
  r->numChildren = 0;
}

void __aggResult_free(RSIndexResult *r) {
}

void IndexResult_Free(RSIndexResult *r) {

  if (r->type == RSResultType_Intersection || r->type == RSResultType_Union) {
    rm_free(r->agg.children);
    r->agg.children = NULL;
  }
  // rm_free(r);
}

#define __absdelta(x, y) (x > y ? x - y : y - x)
/**
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list
*/
int IndexResult_MinOffsetDelta(RSIndexResult *r) {
  if (r->type == RSResultType_Term || r->agg.numChildren <= 1) {
    return 1;
  }

  RSAggregateResult *agg = &r->agg;
  int dist = 0;
  int num = agg->numChildren;

  RSOffsetIterator v1, v2;
  for (int i = 1; i < num; i++) {
    // if this is not the first iteration, we take v1 from v2 and rewind it

    v1 = RSIndexResult_IterateOffsets(agg->children[i - 1]);
    v2 = RSIndexResult_IterateOffsets(agg->children[i]);

    uint32_t p1 = v1.Next(v1.ctx);
    uint32_t p2 = v2.Next(v2.ctx);
    int cd = __absdelta(p2, p1);
    while (cd > 1 && p1 != RS_OFFSETVECTOR_EOF && p2 != RS_OFFSETVECTOR_EOF) {
      cd = MIN(__absdelta(p2, p1), cd);
      if (p2 > p1) {
        p1 = v1.Next(v1.ctx);
      } else {
        p2 = v2.Next(v2.ctx);
      }
    }

    v1.Free(v1.ctx);
    v2.Free(v2.ctx);

    dist += cd * cd;
  }

  // we return 1 if ditance could not be calculate, to avoid division by zero
  return dist ? dist : agg->numChildren - 1;
}

int __indexResult_withinRangeInOrder(RSOffsetIterator *iters, uint32_t *positions, int num,
                                     int maxSlop) {
  while (1) {

    // we start from the beginning, and a span of 0
    int span = 0;
    for (int i = 0; i < num; i++) {
      // take the current position and the position of the previous iterator.
      // For the first iterator we always advance once
      uint32_t pos = i ? positions[i] : iters[i].Next(iters[i].ctx);
      uint32_t lastPos = i ? positions[i - 1] : 0;

      // read while we are not in order
      while (pos != RS_OFFSETVECTOR_EOF && pos < lastPos) {
        pos = iters[i].Next(iters[i].ctx);
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

int __indexResult_withinRangeUnordered(RSOffsetIterator *iters, uint32_t *positions, int num,
                                       int maxSlop) {
  for (int i = 0; i < num; i++) {
    positions[i] = iters[i].Next(iters[i].ctx);
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

    positions[minPos] = iters[minPos].Next(iters[minPos].ctx);
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
int IndexResult_IsWithinRange(RSIndexResult *ir, int maxSlop, int inOrder) {

  if (ir->type == RSResultType_Term || ir->agg.numChildren <= 1) {
    return 1;
  }
  RSAggregateResult *r = &ir->agg;
  int num = r->numChildren;

  // Fill a list of iterators and the last read positions
  RSOffsetIterator iters[num];
  uint32_t positions[num];
  for (int i = 0; i < num; i++) {
    iters[i] = RSIndexResult_IterateOffsets(r->children[i]);
    positions[i] = 0;
  }

  if (inOrder)
    return __indexResult_withinRangeInOrder(iters, positions, num, maxSlop);
  else
    return __indexResult_withinRangeUnordered(iters, positions, num, maxSlop);
}
