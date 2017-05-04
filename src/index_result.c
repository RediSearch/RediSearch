#include "index_result.h"
#include "varint.h"
#include "rmalloc.h"
#include <math.h>
#include <sys/param.h>

/* Allocate a new aggregate result of a given type with a given capacity*/
RSIndexResult *__newAggregateResult(size_t cap, RSResultType t) {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){
      .type = t,
      .docId = 0,
      .freq = 0,
      .fieldMask = 0,

      .agg = (RSAggregateResult){.numChildren = 0,
                                 .childrenCap = cap,
                                 .typeMask = 0x0000,
                                 .children = rm_calloc(cap, sizeof(RSIndexResult *))}};
  return res;
}

/* Allocate a new intersection result with a given capacity*/
RSIndexResult *NewIntersectResult(size_t cap) {
  return __newAggregateResult(cap, RSResultType_Intersection);
}

/* Allocate a new union result with a given capacity*/
RSIndexResult *NewUnionResult(size_t cap) {
  return __newAggregateResult(cap, RSResultType_Union);
}

/* Allocate a new token record result for a given term */
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

RSIndexResult *NewVirtualResult() {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){
      .type = RSResultType_Virtual, .docId = 0, .fieldMask = 0, .freq = 0,
  };
  return res;
}

void AggregateResult_AddChild(RSIndexResult *parent, RSIndexResult *child) {

  // printf("adding child %d to ", child->docId);
  // IndexResult_Print(parent, 0);

  RSAggregateResult *agg = &parent->agg;

  /* Increase capacity if needed */
  if (agg->numChildren >= agg->childrenCap) {
    agg->childrenCap = agg->childrenCap ? agg->childrenCap * 2 : 1;
    agg->children = rm_realloc(agg->children, agg->childrenCap * sizeof(RSIndexResult *));
  }
  agg->children[agg->numChildren++] = child;
  // update the parent's type mask
  agg->typeMask |= child->type;
  parent->freq += child->freq;
  parent->docId = child->docId;
  parent->fieldMask |= child->fieldMask;
  // printf("\nAfter: ");
  // IndexResult_Print(parent, 0);
  // printf("\n");
}

void IndexResult_Print(RSIndexResult *r, int depth) {
  // for (int i = 0; i < depth; i++) printf("  ");
  if (r->type == RSResultType_Term) {
    printf("Term{%s => %d}, ", r->term.term ? r->term.term->str : "nil", r->docId);
    return;
  }
  if (r->type == RSResultType_Virtual) {
    printf("Virtual{%d}, ", r->docId);
    return;
  }
  printf("%s => %d{ ", r->type == RSResultType_Intersection ? "Inter" : "Union", r->docId);

  for (int i = 0; i < r->agg.numChildren; i++) {

    IndexResult_Print(r->agg.children[i], depth + 1);
  }
  //  for (int i = 0; i < depth; i++) printf("  ");

  printf("},");

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
  ret->str = tok->str ? rm_strndup(tok->str, tok->len) : NULL;
  ret->len = tok->len;
  ret->flags = tok->flags;
  return ret;
}

void Term_Free(RSQueryTerm *t) {
  if (t) {
    if (t->str) rm_free(t->str);
    rm_free(t);
  }
}

void IndexResult_Init(RSIndexResult *h) {

  h->docId = 0;
  h->fieldMask = 0;
  h->freq = 0;

  if (h->type == RSResultType_Intersection || h->type == RSResultType_Union) {
    h->agg.numChildren = 0;
  }
}

int RSIndexResult_HasOffsets(RSIndexResult *res) {
  switch (res->type) {
    case RSResultType_Term:
      return 1;
    case RSResultType_Intersection:
    case RSResultType_Union:
      // the intersection and union aggregates can have offsets if they are not purely made of
      // virtual results
      return res->agg.typeMask != RSResultType_Virtual;

    // a virtual result doesn't have offsets!
    case RSResultType_Virtual:
    default:
      return 0;
  }
}

/* Reset the aggregate result's child vector */
inline void AggregateResult_Reset(RSIndexResult *r) {

  r->docId = 0;
  r->agg.numChildren = 0;
  r->agg.typeMask = 0;
}

void IndexResult_Free(RSIndexResult *r) {

  if (r->type == RSResultType_Intersection || r->type == RSResultType_Union) {
    rm_free(r->agg.children);
    r->agg.children = NULL;
  }
  rm_free(r);
}

inline int RSIndexResult_IsAggregate(RSIndexResult *r) {
  return (r->type & RS_RESULT_AGGREGATE) != 0;
}
#define __absdelta(x, y) (x > y ? x - y : y - x)
/**
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list
*/
int IndexResult_MinOffsetDelta(RSIndexResult *r) {
  if (!RSIndexResult_IsAggregate(r) || r->agg.numChildren <= 1) {
    return 1;
  }

  RSAggregateResult *agg = &r->agg;
  int dist = 0;
  int num = agg->numChildren;

  RSOffsetIterator v1, v2;
  int i = 0;
  while (i < num) {
    // if either
    while (i < num && !RSIndexResult_HasOffsets(agg->children[i])) {
      i++;
      continue;
    }
    if (i == num) break;
    v1 = RSIndexResult_IterateOffsets(agg->children[i]);
    i++;

    while (i < num && !RSIndexResult_HasOffsets(agg->children[i])) {
      i++;
      continue;
    }
    if (i == num) {
      v1.Free(v1.ctx);
      dist = dist ? dist : 100;
      break;
    }
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
      // printf("Before: i=%d, pos=%d, lastPos %d\n", i, pos, lastPos);

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
      positions[i] = pos;

      // add the diff from the last pos to the total span
      if (i > 0) {
        span += ((int)pos - (int)lastPos - 1);
        // if we are already out of slop - just quit
        if (span > maxSlop) {
          break;
        }
      }
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

/* Check the index result for maximal slop, in an unordered fashion.
 * The algorithm is simple - we find the first offsets min and max such that max-min<=maxSlop */
int __indexResult_withinRangeUnordered(RSOffsetIterator *iters, uint32_t *positions, int num,
                                       int maxSlop) {
  for (int i = 0; i < num; i++) {
    positions[i] = iters[i].Next(iters[i].ctx);
  }
  uint32_t minPos, maxPos, min, max;
  // find the max member
  max = _arrayMax(positions, num, &maxPos);

  while (1) {

    // we start from the beginning, and a span of 0
    min = _arrayMin(positions, num, &minPos);
    if (min != max) {
      // calculate max - min
      int span = (int)max - (int)min - (num - 1);
      // printf("maxslop %d min %d, max %d, minPos %d, maxPos %d, span %d\n", maxSlop, min, max,
      //        minPos, maxPos, span);
      // if it matches the condition - just return success
      if (span <= maxSlop) {
        return 1;
      }
    }

    // if we are not meeting the conditions - advance the minimal iterator
    positions[minPos] = iters[minPos].Next(iters[minPos].ctx);
    // If the minimal iterator is larger than the max iterator, the minimal iterator is the new
    // maximal iterator.
    if (positions[minPos] != RS_OFFSETVECTOR_EOF && positions[minPos] > max) {
      maxPos = minPos;
      max = positions[maxPos];

    } else if (positions[minPos] == RS_OFFSETVECTOR_EOF) {
      // this means we've reached the end
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
  int n = 0;
  for (int i = 0; i < num; i++) {
    // collect only iterators for nodes that can have offsets
    if (RSIndexResult_HasOffsets(r->children[i])) {
      iters[n] = RSIndexResult_IterateOffsets(r->children[i]);
      positions[n] = 0;
      n++;
    }
  }

  int rc;
  // cal the relevant algorithm based on ordered/unordered condition
  if (inOrder)
    rc = __indexResult_withinRangeInOrder(iters, positions, n, maxSlop);
  else
    rc = __indexResult_withinRangeUnordered(iters, positions, n, maxSlop);
  // printf("slop result for %d: %d\n", ir->docId, rc);
  for (int i = 0; i < n; i++) {
    iters[i].Free(iters[i].ctx);
  }
  return rc;
}
