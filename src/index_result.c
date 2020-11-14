
#include "index_result.h"
#include "varint.h"
#include "rmalloc.h"

#include <math.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

// Allocate a new aggregate result of a given type with a given capacity
RSIndexResult *__newAggregateResult(size_t cap, RSResultType t, double weight) {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){
      .type: t,
      .docId: 0,
      .freq: 0,
      .fieldMask: 0,
      .isCopy: 0,
      .weight: weight,
      .agg: (RSAggregateResult){.numChildren: 0,
                                .childrenCap: cap,
                                .typeMask: 0x0000,
                                .children: rm_calloc(cap, sizeof(RSIndexResult *))}};
  return res;
}

// Allocate a new intersection result with a given capacity
RSIndexResult *NewIntersectResult(size_t cap, double weight) {
  return new AggregateResult(RSResultType_Intersection, cap, weight);
}

// Allocate a new union result with a given capacity
RSIndexResult *NewUnionResult(size_t cap, double weight) {
  return new AggregateResult(RSResultType_Union, cap, weight);
}

// Allocate a new token record result for a given term
RSIndexResult *NewTermRecord(RSQueryTerm *term, double weight) {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){.type: RSResultType_Term,
                         .docId: 0,
                         .fieldMask: 0,
                         .isCopy: 0,
                         .freq: 0,
                         .weight: weight,
                         .term = (RSTermRecord){
                             .term: term,
                             .offsets: (RSOffsetVector){},
                         }};
  return res;
}

RSIndexResult *NewNumericResult() {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){.type = RSResultType_Numeric,
                         .docId = 0,
                         .isCopy = 0,
                         .fieldMask = RS_FIELDMASK_ALL,
                         .freq = 1,
                         .weight = 1,
                         .num = (RSNumericRecord){.value = 0}};
  return res;
}

RSIndexResult *NewVirtualResult(double weight) {
  RSIndexResult *res = rm_new(RSIndexResult);

  *res = (RSIndexResult){
      .type = RSResultType_Virtual,
      .docId = 0,
      .fieldMask = 0,
      .freq = 0,
      .weight = weight,
      .isCopy = 0,
  };
  return res;
}

#endif // 0

//---------------------------------------------------------------------------------------------

RSIndexResult::RSIndexResult(const RSIndexResult &src) {
  *this = src;
  isCopy = 1;

  switch (src.type) {
    // copy aggregate types
    case RSResultType_Intersection:
    case RSResultType_Union:
      // allocate a new child pointer array
      agg.children = rm_malloc(src.agg.numChildren * sizeof(RSIndexResult *));
      agg.childrenCap = agg.numChildren;
      // deep copy recursively all children
      for (int i = 0; i < src.agg.numChildren; i++) {
        agg.children[i] = new RSIndexResult(*src.agg.children[i]);
      }
      break;

    // copy term results
    case RSResultType_Term:
      // copy the offset vectors
      if (src.term.offsets.data) {
        term.offsets.data = rm_malloc(term.offsets.len);
        memcpy(term.offsets.data, src.term.offsets.data, term.offsets.len);
      }
      break;

    // the rest have no dynamic stuff, we can just copy the base result
    default:
      break;
  }
}

//---------------------------------------------------------------------------------------------

void RSIndexResult::Print(int depth) const {
  for (int i = 0; i < depth; i++) printf("  ");

  if (type == RSResultType_Term) {
    printf("Term{%llu: %s},\n", (unsigned long long)docId,
           term.term ? term.term->str : "nil");
    return;
  }
  if (type == RSResultType_Virtual) {
    printf("Virtual{%llu},\n", (unsigned long long)docId);
    return;
  }
  if (type == RSResultType_Numeric) {
    printf("Numeric{%llu:%f},\n", (unsigned long long)docId, num.value);
    return;
  }
  printf("%s => %llu{ \n", type == RSResultType_Intersection ? "Inter" : "Union",
         (unsigned long long)docId);

  for (int i = 0; i < agg.numChildren; i++) {
    agg.children[i]->Print(depth + 1);
  }

  for (int i = 0; i < depth; i++) printf("  ");

  printf("},\n");

  // printf("docId: %d, finalScore: %f, flags %x. Terms:\n", docId, finalScore, fieldMask);

  // for (int i = 0; i < numRecords; i++) {
  //   printf("\t%s, %d tf %d, flags %x\n", records[i].term->str, records[i].docId,
  //          records[i].freq, records[i].fieldMask);
  // }
  // printf("----------\n");
}

//---------------------------------------------------------------------------------------------

RSQueryTerm::RSQueryTerm(RSToken *tok, int id) : id(id), idf(1.0), flags(tok->flags),
  str(tok->str ? rm_strndup(tok->str, tok->len) : NULL), len(tok->len) {
}

//---------------------------------------------------------------------------------------------

RSQueryTerm::~RSQueryTerm() {
  if (str) rm_free(str);
}

//---------------------------------------------------------------------------------------------

void RSIndexResult::Reset() {
  docId = 0;
  fieldMask = 0;
  freq = 0;

  if (type == RSResultType_Intersection || type == RSResultType_Union) {
    agg.numChildren = 0;
  }
}

//---------------------------------------------------------------------------------------------

int RSIndexResult::HasOffsets() const {
  switch (type) {
    case RSResultType_Term:
      return term.offsets.len > 0;

    case RSResultType_Intersection:
    case RSResultType_Union:
      // the intersection and union aggregates can have offsets if they are not purely made of
      // virtual results
      return agg.typeMask != RSResultType_Virtual && agg.typeMask != RSResultType_Numeric;

    // a virtual result doesn't have offsets!
    case RSResultType_Virtual:
    case RSResultType_Numeric:
    default:
      return 0;
  }
}

//---------------------------------------------------------------------------------------------

RSIndexResult::~RSIndexResult() {
  if (type == RSResultType_Intersection || type == RSResultType_Union) {
    // for deep-copy results we also free the children
    if (isCopy && agg.children) {
      for (int i = 0; i < agg.numChildren; i++) {
        IndexResult_Free(r->agg.children[i]);
      }
    }
    rm_free(r->agg.children);
    r->agg.children = NULL;
  } else if (r->type == RSResultType_Term) {
    if (r->isCopy) {
      rm_free(r->term.offsets.data);

    } else {  // non copy result...

      // we only free up terms for non copy results
      if (r->term.term != NULL) {
        delete r->term.term;
      }
    }
  }

  rm_free(r);
}

//------------------------------------------------------------------------------`---------------

inline int RSIndexResult_IsAggregate(const RSIndexResult *r) {
  return (r->type & RS_RESULT_AGGREGATE) != 0;
}

#define __absdelta(x, y) (x > y ? x - y : y - x)

//------------------------------------------------------------------------------`---------------

/**
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list
*/

int IndexResult_MinOffsetDelta(const RSIndexResult *r) {
  if (!RSIndexResult_IsAggregate(r) || r->agg.numChildren <= 1) {
    return 1;
  }

  const RSAggregateResult *agg = &r->agg;
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

    uint32_t p1 = v1.Next(v1.ctx, NULL);
    uint32_t p2 = v2.Next(v2.ctx, NULL);
    int cd = __absdelta(p2, p1);
    while (cd > 1 && p1 != RS_OFFSETVECTOR_EOF && p2 != RS_OFFSETVECTOR_EOF) {
      cd = MIN(__absdelta(p2, p1), cd);
      if (p2 > p1) {
        p1 = v1.Next(v1.ctx, NULL);
      } else {
        p2 = v2.Next(v2.ctx, NULL);
      }
    }

    v1.Free(v1.ctx);
    v2.Free(v2.ctx);

    dist += cd * cd;
  }

  // we return 1 if ditance could not be calculate, to avoid division by zero
  return dist ? sqrt(dist) : agg->numChildren - 1;
}

//---------------------------------------------------------------------------------------------

void IndexResult::GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len) const {
  if (len == cap) return;

  switch (type) {
    case RSResultType_Intersection:
    case RSResultType_Union:
      for (int i = 0; i < agg.numChildren; i++) {
        agg.children[i]->GetMatchedTerms(arr, cap, len);
      }
      break;

    case RSResultType_Term:
      if (term.term) {
        const char *s = term.term->str;
        // make sure we have a term string and it's not an expansion
        if (s) {
          arr[len++] = term.term;
        }

        // fprintf(stderr, "Term! %zd\n", *len);
      }
    default:
      return;
  }
}

//---------------------------------------------------------------------------------------------

// Fill an array of max capacity cap with all the matching text terms for the result.
// The number of matching terms is returned.

size_t IndexResult::GetMatchedTerms(RSQueryTerm **arr, size_t cap) {
  size_t arrlen = 0;
  GetMatchedTerms(arr, cap, arrlen);
  return arrlen;
}

//---------------------------------------------------------------------------------------------

bool __indexResult_withinRangeInOrder(RSOffsetIterator *iters, uint32_t *positions, int num,
                                     int maxSlop) {
  while (1) {
    // we start from the beginning, and a span of 0
    int span = 0;
    for (int i = 0; i < num; i++) {
      // take the current position and the position of the previous iterator.
      // For the first iterator we always advance once
      uint32_t pos = i ? positions[i] : iters[i].Next(iters[i].ctx, NULL);
      uint32_t lastPos = i ? positions[i - 1] : 0;
      // printf("Before: i=%d, pos=%d, lastPos %d\n", i, pos, lastPos);

      // read while we are not in order
      while (pos != RS_OFFSETVECTOR_EOF && pos < lastPos) {
        pos = iters[i].Next(iters[i].ctx, NULL);
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
      return true;
    }
  }

  return false;
}

//---------------------------------------------------------------------------------------------

static inline uint32_t _arrayMin(uint32_t *arr, int len, uint32_t *pos) {
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

//---------------------------------------------------------------------------------------------

static inline uint32_t _arrayMax(uint32_t *arr, int len, uint32_t *pos) {
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

//---------------------------------------------------------------------------------------------

/* Check the index result for maximal slop, in an unordered fashion.
 * The algorithm is simple - we find the first offsets min and max such that max-min<=maxSlop */
bool __indexResult_withinRangeUnordered(RSOffsetIterator *iters, uint32_t *positions, int num,
                                       int maxSlop) {
  for (int i = 0; i < num; i++) {
    positions[i] = iters[i].Next(iters[i].ctx, NULL);
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
        return true;
      }
    }

    // if we are not meeting the conditions - advance the minimal iterator
    positions[minPos] = iters[minPos].Next(iters[minPos].ctx, NULL);
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

  return false;
}

//---------------------------------------------------------------------------------------------

/** Test the result offset vectors to see if they fall within a max "slop" or distance between the
 * terms. That is the total number of non matched offsets between the terms is no bigger than
 * maxSlop.
 * e.g. for an exact match, the slop allowed is 0.
 */

bool IndexResult::IsWithinRange(int maxSlop, bool inOrder) const {
  // check if calculation is even relevant here...
  if ((type & (RSResultType_Term | RSResultType_Virtual | RSResultType_Numeric)) || agg.numChildren <= 1) {
    return true;
  }

  RSAggregateResult *r = &agg;
  int num = numChildren;

  // Fill a list of iterators and the last read positions
  RSOffsetIterator iters[num];
  uint32_t positions[num];
  int n = 0;
  for (int i = 0; i < num; i++) {
    // collect only iterators for nodes that can have offsets
    auto &child = *children[i];
    if (child.HasOffsets()) {
      iters[n] = child.IterateOffsets();
      positions[n] = 0;
      n++;
    }
  }

  // No applicable offset children - just return true
  if (n == 0) {
    return true;
  }

  int rc;
  // cal the relevant algorithm based on ordered/unordered condition
  if (inOrder)
    rc = __indexResult_withinRangeInOrder(iters, positions, n, maxSlop);
  else
    rc = __indexResult_withinRangeUnordered(iters, positions, n, maxSlop);
  // printf("slop result for %d: %d\n", ir->docId, rc);
  for (int i = 0; i < n; i++) {
    delete iters[i];
  }
  return !!rc;
}

///////////////////////////////////////////////////////////////////////////////////////////////
