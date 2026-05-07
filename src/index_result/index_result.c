/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "index_result.h"
#include "types_rs.h"
#include "rmalloc.h"
#include <math.h>
#include <sys/param.h>

int RSIndexResult_HasOffsets(const RSIndexResult *res) {
  switch (res->data.tag) {
    case RSResultData_Term:
      return RSOffsetVector_Len(IndexResult_TermOffsetsRef(res)) > 0;
    case RSResultData_Intersection:
    case RSResultData_Union:
    {
      // SAFETY: We checked the tag above, so we can safely assume that res is an aggregate result
      // and skip the tag check on the next line.
      const RSAggregateResult *agg = IndexResult_AggregateRefUnchecked(res);

      const uint8_t mask = AggregateResult_KindMask(agg);
      // the intersection and union aggregates can have offsets if they are not purely made of
      // virtual results
      return mask != RSResultData_Virtual && mask != RS_RESULT_NUMERIC;
    }
    // a virtual result doesn't have offsets!
    case RSResultData_Virtual:
    case RSResultData_Numeric:
    case RSResultData_Metric:
    default:
      return 0;
  }
}

#define __absdelta(x, y) (x > y ? x - y : y - x)
/**
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list
*/
int IndexResult_MinOffsetDelta(const RSIndexResult *r) {
  const RSAggregateResult *agg = IndexResult_AggregateRef(r);
  if (!agg) {
    return 1;
  }

  int dist = 0;
  size_t num = AggregateResult_NumChildren(agg);
  if (num <= 1) {
    return 1;
  }

  RSOffsetIterator v1, v2;
  int i = 0;
  while (i < num) {
    // if either
    while (i < num && !RSIndexResult_HasOffsets(AggregateResult_GetUnchecked(agg, i))) {
      i++;
      continue;
    }
    if (i == num) break;
    v1 = RSIndexResult_IterateOffsets(AggregateResult_GetUnchecked(agg, i));
    i++;

    while (i < num && !RSIndexResult_HasOffsets(AggregateResult_GetUnchecked(agg, i))) {
      i++;
      continue;
    }
    if (i == num) {
      v1.Free(v1.ctx);
      break;
    }
    v2 = RSIndexResult_IterateOffsets(AggregateResult_GetUnchecked(agg, i));

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

  // we return 1 if distance could not be calculate, to avoid division by zero
  return dist ? sqrt(dist) : num - 1;
}

void result_GetMatchedTerms(const RSIndexResult *r, RSQueryTerm *arr[], size_t cap, size_t *len) {
  if (*len == cap) return;

  switch (r->data.tag) {
    case RSResultData_Intersection:
    case RSResultData_Union:
    {
      // SAFETY: We checked the tag above, so we can safely assume that r is an aggregate result
      // and skip the tag check on the next line.
      const RSAggregateResult *agg = IndexResult_AggregateRefUnchecked(r);
      AggregateRecordsSlice children = AggregateResult_GetRecordsSlice(agg);
      for (int i = 0; i < children.len; i++) {
        result_GetMatchedTerms(children.ptr[i], arr, cap, len);
      }

      break;
    }
    case RSResultData_Term:
    {
      RSQueryTerm *term = IndexResult_QueryTermRef(r);
      if (term) {
        size_t s_len = 0;
        // make sure we have a term string and it's not an expansion
        if (QueryTerm_GetStrAndLen(term, &s_len)) {
          arr[(*len)++] = term;
        }
      }
    }
    default:
      return;
  }
}

size_t IndexResult_GetMatchedTerms(const RSIndexResult *r, RSQueryTerm **arr, size_t cap) {
  size_t arrlen = 0;
  result_GetMatchedTerms(r, arr, cap, &arrlen);
  return arrlen;
}
