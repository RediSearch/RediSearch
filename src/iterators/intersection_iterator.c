/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "intersection_iterator.h"
#include "empty_iterator.h"
#include "union_iterator.h"
#include "index_result.h"

/**************************** Read + SkipTo Helpers ****************************/

static inline bool II_currentIsRelevant(IntersectionIterator *it) {
  return IndexResult_IsWithinRange(it->base.current, it->max_slop, it->in_order);
}

static IteratorStatus II_AgreeOnDocId(IntersectionIterator *it) {
  const t_docId docId = it->base.lastDocId;
  AggregateResult_Reset(it->base.current);
  for (uint32_t i = 0; i < it->num_its; i++) {
    RS_ASSERT(it->its[i]->lastDocId <= docId);
    if (it->its[i]->lastDocId < docId) {
      // Advance the iterator to the requested docId
      IteratorStatus rc = it->its[i]->SkipTo(it->its[i], docId);
      if (rc != ITERATOR_OK) {
        if (rc == ITERATOR_EOF) {
          // Some child iterator reached EOF, so the intersection is also at EOF
          it->base.atEOF = true;
        } else if (rc == ITERATOR_NOTFOUND) {
          it->base.lastDocId = it->its[i]->lastDocId;
        }
        return rc;
      }
    }
    AggregateResult_AddChild(it->base.current, it->its[i]->current);
  }
  return ITERATOR_OK;
}

static inline IteratorStatus II_Read_Internal_CheckRelevancy(IntersectionIterator *it) {
  IteratorStatus rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(it);
    if (rc != ITERATOR_OK) {
      continue;
    }
    if (!II_currentIsRelevant(it)) {
      it->base.lastDocId++; // advance the last docId to the next possible value
      continue;
    }
    // Hit!
    return ITERATOR_OK;

  } while (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND);
  return rc;
}

static inline IteratorStatus II_Read_Internal(IntersectionIterator *it) {
  IteratorStatus rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(it);
  } while (rc == ITERATOR_NOTFOUND);
  return rc;
}

/*********************** Intersection Iterator API Implementation ***********************/

static IteratorStatus II_Read_CheckRelevancy(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  const t_docId prevLastDocId = base->lastDocId;

  base->lastDocId++; // advance the last docId. Current docId is at least this
  IteratorStatus rc = II_Read_Internal_CheckRelevancy(it);
  if (rc != ITERATOR_OK) {
    // If we didn't find a relevant result, we need to reset the lastDocId to the previous value
    base->lastDocId = prevLastDocId;
    AggregateResult_Reset(base->current);
  }
  return rc;
}

static IteratorStatus II_Read(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  const t_docId prevLastDocId = base->lastDocId;

  base->lastDocId++; // advance the last docId. Current docId is at least this
  IteratorStatus rc = II_Read_Internal(it);
  if (rc != ITERATOR_OK) {
    // If we didn't find a relevant result, we need to reset the lastDocId to the previous value
    base->lastDocId = prevLastDocId;
    AggregateResult_Reset(base->current);
  }
  return rc;
}

static IteratorStatus II_SkipTo_CheckRelevancy(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  const t_docId prevLastDocId = base->lastDocId;

  base->lastDocId = docId;

  IteratorStatus rc = II_AgreeOnDocId(it);
  if (ITERATOR_OK == rc) {
    if (II_currentIsRelevant(it)) {
      return ITERATOR_OK;
    }
    // Agreed on docId, but not relevant - need to read the next valid result.
    base->lastDocId++; // advance the last docId to the next possible value
  } else if (ITERATOR_NOTFOUND != rc) {
    base->lastDocId = prevLastDocId; // reset the lastDocId to the previous value
    AggregateResult_Reset(base->current);
    return rc; // Unexpected - bubble up
  }

  // Not found - but we need to read the next valid result.
  rc = II_Read_Internal_CheckRelevancy(it);
  // Return rc, switching OK to NOTFOUND
  if (rc == ITERATOR_OK) {
    return ITERATOR_NOTFOUND;
  }
  base->lastDocId = prevLastDocId; // reset the lastDocId to the previous value
  AggregateResult_Reset(base->current);
  return rc; // Unexpected - bubble up
}

static IteratorStatus II_SkipTo(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  const t_docId prevLastDocId = base->lastDocId;

  base->lastDocId = docId;

  IteratorStatus rc = II_AgreeOnDocId(it);
  if (ITERATOR_NOTFOUND == rc) {
    // Not found - but we need to read the next valid result
    rc = II_Read_Internal(it);
    if (rc == ITERATOR_OK) rc = ITERATOR_NOTFOUND;
  }
  if (!(ITERATOR_OK == rc || ITERATOR_NOTFOUND == rc)) {
    base->lastDocId = prevLastDocId; // reset the lastDocId to the previous value
    AggregateResult_Reset(base->current);
  }
  return rc;
}

static size_t II_NumEstimated(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  return it->num_expected;
}

static void II_Rewind(QueryIterator *base) {
  IntersectionIterator *ii = (IntersectionIterator *)base;

  base->atEOF = false;
  base->lastDocId = 0;
  AggregateResult_Reset(base->current);

  // rewind all child iterators
  for (int i = 0; i < ii->num_its; i++) {
    ii->its[i]->Rewind(ii->its[i]);
  }
}

static void II_Free(QueryIterator *base) {
  IntersectionIterator *ii = (IntersectionIterator *)base;

  for (int i = 0; i < ii->num_its; i++) {
    // This function may be called with NULL iterators, so we check for NULLs
    if (ii->its[i]) ii->its[i]->Free(ii->its[i]);
  }

  rm_free(ii->its);
  IndexResult_Free(base->current);
  rm_free(base);
}

/*********************** Constructor helpers for the intersection iterator ***********************/

static inline double iteratorFactor(const QueryIterator *it) {
  double factor = 1.0;
  if (it->type == INTERSECT_ITERATOR) {
  /* on INTERSECT iterator, we divide the estimate by the number of children
   * since we skip as soon as a number is not in all iterators */
    factor = 1.0 / ((const IntersectionIterator *)it)->num_its;
  } else if (it->type == UNION_ITERATOR && RSGlobalConfig.prioritizeIntersectUnionChildren) {
    factor = ((const UnionIterator *)it)->num;
  }
  return factor;
}

typedef int (*CompareFunc)(const void *a, const void *b);
static int cmpIter(QueryIterator **it1, QueryIterator **it2) {
  double factor1 = iteratorFactor(*it1);
  double factor2 = iteratorFactor(*it2);
  double est1 = (*it1)->NumEstimated(*it1) * factor1;
  double est2 = (*it2)->NumEstimated(*it2) * factor2;
  return (int)(est1 - est2);
}

// Set estimation for number of results. Returns false if the query is empty (some of the iterators are NULL)
static bool II_SetEstimation(IntersectionIterator *it) {
  // Set the expected number of results to the minimum of all iterators.
  // If any of the iterators is NULL, we set the expected number to 0
  RS_ASSERT(it->num_its); // Ensure there is at least one iterator, so we can set num_expected to SIZE_MAX temporarily
  it->num_expected = SIZE_MAX;
  for (size_t i = 0; i < it->num_its; ++i) {
    QueryIterator *cur = it->its[i];
    if (!cur) {
      // If the current iterator is empty, then the entire query will fail
      it->num_expected = 0;
      return false;
    }
    size_t amount = cur->NumEstimated(cur);
    if (amount < it->num_expected) {
      it->num_expected = amount;
    }
  }
  return true;
}

QueryIterator *NewIntersectionIterator(QueryIterator **its, size_t num, int max_slop, bool in_order, double weight) {
  RS_ASSERT(its && num > 0);
  IntersectionIterator *it = rm_calloc(1, sizeof(*it));
  it->its = its;
  it->num_its = num;
  it->max_slop = max_slop;
  it->in_order = in_order;

  bool allValid = II_SetEstimation(it);

  // Sort children iterators from low count to high count which reduces the number of iterations.
  if (!in_order && allValid) {
    qsort(its, num, sizeof(*its), (CompareFunc)cmpIter);
  }

  // bind the iterator calls
  QueryIterator *base = &it->base;
  base->type = INTERSECT_ITERATOR;
  base->atEOF = false;
  base->lastDocId = 0;
  base->current = NewIntersectResult(num, weight);
  base->NumEstimated = II_NumEstimated;
  base->Read = max_slop < 0 ? II_Read : II_Read_CheckRelevancy;
  base->SkipTo = max_slop < 0 ? II_SkipTo : II_SkipTo_CheckRelevancy;
  base->Free = II_Free;
  base->Rewind = II_Rewind;

  if (!allValid) {
    // Some of the iterators are NULL, so the intersection will always be empty.
    base->Free(base);
    base = IT_V2(NewEmptyIterator)();
  }
  return base;
}
