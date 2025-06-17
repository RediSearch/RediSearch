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

static inline IteratorStatus II_ReadFromFirstChild(IntersectionIterator *it, t_docId *out) {
  // Read from the first child iterator, which is guaranteed to be non-NULL
  RS_ASSERT(it->num_its > 0 && it->its[0] != NULL);
  QueryIterator *child = it->its[0];
  IteratorStatus rc = child->Read(child);
  if (rc == ITERATOR_OK) {
    *out = child->lastDocId; // If we read successfully, we return the docId
  } else if (rc == ITERATOR_EOF) {
    it->base.atEOF = true; // If the first child is at EOF, the intersection is also at EOF
  }
  return rc;
}

/**
 * Check if all iterators agree on the current docId `curTarget` holds.
 * If they do, aggregate their results into `current` and return ITERATOR_OK.
 * If any of the iterators is at EOF, set `atEOF` to true and return ITERATOR_EOF.
 * If any of the iterators is not at the requested docId, advance it to the requested `docId` and
 * return ITERATOR_NOTFOUND. The caller may retry calling this function in that case.
 * In case of an error, return the error code.
 */
static IteratorStatus II_AgreeOnDocId(IntersectionIterator *it, t_docId *curTarget) {
  const t_docId docId = *curTarget;

  for (uint32_t i = 0; i < it->num_its; i++) {
    RS_ASSERT(it->its[i]->lastDocId <= docId);
    if (it->its[i]->lastDocId < docId) {
      // Advance the iterator to the requested docId
      IteratorStatus rc = it->its[i]->SkipTo(it->its[i], docId);
      if (rc != ITERATOR_OK) { // Not OK, we break out of the loop
        if (rc == ITERATOR_EOF) {
          // Some child iterator reached EOF, so the intersection is also at EOF
          it->base.atEOF = true;
        } else if (rc == ITERATOR_NOTFOUND) {
          // The child iterator did not find the requested docId, so we need to advance the lastDocId
          // to the next possible value (the result that the child iterator yielded)
          *curTarget = it->its[i]->lastDocId;
        }
        return rc;
      }
    }
  }
  // All iterators agree on the docId, so we can set the current result
  AggregateResult_Reset(it->base.current);
  for (uint32_t i = 0; i < it->num_its; i++) {
    RS_ASSERT(docId == it->its[i]->current->docId);
    AggregateResult_AddChild(it->base.current, it->its[i]->current);
  }
  return ITERATOR_OK;
}

static inline IteratorStatus II_Find_Consensus_WithRelevancyCheck(IntersectionIterator *it, t_docId docId) {
  IteratorStatus rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(it, &docId);
    if (rc != ITERATOR_OK) {
      continue;
    }
    if (!II_currentIsRelevant(it)) {
      rc = II_ReadFromFirstChild(it, &docId);
      continue;
    }
    // Hit!
    it->base.lastDocId = it->base.current->docId;
    return ITERATOR_OK;

  } while (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND);
  return rc;
}

static inline IteratorStatus II_Find_Consensus(IntersectionIterator *it, t_docId docId) {
  IteratorStatus rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(it, &docId);
  } while (rc == ITERATOR_NOTFOUND);
  if (rc == ITERATOR_OK) {
    it->base.lastDocId = it->base.current->docId;
  }
  return rc;
}

/*********************** Intersection Iterator API Implementation ***********************/

static IteratorStatus II_Read_CheckRelevancy(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  t_docId target;
  IteratorStatus rc = II_ReadFromFirstChild(it, &target);
  if (rc == ITERATOR_OK) {
    rc = II_Find_Consensus_WithRelevancyCheck(it, target);
  }
  return rc;
}

static IteratorStatus II_Read(QueryIterator *base) {
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  t_docId target;
  IteratorStatus rc = II_ReadFromFirstChild(it, &target);
  if (rc == ITERATOR_OK) {
    rc = II_Find_Consensus(it, target);
  }
  return rc;
}

static IteratorStatus II_SkipTo_CheckRelevancy(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  IteratorStatus rc = II_AgreeOnDocId(it, &docId);
  if (ITERATOR_OK == rc) {
    if (II_currentIsRelevant(it)) {
      it->base.lastDocId = it->base.current->docId;
      return ITERATOR_OK;
    }
    // Agreed on docId, but not relevant - need to read the next valid result.
    rc = II_ReadFromFirstChild(it, &docId);
    if (rc != ITERATOR_OK) return rc; // If we failed to read, bubble up the error
  } else if (ITERATOR_NOTFOUND != rc) {
    return rc; // Unexpected - bubble up
  }

  // Not found - but we need to read the next valid result.
  // `docId` is set to the next valid docId (by `II_AgreeOnDocId` or `II_ReadFromFirstChild`).
  rc = II_Find_Consensus_WithRelevancyCheck(it, docId);
  // Return rc, switching OK to NOTFOUND
  if (rc == ITERATOR_OK) {
    return ITERATOR_NOTFOUND;
  }
  return rc; // Unexpected - bubble up
}

static IteratorStatus II_SkipTo(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  IntersectionIterator *it = (IntersectionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  IteratorStatus rc = II_AgreeOnDocId(it, &docId);
  if (ITERATOR_OK == rc) {
    it->base.lastDocId = it->base.current->docId;
  } else if (ITERATOR_NOTFOUND == rc) {
    // Not found - but we need to read the next valid result
    IteratorStatus read_rc = II_Find_Consensus(it, docId);
    if (read_rc != ITERATOR_OK) return read_rc; // If we failed to read, bubble up the error
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
  for (uint32_t i = 0; i < ii->num_its; i++) {
    ii->its[i]->Rewind(ii->its[i]);
  }
}

static void II_Free(QueryIterator *base) {
  IntersectionIterator *ii = (IntersectionIterator *)base;

  for (uint32_t i = 0; i < ii->num_its; i++) {
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
    // on INTERSECT iterator, we divide the estimate by the number of children
    // since we skip as soon as a number is not in all iterators
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
  for (uint32_t i = 0; i < it->num_its; ++i) {
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
  // If max_slop is negative, we set it to INT_MAX in case `in_order` is true
  it->max_slop = max_slop < 0 ? INT_MAX : max_slop;
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
  if (max_slop < 0 && !in_order) {
    // No slop and no order means every result is relevant, so we can use the fast path
    base->Read = II_Read;
    base->SkipTo = II_SkipTo;
  } else {
    // Otherwise, we need to check relevancy
    base->Read = II_Read_CheckRelevancy;
    base->SkipTo = II_SkipTo_CheckRelevancy;
  }
  base->Free = II_Free;
  base->Rewind = II_Rewind;

  if (!allValid) {
    // Some of the iterators are NULL, so the intersection will always be empty.
    base->Free(base);
    base = IT_V2(NewEmptyIterator)();
  }
  return base;
}
