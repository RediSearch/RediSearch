/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "forward_index.h"
#include "index.h"
#include "varint.h"
#include "spec.h"
#include <math.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "util/heap.h"
#include "profile.h"
#include "hybrid_reader.h"
#include "metric_iterator.h"
#include "optimizer_reader.h"
#include "util/units.h"

static int UI_SkipToHigh(IndexIterator *base, t_docId docId, RSIndexResult **hit);
static int UI_SkipToQuick(IndexIterator *base, t_docId docId, RSIndexResult **hit);
static int UI_SkipToFull(IndexIterator *base, t_docId docId, RSIndexResult **hit);
static inline int UI_ReadUnsorted(IndexIterator *base, RSIndexResult **hit);
static int UI_ReadFull(IndexIterator *base, RSIndexResult **hit);
static int UI_ReadQuick(IndexIterator *base, RSIndexResult **hit);
static int UI_ReadSortedHigh(IndexIterator *base, RSIndexResult **hit);
static size_t UI_NumEstimated(IndexIterator *base);

static int II_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit);
static int II_ReadSorted(IndexIterator *base, RSIndexResult **hit);
static size_t II_NumEstimated(IndexIterator *base);

#define CURRENT_RECORD(ii) (ii)->base.current

int cmpLastDocId(const void *e1, const void *e2, const void *udata) {
  const IndexIterator *it1 = e1, *it2 = e2;
  if (it1->LastDocId < it2->LastDocId) {
    return 1;
  } else if (it1->LastDocId > it2->LastDocId) {
    return -1;
  }
  return 0;
}


// Profile iterator, used for profiling. PI is added between all iterator
typedef struct {
  IndexIterator base;
  IndexIterator *child;
  size_t counter;
  clock_t cpuTime;
  int eof;
} ProfileIterator, ProfileIteratorCtx;

typedef struct {
  IndexIterator base;
  /**
   * We maintain two iterator arrays. One is the original iterator list, and
   * the other is the list of currently active iterators. When an iterator
   * reaches EOF, it is set to NULL in the `its` list, but is still retained in
   * the `origits` list, for the purpose of supporting things like Rewind() and
   * Free()
   */
  IndexIterator **its;
  IndexIterator **origits;
  uint32_t num;
  uint32_t norig;
  uint32_t currIt;
  heap_t *heapMinId;

  // If set to 1, we exit skips after the first hit found and not merge further results
  int quickExit;
  size_t nexpected;
  double weight;

  // type of query node UNION,GEO,NUMERIC...
  QueryNodeType origType;
  // original string for fuzzy or prefix unions
  const char *qstr;
} UnionIterator;

static void resetMinIdHeap(UnionIterator *ui) {
  heap_t *hp = ui->heapMinId;
  heap_clear(hp);

  for (int i = 0; i < ui->num; i++) {
    heap_offerx(hp, ui->its[i]);
  }
  RS_LOG_ASSERT(heap_count(hp) == ui->num,
                "count should be equal to number of iterators");
}

static void UI_HeapAddChildren(UnionIterator *ui, IndexIterator *it) {
  AggregateResult_AddChild(CURRENT_RECORD(ui), IITER_CURRENT_RECORD(it));
}

static void UI_SyncIterList(UnionIterator *ui) {
  ui->num = ui->norig;
  memcpy(ui->its, ui->origits, sizeof(*ui->its) * ui->norig);
  if (ui->heapMinId) {
    resetMinIdHeap(ui);
  }
}

/**
 * Removes the exhausted iterator from the active list, so that future
 * reads will no longer iterate over it
 */
static inline int UI_RemoveExhausted(UnionIterator *it, int badix) {
  // e.g. assume we have 10 entries, and we want to remove index 8, which means
  // one more valid entry at the end. This means we use
  // source: its + 8 + 1
  // destination: its + 8
  // number: it->len (10) - (8) - 1 == 1
  memmove(it->its + badix, it->its + badix + 1, sizeof(*it->its) * (it->num - badix - 1));
  it->num--;
  // Repeat the same index again, because we have a new iterator at the same
  // position
  return badix - 1;
}

static void UI_Abort(IndexIterator *base) {
  UnionIterator *it = (UnionIterator *)base;
  IITER_SET_EOF(base);
  for (int i = 0; i < it->num; i++) {
    if (it->its[i]) {
      it->its[i]->Abort(it->its[i]);
    }
  }
}

static void UI_Rewind(IndexIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  IITER_CLEAR_EOF(base);
  base->LastDocId = 0;
  CURRENT_RECORD(ui)->docId = 0;

  UI_SyncIterList(ui);

  // rewind all child iterators
  for (size_t i = 0; i < ui->num; i++) {
    ui->its[i]->Rewind(ui->its[i]);
  }
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, int quickExit,
                                double weight, QueryNodeType type, const char *qstr, IteratorsConfig *config) {
  // create union context
  UnionIterator *ctx = rm_calloc(1, sizeof(UnionIterator));
  ctx->origits = its;
  ctx->weight = weight;
  ctx->origType = type;
  ctx->num = num;
  ctx->norig = num;
  IITER_CLEAR_EOF(&ctx->base);
  CURRENT_RECORD(ctx) = NewUnionResult(num, weight);
  ctx->quickExit = quickExit;
  ctx->its = rm_calloc(ctx->num, sizeof(*ctx->its));
  ctx->nexpected = 0;
  ctx->currIt = 0;
  ctx->heapMinId = NULL;
  ctx->qstr = qstr;

  // bind the union iterator calls
  IndexIterator *it = &ctx->base;
  it->LastDocId = 0;
  it->type = UNION_ITERATOR;
  it->NumEstimated = UI_NumEstimated;
  it->Read = quickExit ? UI_ReadQuick : UI_ReadFull;
  it->SkipTo = quickExit ? UI_SkipToQuick : UI_SkipToFull;
  it->HasNext = NULL;
  it->Free = UnionIterator_Free;
  it->Abort = UI_Abort;
  it->Rewind = UI_Rewind;
  UI_SyncIterList(ctx);

  for (size_t i = 0; i < num; ++i) {
    ctx->nexpected += IITER_NUM_ESTIMATED(its[i]);
  }

  if (ctx->norig > config->minUnionIterHeap && 0) {
    it->Read = UI_ReadSortedHigh;
    it->SkipTo = UI_SkipToHigh;
    ctx->heapMinId = rm_malloc(heap_sizeof(num));
    heap_init(ctx->heapMinId, cmpLastDocId, NULL, num);
    resetMinIdHeap(ctx);
  }

  return it;
}

void UI_Foreach(IndexIterator *index_it, void (*callback)(IndexReader *it)) {
  UnionIterator *ui = (UnionIterator *)index_it;
  for (int i = 0; i < ui->num; ++i) {
    IndexIterator *it = ui->its[i];
    if (it->type == PROFILE_ITERATOR) {
      // If this is a profile query, each IndexReader is wrapped in a ProfileIterator
      it = ((ProfileIterator *)it)->child;
    }
    RS_LOG_ASSERT_FMT(it->type == READ_ITERATOR, "Expected read iterator, got %d", it->type);
    callback((IndexReader *)it);
  }
}

static size_t UI_NumEstimated(IndexIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  return ui->nexpected;
}

static inline int UI_ReadUnsorted(IndexIterator *base, RSIndexResult **hit) {
  UnionIterator *ui = (UnionIterator *)base;
  int rc = INDEXREAD_OK;
  RSIndexResult *res = NULL;
  while (ui->currIt < ui->num) {
    rc = ui->origits[ui->currIt]->Read(ui->origits[ui->currIt], &res);
    if (rc == INDEXREAD_OK) {
      *hit = res;
      return rc;
    }
    ++ui->currIt;
  }
  return INDEXREAD_EOF;
}

static inline int UI_SkipAdvanceLagging(UnionIterator *ui) {
  RSIndexResult *h;
  const t_docId nextId = ui->base.LastDocId;
  ui->base.LastDocId = UINT64_MAX;
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId < nextId) {
      int rc = cur->SkipTo(cur, nextId, &h);
      if (rc == INDEXREAD_EOF) {
        i = UI_RemoveExhausted(ui, i);
        continue;
      } else if (rc != INDEXREAD_OK && rc != INDEXREAD_NOTFOUND) {
        return rc;
      }
    }
    if (ui->base.LastDocId > cur->LastDocId) ui->base.LastDocId = cur->LastDocId;
  }
  return INDEXREAD_OK;
}

static inline int UI_ReadAdvanceLagging(UnionIterator *ui) {
  RSIndexResult *h;
  const t_docId nextId = ui->base.LastDocId;
  ui->base.LastDocId = UINT64_MAX;
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId < nextId) {
      int rc = cur->Read(cur, &h);
      if (rc == INDEXREAD_EOF) {
        i = UI_RemoveExhausted(ui, i);
        continue;
      } else if (rc != INDEXREAD_OK) {
        return rc;
      }
    }
    if (ui->base.LastDocId > cur->LastDocId) ui->base.LastDocId = cur->LastDocId;
  }
  return INDEXREAD_OK;
}

static inline void UI_SetFull(UnionIterator *ui) {
  AggregateResult_Reset(CURRENT_RECORD(ui));
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId == ui->base.LastDocId) {
      AggregateResult_AddChild(CURRENT_RECORD(ui), cur->current);
    }
  }
}

static inline void UI_SetFirst(UnionIterator *ui) {
  AggregateResult_Reset(CURRENT_RECORD(ui));
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId == ui->base.LastDocId) {
      AggregateResult_AddChild(CURRENT_RECORD(ui), cur->current);
      return;
    }
  }
}

static int UI_ReadFull(IndexIterator *base, RSIndexResult **hit) {
  UnionIterator *ui = (UnionIterator *)base;

  if (ui->num == 0 || !IITER_HAS_NEXT(&ui->base)) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  base->LastDocId++; // advance the last docId to the minimal expected value
  int rc = UI_ReadAdvanceLagging(ui);
  if (rc != INDEXREAD_OK) return rc;

  if (ui->num == 0) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  // Not depleted yet
  UI_SetFull(ui);
  if (hit) *hit = CURRENT_RECORD(ui);
  return INDEXREAD_OK;
}

static int UI_ReadQuick(IndexIterator *base, RSIndexResult **hit) {
  UnionIterator *ui = (UnionIterator *)base;

  if (ui->num == 0 || !IITER_HAS_NEXT(&ui->base)) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  base->LastDocId++; // advance the last docId to the minimal expected value
  // UI_SkipAdvanceLagging(ui); // TODO: use skip with a quick abort if we have a candidate for ui->minID + 1
  int rc = UI_ReadAdvanceLagging(ui);
  if (rc != INDEXREAD_OK) return rc;

  if (ui->num == 0) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  // Not depleted yet
  UI_SetFirst(ui);
  if (hit) *hit = CURRENT_RECORD(ui);
  return INDEXREAD_OK;
}

static int UI_SkipToFull(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  UnionIterator *ui = (UnionIterator *)base;

  if (ui->num == 0 || !IITER_HAS_NEXT(base)) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  ui->base.LastDocId = docId; // advance the last docId to the minimal expected value
  int rc = UI_SkipAdvanceLagging(ui); // advance lagging iterators to `docId` or above
  if (rc != INDEXREAD_OK) return rc;

  if (ui->num == 0) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  // Not depleted yet
  UI_SetFull(ui);
  if (hit) *hit = CURRENT_RECORD(ui);
  return ui->base.LastDocId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}

static int UI_SkipToQuick(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  UnionIterator *ui = (UnionIterator *)base;

  if (ui->num == 0 || !IITER_HAS_NEXT(base)) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  ui->base.LastDocId = docId; // advance the last docId to the minimal expected value
  int rc = UI_SkipAdvanceLagging(ui); // advance lagging iterators to `docId` or above
  if (rc != INDEXREAD_OK) return rc;

  if (ui->num == 0) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  // Not depleted yet
  UI_SetFirst(ui);
  if (hit) *hit = CURRENT_RECORD(ui);
  return ui->base.LastDocId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
}

// UI_Read for iterator with high count of children
static inline int UI_ReadSortedHigh(IndexIterator *base, RSIndexResult **hit) {
  UnionIterator *ui = (UnionIterator *)base;
  IndexIterator *it = NULL;
  RSIndexResult *res;
  heap_t *hp = ui->heapMinId;

  // nothing to do
  if (!IITER_HAS_NEXT(&ui->base)) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }
  AggregateResult_Reset(CURRENT_RECORD(ui));
  t_docId nextValidId = ui->base.LastDocId + 1;

  /*
   * A min-heap maintains all sub-iterators which are not EOF.
   * In a loop, the iterator in heap root is checked. If it is valid, it is used,
   * otherwise, Read() is called on sub-iterator and it is returned into the heap
   * for future calls.
   */
  while (heap_count(hp)) {
    it = heap_peek(hp);
    res = IITER_CURRENT_RECORD(it);
    if (it->LastDocId >= nextValidId && it->LastDocId != 0) {
      // valid result since id at root of min-heap is higher than union min id
      break;
    }
    // read the next result and if valid, return the iterator into the heap
    int rc = it->SkipTo(it, nextValidId, &res);

    // refresh heap with iterator with updated LastDocId
    if (rc == INDEXREAD_EOF) {
      heap_poll(hp);
    } else {
      it->LastDocId = res->docId;
      heap_replace(hp, it);
      // after SkipTo, try test again for validity
      if (ui->quickExit && it->LastDocId == nextValidId) {
        break;
      }
    }
  }

  if (!heap_count(hp)) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  ui->base.LastDocId = it->LastDocId;

  // On quickExit we just return one result.
  // Otherwise, we collect all the results that equal to the root of the heap.
  if (ui->quickExit) {
    AggregateResult_AddChild(CURRENT_RECORD(ui), res);
  } else {
    heap_cb_root(hp, (HeapCallback)UI_HeapAddChildren, ui);
  }

  *hit = CURRENT_RECORD(ui);
  return INDEXREAD_OK;
}

// UI_SkipTo for iterator with high count of children
static int UI_SkipToHigh(IndexIterator *base, t_docId docId, RSIndexResult **hit) {

  if (docId == 0) {
    // return UI_ReadSorted(base, hit);
  }

  if (!IITER_HAS_NEXT(base)) {
    return INDEXREAD_EOF;
  }
  UnionIterator *ui = (UnionIterator *)base;

  AggregateResult_Reset(CURRENT_RECORD(ui));
  CURRENT_RECORD(ui)->weight = ui->weight;
  int rc = INDEXREAD_EOF;
  IndexIterator *it = NULL;
  RSIndexResult *res;
  heap_t *hp = ui->heapMinId;

  while (heap_count(hp)) {
    it = heap_peek(hp);
    if (it->LastDocId >= docId) {
      // if the iterator is at or ahead of docId - we avoid reading the entry
      // in this case, we are either past or at the requested docId, no need to actually read
      break;
    }

    rc = it->SkipTo(it, docId, &res);
    if (rc == INDEXREAD_EOF) {
      heap_poll(hp); // return value was already received from heap_peak
      // iterator is not returned to heap
      continue;
    }
    RS_LOG_ASSERT(res, "should not be NULL");

    // refresh heap with iterator with updated LastDocId
    it->LastDocId = res->docId;
    heap_replace(hp, it);
    if (ui->quickExit && it->LastDocId == docId) {
      break;
    }
  }

  if (heap_count(hp) == 0) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  rc = (it->LastDocId == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;

  // On quickExit we just return one result.
  // Otherwise, we collect all the results that equal to the root of the heap.
  if (ui->quickExit) {
    AggregateResult_AddChild(CURRENT_RECORD(ui), IITER_CURRENT_RECORD(it));
  } else {
    heap_cb_root(hp, (HeapCallback)UI_HeapAddChildren, ui);
  }

  ui->base.LastDocId = it->LastDocId;
  *hit = CURRENT_RECORD(ui);
  return rc;
}

void UnionIterator_Free(IndexIterator *base) {
  if (base == NULL) return;

  UnionIterator *ui = (UnionIterator *)base;
  for (int i = 0; i < ui->norig; i++) {
    IndexIterator *it = ui->origits[i];
    if (it) {
      it->Free(it);
    }
  }

  IndexResult_Free(CURRENT_RECORD(ui));
  if (ui->heapMinId) heap_free(ui->heapMinId);
  rm_free(ui->its);
  rm_free(ui->origits);
  rm_free(ui);
}

void trimUnionIterator(IndexIterator *iter, size_t offset, size_t limit, bool asc) {
  RS_LOG_ASSERT(iter->type == UNION_ITERATOR, "trim applies to union iterators only");
  UnionIterator *ui = (UnionIterator *)iter;
  if (ui->norig <= 2) { // nothing to trim
    return;
  }

  size_t curTotal = 0;
  int i;
  if (offset == 0) {
    if (asc) {
      for (i = 1; i < ui->num; ++i) {
        IndexIterator *it = ui->origits[i];
        curTotal += it->NumEstimated(it);
        if (curTotal > limit) {
          ui->num = i + 1;
          memset(ui->its + ui->num, 0, ui->norig - ui->num);
          break;
        }
      }
    } else {  //desc
      for (i = ui->num - 2; i > 0; --i) {
        IndexIterator *it = ui->origits[i];
        curTotal += it->NumEstimated(it);
        if (curTotal > limit) {
          ui->num -= i;
          memmove(ui->its, ui->its + i, ui->num);
          memset(ui->its + ui->num, 0, ui->norig - ui->num);
          break;
        }
      }
    }
  } else {
    UI_SyncIterList(ui);
  }
  iter->Read = UI_ReadUnsorted;
}

/* The context used by the intersection methods during iterating an intersect
 * iterator */
typedef struct {
  IndexIterator base;
  IndexIterator **its;
  unsigned num;
  int maxSlop;
  bool inOrder;
  double weight;
  size_t nexpected;
} IntersectIterator;

void IntersectIterator_Free(IndexIterator *base) {
  if (base == NULL) return;
  IntersectIterator *ui = (IntersectIterator *)base;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i] != NULL) {
      ui->its[i]->Free(ui->its[i]);
    }
  }

  rm_free(ui->its);
  IndexResult_Free(base->current);
  rm_free(base);
}

static void II_Abort(IndexIterator *base) {
  IntersectIterator *it = (IntersectIterator *)base;
  IITER_SET_EOF(base);
  for (int i = 0; i < it->num; i++) {
    if (it->its[i]) {
      it->its[i]->Abort(it->its[i]);
    }
  }
}

static void II_Rewind(IndexIterator *base) {
  IntersectIterator *ii = (IntersectIterator *)base;
  IITER_CLEAR_EOF(base);
  base->LastDocId = 0;

  // rewind all child iterators
  for (int i = 0; i < ii->num; i++) {
    if (ii->its[i]) {
      ii->its[i]->Rewind(ii->its[i]);
    }
  }
}

typedef int (*CompareFunc)(const void *a, const void *b);
static int cmpIter(IndexIterator **it1, IndexIterator **it2) {

  double factor1 = 1;
  double factor2 = 1;
  enum iteratorType it_1_type = (*it1)->type;
  enum iteratorType it_2_type = (*it2)->type;

  /*
   * on INTERSECT iterator, we divide the estimate by the number of children
   * since we skip as soon as a number is not in all iterators */
  if (it_1_type == INTERSECT_ITERATOR) {
    factor1 = 1 / MAX(1, ((IntersectIterator *)*it1)->num);
  } else if (it_1_type == UNION_ITERATOR && RSGlobalConfig.prioritizeIntersectUnionChildren) {
    factor1 = ((UnionIterator *)*it1)->num;
  }
  if (it_2_type == INTERSECT_ITERATOR) {
    factor2 = 1 / MAX(1, ((IntersectIterator *)*it2)->num);
  } else if (it_2_type == UNION_ITERATOR && RSGlobalConfig.prioritizeIntersectUnionChildren) {
    factor2 = ((UnionIterator *)*it2)->num;
  }

  return (int)((*it1)->NumEstimated((*it1)) * factor1 - (*it2)->NumEstimated((*it2)) * factor2);
}

// Set estimation for number of results. Returns false if the query is empty (some of the iterators are NULL)
static bool II_SetEstimation(IntersectIterator *ctx) {
  /**
   * 1. Go through all the iterators, ensuring none of them is NULL
   *    (replace with empty if indeed NULL)
   */
  ctx->nexpected = SIZE_MAX;
  for (size_t i = 0; i < ctx->num; ++i) {
    IndexIterator *curit = ctx->its[i];

    if (!curit) {
      // If the current iterator is empty, then the entire
      // query will fail;
      ctx->nexpected = 0;
      return false;
    }

    size_t amount = IITER_NUM_ESTIMATED(curit);
    if (amount < ctx->nexpected) {
      ctx->nexpected = amount;
    }
  }
  return true;
}

void AddIntersectIterator(IndexIterator *parentIter, IndexIterator *childIter) {
  RS_LOG_ASSERT(parentIter->type == INTERSECT_ITERATOR, "add applies to intersect iterators only");
  IntersectIterator *ii = (IntersectIterator *)parentIter;
  ii->num++;
  ii->its = rm_realloc(ii->its, ii->num);
  ii->its[ii->num - 1] = childIter;
}

IndexIterator *NewIntersectIterator(IndexIterator **its_, size_t num, DocTable *dt,
                                   t_fieldMask fieldMask, int maxSlop, int inOrder, double weight) {
  // printf("Creating new intersection iterator with fieldMask=%llx\n", fieldMask);
  IntersectIterator *ctx = rm_calloc(1, sizeof(*ctx));
  ctx->maxSlop = maxSlop;
  ctx->inOrder = inOrder;
  // ctx->fieldMask = fieldMask;
  ctx->weight = weight;

  ctx->base.current = NewIntersectResult(num, weight);
  ctx->its = its_;
  ctx->num = num;

  bool allValid = II_SetEstimation(ctx);
  ctx->base.isValid = allValid;

  // Sort children iterators from low count to high count which reduces the number of iterations.
  if (!ctx->inOrder && allValid) {
    qsort(ctx->its, ctx->num, sizeof(*ctx->its), (CompareFunc)cmpIter);
  }

  // bind the iterator calls
  IndexIterator *it = &ctx->base;
  it->type = INTERSECT_ITERATOR;
  it->LastDocId = 0;
  it->NumEstimated = II_NumEstimated;
  it->Read = II_ReadSorted;
  it->SkipTo = II_SkipTo;
  it->Free = IntersectIterator_Free;
  it->Abort = II_Abort;
  it->Rewind = II_Rewind;
  it->HasNext = NULL;
  return it;
}

static int II_AgreeOnDocId(IntersectIterator *ic) {
  t_docId docId = ic->base.LastDocId;
  for (int i = 0; i < ic->num; i++) {
    RS_LOG_ASSERT_FMT(ic->its[i]->LastDocId <= docId, "docId %zu, docIds[%d] %zu", docId, i, ic->its[i]->LastDocId); // todo: remove
    if (ic->its[i]->LastDocId < docId) {
      int rc = ic->its[i]->SkipTo(ic->its[i], docId, &ic->its[i]->current);
      if (rc != INDEXREAD_OK) {
        if (rc == INDEXREAD_EOF) {
          IITER_SET_EOF(&ic->base);
        } else if (rc == INDEXREAD_NOTFOUND) {
          ic->base.LastDocId = ic->its[i]->LastDocId;
        }
        return rc;
      }
    }
  }
  return INDEXREAD_OK;
}

static inline void II_setResult(IntersectIterator *ic) {
  AggregateResult_Reset(ic->base.current);
  for (int i = 0; i < ic->num; i++) {
    AggregateResult_AddChild(ic->base.current, ic->its[i]->current);
  }
}

static inline bool II_currentIsRelevant(IntersectIterator *ic) {
  // // make sure the flags are matching.
  // if ((ic->base.current->fieldMask & ic->fieldMask) == 0) {
  //   return false;
  // }
  // If we need to match slop and order, we do it now, and possibly skip the result
  if (ic->maxSlop >= 0 && !IndexResult_IsWithinRange(ic->base.current, ic->maxSlop, ic->inOrder)) {
    return false;
  }
  return true;
}

static inline int II_Read_Internal(IntersectIterator *ic, RSIndexResult **hit) {
  int rc;
  do { // retry until we agree on the docId
    rc = II_AgreeOnDocId(ic);
    if (rc != INDEXREAD_OK) {
      continue;
    }
    II_setResult(ic);
    if (!II_currentIsRelevant(ic)) {
      continue;
    }
    // Hit!
    if (hit) *hit = ic->base.current;
    return INDEXREAD_OK;

  } while (rc == INDEXREAD_OK || rc == INDEXREAD_NOTFOUND);
  return rc;
}


static int II_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  if (!base->isValid) {
    return INDEXREAD_EOF;
  }
  IntersectIterator *ic = (IntersectIterator *)base;

  RS_LOG_ASSERT_FMT(base->LastDocId < docId, "lastDocId %zu, docId %zu", base->LastDocId, docId);
  base->LastDocId = docId;

  int rc = II_AgreeOnDocId(ic);
  if (INDEXREAD_OK == rc) {
    II_setResult(ic);

    if (II_currentIsRelevant(ic)) {
      if (hit) *hit = ic->base.current;
      return INDEXREAD_OK;
    }
  } else if (INDEXREAD_NOTFOUND != rc) {
    return rc; // Unexpected - bubble up
  }

  // Not found - but we need to read the next valid result into hit
  rc = II_Read_Internal(ic, hit);
  // Return rc, switching OK to NOTFOUND
  if (rc == INDEXREAD_OK)
    return INDEXREAD_NOTFOUND;
  else
    return rc; // Unexpected - bubble up
}

static size_t II_NumEstimated(IndexIterator *base) {
  IntersectIterator *ic = (IntersectIterator *)base;
  return ic->nexpected;
}

static int II_ReadSorted(IndexIterator *base, RSIndexResult **hit) {
  IntersectIterator *ic = (IntersectIterator *)base;

  if (!ic->base.isValid) {
    return INDEXREAD_EOF;
  }

  base->LastDocId++; // advance the last docId. Current docId is at least this
  return II_Read_Internal(ic, hit);
}

/* A Not iterator works by wrapping another iterator, and returning OK for
 * misses, and NOTFOUND for hits. It takes its reference from a wildcard iterator
 * if `INDEXALL` is on (optimization). */
typedef struct {
  IndexIterator base;         // base index iterator
  IndexIterator *wcii;        // wildcard index iterator
  IndexIterator *child;       // child index iterator
  t_docId maxDocId;
  double weight;
  TimeoutCtx timeoutCtx;
} NotIterator, NotContext;

static void NI_Abort(IndexIterator *base) {
  IITER_CLEAR_EOF(base);
  NotContext *nc = (NotContext *)base;
  if (nc->wcii) {
    nc->wcii->Abort(nc->wcii);
  }
  nc->child->Abort(nc->child);
}

static void NI_Rewind(IndexIterator *base) {
  NotContext *nc = (NotContext *)base;
  if (nc->wcii) {
    nc->wcii->Rewind(nc->wcii);
  }
  nc->base.current->docId = 0;
  IITER_CLEAR_EOF(base);
  base->LastDocId = 0;
  nc->child->Rewind(nc->child);
}

static void NI_Free(IndexIterator *base) {
  NotContext *nc = (NotContext *)base;
  nc->child->Free(nc->child);
  if (nc->wcii) {
    nc->wcii->Free(nc->wcii);
  }
  IndexResult_Free(nc->base.current);
  rm_free(base);
}

/* SkipTo for NOT iterator - Non-optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static int NI_SkipTo_NO(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;

  // do not skip beyond max doc id
  if (docId > nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  // Get the child's last read docId
  t_docId childId = nc->child->LastDocId;

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  // So we are okay!
  if (childId > docId || !IITER_HAS_NEXT(nc->child)) {
    goto ok;
  }

  if (childId < docId) {
    // read the next entry from the child
    int rc = nc->child->SkipTo(nc->child, docId, hit);
    if (rc != INDEXREAD_OK) {
      goto ok; // EOF or NOTFOUND - we have a match
    }
  }
  // If the child docId is the one we are looking for, it's an anti match!
  // We need to return NOTFOUND and set hit to the next valid docId
  RSIndexResult *child_res;
  int rc;
  do {
    docId++;
    rc = nc->child->Read(nc->child, &child_res);
  } while (child_res->docId == docId && rc == INDEXREAD_OK);
  nc->base.current->docId = base->LastDocId = docId;
  *hit = nc->base.current;
  return INDEXREAD_NOTFOUND;

ok:
  // NOT FOUND or end means OK. We need to set the docId to the hit we will bubble up
  nc->base.current->docId = base->LastDocId = docId;
  *hit = nc->base.current;
  return INDEXREAD_OK;
}

/* SkipTo for NOT iterator - Optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
int NI_SkipTo_O(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;

  // do not skip beyond max doc id
  if (docId > nc->maxDocId) {
    IITER_SET_EOF(nc->wcii);
    IITER_SET_EOF(&nc->base);
    return INDEXREAD_EOF;
  }

  // Get the child's last read docId
  t_docId childId = nc->child->LastDocId;

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  // So we are okay!
  if (childId > docId || !IITER_HAS_NEXT(nc->child)) {
    goto ok;
  }

  // If the child docId is the one we are looking for, it's an anti match!
  int wcii_rc;
  if (childId == docId) {
    // Skip the inner wildcard to `docId`, and return NOTFOUND
    wcii_rc = nc->wcii->SkipTo(nc->wcii, docId, hit);
    if (wcii_rc == INDEXREAD_EOF) {
      IITER_SET_EOF(&nc->base);
    }
    // Note: If this is the last block in the child index and not in the wildcard
    // index, we may have a docId in the child that does not exist in the
    // wildcard index
    nc->base.current->docId = base->LastDocId = nc->wcii->LastDocId;
    *hit = nc->base.current;
    return INDEXREAD_NOTFOUND;
  }

  // read the next entry from the child
  int rc = nc->child->SkipTo(nc->child, docId, hit);

  // OK means not found
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }

ok:
  // NOT FOUND or end at child means OK. We need to set the docId to the hit we
  // will bubble up
  wcii_rc = nc->wcii->SkipTo(nc->wcii, docId, hit);
  nc->base.current->docId = base->LastDocId = nc->wcii->LastDocId;
  if (wcii_rc == INDEXREAD_EOF) {
    IITER_SET_EOF(&nc->base);
    return INDEXREAD_EOF;
  } else if (wcii_rc == INDEXREAD_NOTFOUND) {
    // This doc-id was deleted
    return INDEXREAD_NOTFOUND;
  }
  RS_LOG_ASSERT_FMT(base->LastDocId == docId, "Expected docId to be %zu, got %zu", docId, base->LastDocId);
  return INDEXREAD_OK;
}

static size_t NI_NumEstimated(IndexIterator *base) {
  NotContext *nc = (NotContext *)base;
  return nc->maxDocId - nc->child->NumEstimated(nc->child);
}

/* Read from a NOT iterator - Non-Optimized version. This is applicable only if
 * the only or leftmost node of a query is a NOT node. We simply read until max
 * docId, skipping docIds that exist in the child */
static int NI_ReadSorted_NO(IndexIterator *base, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;
  if (base->LastDocId >= nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  // if we have a child, get the latest result from the child
  RSIndexResult *cr = IITER_CURRENT_RECORD(nc->child);

  if (cr == NULL || cr->docId == 0) {
    nc->child->Read(nc->child, &cr);
  }

  // advance our reader by one, and let's test if it's a valid value or not
  nc->base.current->docId++;

  // If we don't have a child result, or the child result is ahead of the current counter,
  // we just increment our virtual result's id until we hit the child result's
  // in which case we'll read from the child and bypass it by one.
  if (cr == NULL || cr->docId > nc->base.current->docId || !IITER_HAS_NEXT(nc->child)) {
    goto ok;
  }

  while (cr->docId == nc->base.current->docId) {
    // advance our docId to the next possible id
    nc->base.current->docId++;

    // read the next entry from the child
    if (nc->child->Read(nc->child, &cr) == INDEXREAD_EOF) {
      break;
    }

    // Check for timeout with low granularity (MOD-5512)
    if (TimedOut_WithCtx_Gran(&nc->timeoutCtx, 5000)) {
      IITER_SET_EOF(&nc->base);
      return INDEXREAD_TIMEOUT;
    }
  }
  nc->timeoutCtx.counter = 0;

ok:
  // make sure we did not overflow
  if (nc->base.current->docId > nc->maxDocId) {
    IITER_SET_EOF(&nc->base);
    return INDEXREAD_EOF;
  }

  // Set the next entry and return ok
  base->LastDocId = nc->base.current->docId;
  if (hit) *hit = nc->base.current;

  return INDEXREAD_OK;
}

/* Read from a NOT iterator - Optimized version, utilizing the `existing docs`
 * inverted index. This is applicable only if the only or leftmost node of a
 * query is a NOT node. We simply read until max docId, skipping docIds that
 * exist in the child */
static int NI_ReadSorted_O(IndexIterator *base, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;
  RSIndexResult *cr = NULL;
  int wcii_rc;
  int child_rc = INDEXREAD_OK;

  if (base->LastDocId > nc->maxDocId) {
    IITER_SET_EOF(&nc->base);
    return INDEXREAD_EOF;
  }

  // if we have a child, get the latest result from the child
  cr = IITER_CURRENT_RECORD(nc->child);

  if (cr == NULL || cr->docId == 0) {
    nc->child->Read(nc->child, &cr);
  }

  // Advance the embedded wildcard iterator
  RSIndexResult *wcii_res = NULL;
  wcii_rc = nc->wcii->Read(nc->wcii, &wcii_res);

  if (wcii_rc == INDEXREAD_EOF) {
    // If the wildcard iterator hit EOF, we're done
    IITER_SET_EOF(&nc->base);
    return INDEXREAD_EOF;
  }
  nc->base.current->docId = wcii_res->docId;

  // If there is no child result, or the child result is ahead of the wildcard
  // iterator result, we wish to return the current docId.
  if (cr == NULL || cr->docId > wcii_res->docId || !IITER_HAS_NEXT(nc->child)) {
    goto ok;
  }

  while (cr->docId == wcii_res->docId && child_rc != INDEXREAD_EOF) {
    wcii_rc = nc->wcii->Read(nc->wcii, &wcii_res);
    nc->base.current->docId = wcii_res->docId;

    if (wcii_rc == INDEXREAD_EOF) {
      // No more valid docs --> Done.
      IITER_SET_EOF(&nc->base);
      return INDEXREAD_EOF;
    }

    // read next entry from child
    // If the child docId is smaller than the wildcard docId, it was cleaned from
    // the `existingDocs` inverted index but not yet from child -> skip it.
    do {
      child_rc = nc->child->Read(nc->child, &cr);
    } while (child_rc != INDEXREAD_EOF && cr->docId < wcii_res->docId);

    // Check for timeout
    if (TimedOut_WithCtx_Gran(&nc->timeoutCtx, 5000)) {
      IITER_SET_EOF(nc->wcii);
      IITER_SET_EOF(&nc->base);
      return INDEXREAD_TIMEOUT;
    }
  }
  nc->timeoutCtx.counter = 0;

ok:
  // Set the next entry and return ok
  base->LastDocId = nc->base.current->docId;
  if (hit) *hit = nc->base.current;

  return INDEXREAD_OK;
}

/* We always have next, in case anyone asks... ;) */
static int NI_HasNext(IndexIterator *base) {
  NotContext *nc = (NotContext *)base;
  return base->LastDocId < nc->maxDocId;
}

IndexIterator *NewNotIterator(IndexIterator *it, t_docId maxDocId,
  double weight, struct timespec timeout, QueryEvalCtx *q) {

  NotContext *nc = rm_calloc(1, sizeof(*nc));
  bool optimized = q && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    nc->wcii = NewWildcardIterator(q);
  }
  nc->base.current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  nc->base.current->docId = 0;
  nc->base.isValid = 1;
  IndexIterator *ret = &nc->base;

  nc->child = it ? it : NewEmptyIterator();
  nc->maxDocId = maxDocId;          // Valid for the optimized case as well, since this is the maxDocId of the embedded wildcard iterator
  nc->weight = weight;
  nc->timeoutCtx = (TimeoutCtx){ .timeout = timeout, .counter = 0 };

  ret->type = NOT_ITERATOR;
  ret->LastDocId = 0;
  ret->NumEstimated = NI_NumEstimated;
  ret->Free = NI_Free;
  ret->HasNext = NI_HasNext;
  ret->Read = optimized ? NI_ReadSorted_O : NI_ReadSorted_NO;
  ret->SkipTo = optimized ? NI_SkipTo_O : NI_SkipTo_NO;
  ret->Abort = NI_Abort;
  ret->Rewind = NI_Rewind;

  return ret;
}

/**********************************************************
 * Optional clause iterator
 **********************************************************/

typedef struct {
  IndexIterator base;     // base index iterator
  IndexIterator *wcii;    // wildcard index iterator
  IndexIterator *child;   // child index iterator
  RSIndexResult *virt;
  t_docId maxDocId;
  double weight;
} OptionalIterator;

static void OI_Abort(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (nc->wcii) {
    nc->wcii->Abort(nc->wcii);
  }
  if (nc->child) {
    nc->child->Abort(nc->child);
  }
}

static void OI_Rewind(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  base->LastDocId = 0;
  if (nc->wcii) {
    nc->wcii->Rewind(nc->wcii);
  }
  nc->virt->docId = 0;
  if (nc->child) {
    nc->child->Rewind(nc->child);
  }
}

static void OI_Free(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  if (nc->wcii) {
    nc->wcii->Free(nc->wcii);
  }
  IndexResult_Free(nc->virt);
  rm_free(base);
}

// SkipTo for OPTIONAL iterator - Non-optimized version.
static int OI_SkipTo_NO(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  OptionalIterator *nc = (OptionalIterator *)base;

  if (docId > nc->maxDocId || !IITER_HAS_NEXT(base)) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  bool found = false;

  // Set the current ID
  base->LastDocId = docId;

  if (docId == nc->child->LastDocId) {
    // Edge case -- match on the docid we just looked for
    found = true;
    // reset current pointer since this might have been a prior
    // virt return
    nc->base.current = nc->child->current;

  } else if (docId > nc->child->LastDocId) {
    int rc = nc->child->SkipTo(nc->child, docId, &nc->base.current);
    if (rc == INDEXREAD_OK) {
      found = true;
    }
  }

  if (found) {
    // Has a real hit on the child iterator
    nc->base.current->weight = nc->weight;
  } else {
    nc->virt->docId = docId;
    nc->virt->weight = 0;
    nc->base.current = nc->virt;
  }

  *hit = nc->base.current;
  return INDEXREAD_OK;
}

// SkipTo for OPTIONAL iterator - Optimized version.
static int OI_SkipTo_O(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  OptionalIterator *nc = (OptionalIterator *)base;
  RSIndexResult *res;

  if (docId > nc->maxDocId || !IITER_HAS_NEXT(base)) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  if (docId > nc->child->LastDocId) {
    int rc = nc->child->SkipTo(nc->child, docId, &res);
    if (rc == INDEXREAD_TIMEOUT) return rc;
  }

  // Promote the wildcard iterator to the requested docId if the docId
  if (docId > nc->wcii->LastDocId) {
    int rc = nc->wcii->SkipTo(nc->wcii, docId, &res);
    if (rc == INDEXREAD_EOF) IITER_SET_EOF(base);
  }

  if (docId == nc->child->LastDocId) {
    // Has a real hit on the child iterator
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
    nc->base.LastDocId = docId;
  } else {
    nc->virt->docId = nc->wcii->LastDocId;
    nc->virt->weight = 0;
    nc->base.current = nc->virt;
    nc->base.LastDocId = nc->wcii->LastDocId;
  }

  *hit = nc->base.current;
  return INDEXREAD_OK;
}

static size_t OI_NumEstimated(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  return nc->maxDocId;
}

// Read from an OPTIONAL iterator - Non-Optimized version.
static int OI_ReadSorted_NO(IndexIterator *base, RSIndexResult **hit) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (base->LastDocId >= nc->maxDocId) {
    return INDEXREAD_EOF;
  }

  // Increase the size by one
  base->LastDocId++;

  if (base->LastDocId > nc->child->LastDocId) {
    int rc = nc->child->Read(nc->child, &nc->base.current);
    if (rc == INDEXREAD_TIMEOUT) {
      return rc;
    }
  }

  if (base->LastDocId != nc->child->LastDocId) {
    nc->base.current = nc->virt;
    nc->base.current->weight = 0;
  } else {
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
  }

  nc->base.current->docId = base->LastDocId;
  *hit = nc->base.current;
  return INDEXREAD_OK;
}

// Read from optional iterator - Optimized version, utilizing the `existing docs`
// inverted index.
static int OI_ReadSorted_O(IndexIterator *base, RSIndexResult **hit) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (base->LastDocId >= nc->maxDocId) {
    return INDEXREAD_EOF;
  }

  // Get the next docId
  RSIndexResult *wcii_res = NULL;
  int wcii_rc = nc->wcii->Read(nc->wcii, &wcii_res);
  if (wcii_rc != INDEXREAD_OK) {
    // EOF, set invalid
    IITER_SET_EOF(&nc->base);
    return wcii_rc;
  }

  int rc;
  // We loop over this condition, since it reflects that the index is not up to date.
  while (wcii_res->docId > nc->child->LastDocId) {
    rc = nc->child->Read(nc->child, &nc->base.current);
    if (rc == INDEXREAD_TIMEOUT) {
      return rc;
    }
  }

  base->LastDocId = nc->base.current->docId = wcii_res->docId;

  if (base->LastDocId != nc->child->LastDocId) {
    nc->base.current = nc->virt;
    nc->base.current->weight = 0;
  } else {
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
  }

  *hit = nc->base.current;
  return INDEXREAD_OK;
}

/* We always have next, in case anyone asks... ;) */
static int OI_HasNext(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  return (base->LastDocId < nc->maxDocId);
}

IndexIterator *NewOptionalIterator(IndexIterator *it, QueryEvalCtx *q, double weight) {
  OptionalIterator *nc = rm_calloc(1, sizeof(*nc));

  bool optimized = q && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    nc->wcii = NewWildcardIterator(q);
  }
  nc->virt = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  nc->virt->freq = 1;
  nc->base.current = nc->virt;
  nc->child = it ? it : NewEmptyIterator();
  nc->maxDocId = q->docTable->maxDocId;
  nc->weight = weight;

  IndexIterator *ret = &nc->base;
  ret->type = OPTIONAL_ITERATOR;
  ret->LastDocId = 0;
  ret->NumEstimated = OI_NumEstimated;
  ret->Free = OI_Free;
  ret->HasNext = OI_HasNext;
  ret->Read = optimized ? OI_ReadSorted_O : OI_ReadSorted_NO;
  ret->SkipTo = optimized ? OI_SkipTo_O : OI_SkipTo_NO;
  ret->Abort = OI_Abort;
  ret->Rewind = OI_Rewind;

  return ret;
}

/* Wildcard iterator, matching all documents in the database. */
typedef struct {
  IndexIterator base;
  t_docId topId;
  t_docId numDocs;
} WildcardIterator;

/* Free a wildcard iterator */
static void WI_Free(IndexIterator *it) {
  IndexResult_Free(it->current);
  rm_free(it);
}

/* Read reads the next consecutive id, unless we're at the end */
static int WI_Read(IndexIterator *base, RSIndexResult **hit) {
  WildcardIterator *wi = (WildcardIterator *)base;
  base->current->docId = ++base->LastDocId;
  if (base->LastDocId > wi->topId) {
    return INDEXREAD_EOF;
  }
  if (hit) {
    *hit = CURRENT_RECORD(wi);
  }
  return INDEXREAD_OK;
}

/* Skipto for wildcard iterator - always succeeds, but this should normally not happen as it has
 * no
 * meaning */
static int WI_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  WildcardIterator *wi = (WildcardIterator *)base;

  if (base->LastDocId > wi->topId) return INDEXREAD_EOF;

  base->LastDocId = docId;
  CURRENT_RECORD(wi)->docId = docId;
  if (hit) {
    *hit = CURRENT_RECORD(wi);
  }
  return INDEXREAD_OK;
}

static void WI_Abort(IndexIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;
  base->LastDocId = wi->topId + 1;
  IITER_SET_EOF(base);
}

/* We always have next, in case anyone asks... ;) */
static int WI_HasNext(IndexIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;

  return base->LastDocId < wi->topId;
}

static void WI_Rewind(IndexIterator *base) {
  IITER_CLEAR_EOF(base);
  base->LastDocId = 0;
}

static size_t WI_NumEstimated(IndexIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;
  return wi->numDocs;
}

/* Create a new wildcard iterator */
static IndexIterator *NewWildcardIterator_NonOptimized(t_docId maxId, size_t numDocs) {
  WildcardIterator *c = rm_calloc(1, sizeof(*c));
  c->topId = maxId;
  c->numDocs = numDocs;

  CURRENT_RECORD(c) = NewVirtualResult(1, RS_FIELDMASK_ALL);
  CURRENT_RECORD(c)->freq = 1;

  IndexIterator *ret = &c->base;
  ret->type = WILDCARD_ITERATOR;
  ret->LastDocId = 0;
  ret->Free = WI_Free;
  ret->HasNext = WI_HasNext;
  ret->Read = WI_Read;
  ret->SkipTo = WI_SkipTo;
  ret->Abort = WI_Abort;
  ret->Rewind = WI_Rewind;
  ret->NumEstimated = WI_NumEstimated;
  return ret;
}

// Returns a new wildcard iterator.
IndexIterator *NewWildcardIterator(QueryEvalCtx *q) {
  IndexIterator *ret;
  if (q->sctx->spec->rule->index_all == true) {
    if (q->sctx->spec->existingDocs) {
      IndexReader *ir = NewGenericIndexReader(q->sctx->spec->existingDocs,
        q->sctx, 1, 1, RS_INVALID_FIELD_INDEX, FIELD_EXPIRATION_DEFAULT);
      ret = NewReadIterator(ir);
      ret->type = WILDCARD_ITERATOR;
    } else {
      ret = NewEmptyIterator();
    }
    return ret;
  }

  // Non-optimized wildcard iterator, using a simple doc-id increment as its base.
  return NewWildcardIterator_NonOptimized(q->docTable->maxDocId, q->docTable->size);
}

static int EOI_Read(IndexIterator *p, RSIndexResult **e) {
  return INDEXREAD_EOF;
}
static void EOI_Free(IndexIterator *self) {
  // Nothing
}
static size_t EOI_NumEstimated(IndexIterator *self) {
  return 0;
}
static int EOI_SkipTo(IndexIterator *self, t_docId docId, RSIndexResult **hit) {
  return INDEXREAD_EOF;
}
static void EOI_Abort(IndexIterator *self) {
}
static void EOI_Rewind(IndexIterator *self) {
}

static IndexIterator eofIterator = {.Read = EOI_Read,
                                    .Free = EOI_Free,
                                    .SkipTo = EOI_SkipTo,
                                    .LastDocId = 0,
                                    .NumEstimated = EOI_NumEstimated,
                                    .Abort = EOI_Abort,
                                    .Rewind = EOI_Rewind,
                                    .type = EMPTY_ITERATOR};

IndexIterator *NewEmptyIterator(void) {
  return &eofIterator;
}

/**********************************************************
 * Profile printing functions
 **********************************************************/

static int PI_Read(IndexIterator *base, RSIndexResult **e) {
  ProfileIterator *pi = (ProfileIterator *)base;
  pi->counter++;
  clock_t begin = clock();
  int ret = pi->child->Read(pi->child, e);
  if (ret == INDEXREAD_EOF) pi->eof = 1;
  pi->base.current = pi->child->current;
  pi->base.LastDocId = pi->child->LastDocId;
  pi->cpuTime += clock() - begin;
  return ret;
}

static int PI_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  ProfileIterator *pi = (ProfileIterator *)base;
  pi->counter++;
  clock_t begin = clock();
  int ret = pi->child->SkipTo(pi->child, docId, hit);
  if (ret == INDEXREAD_EOF) pi->eof = 1;
  pi->base.current = pi->child->current;
  pi->base.LastDocId = pi->child->LastDocId;
  pi->cpuTime += clock() - begin;
  return ret;
}

static void PI_Free(IndexIterator *it) {
  ProfileIterator *pi = (ProfileIterator *)it;
  pi->child->Free(pi->child);
  rm_free(it);
}

static void PI_Rewind(IndexIterator *it) {
  ProfileIterator *pi = (ProfileIterator *)it;
  pi->child->Rewind(pi->child);
  it->LastDocId = 0;
}

#define PROFILE_ITERATOR_FUNC_SIGN(func, rettype)     \
static rettype PI_##func(IndexIterator *base) {       \
  ProfileIterator *pi = (ProfileIterator *)base;      \
  return pi->child->func(pi->child);                  \
}

PROFILE_ITERATOR_FUNC_SIGN(Abort, void);
PROFILE_ITERATOR_FUNC_SIGN(NumEstimated, size_t);

static int PI_HasNext(IndexIterator *base) {
  ProfileIterator *pi = (ProfileIterator *)base;
  return IITER_HAS_NEXT(pi->child);
}

/* Create a new wildcard iterator */
IndexIterator *NewProfileIterator(IndexIterator *child) {
  ProfileIteratorCtx *pc = rm_calloc(1, sizeof(*pc));
  pc->child = child;
  pc->counter = 0;
  pc->cpuTime = 0;
  pc->eof = 0;

  IndexIterator *ret = &pc->base;
  ret->LastDocId = 0;
  ret->type = PROFILE_ITERATOR;
  ret->Free = PI_Free;
  ret->HasNext = PI_HasNext;
  ret->Read = PI_Read;
  ret->SkipTo = PI_SkipTo;
  ret->Abort = PI_Abort;
  ret->Rewind = PI_Rewind;
  ret->NumEstimated = PI_NumEstimated;
  return ret;
}

#define PRINT_PROFILE_FUNC(name) static void name(RedisModule_Reply *reply,   \
                                                  IndexIterator *root,        \
                                                  size_t counter,             \
                                                  double cpuTime,             \
                                                  int depth,                  \
                                                  int limited,                \
                                                  PrintProfileConfig *config)

PRINT_PROFILE_FUNC(printUnionIt) {
  UnionIterator *ui = (UnionIterator *)root;
  int printFull = !limited  || (ui->origType & QN_UNION);

  RedisModule_Reply_Map(reply);

  printProfileType("UNION");

  RedisModule_Reply_SimpleString(reply, "Query type");
  char *unionTypeStr;
  switch (ui->origType) {
  case QN_GEO : unionTypeStr = "GEO"; break;
  case QN_GEOMETRY : unionTypeStr = "GEOSHAPE"; break;
  case QN_TAG : unionTypeStr = "TAG"; break;
  case QN_UNION : unionTypeStr = "UNION"; break;
  case QN_FUZZY : unionTypeStr = "FUZZY"; break;
  case QN_PREFIX : unionTypeStr = "PREFIX"; break;
  case QN_NUMERIC : unionTypeStr = "NUMERIC"; break;
  case QN_LEXRANGE : unionTypeStr = "LEXRANGE"; break;
  case QN_WILDCARD_QUERY : unionTypeStr = "WILDCARD"; break;
  default:
    RS_LOG_ASSERT(0, "Invalid type for union");
    break;
  }
  if (!ui->qstr) {
    RedisModule_Reply_SimpleString(reply, unionTypeStr);
  } else {
    const char *qstr = ui->qstr;
    if (isUnsafeForSimpleString(qstr)) qstr = escapeSimpleString(qstr);
    RedisModule_Reply_SimpleStringf(reply, "%s - %s", unionTypeStr, qstr);
    if (qstr != ui->qstr) rm_free((char*)qstr);
  }

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounter(counter);

  RedisModule_Reply_SimpleString(reply, "Child iterators");
  if (printFull) {
    RedisModule_Reply_Array(reply);
      for (int i = 0; i < ui->norig; i++) {
        printIteratorProfile(reply, ui->origits[i], 0, 0, depth + 1, limited, config);
      }
    RedisModule_Reply_ArrayEnd(reply);
  } else {
    RedisModule_Reply_SimpleStringf(reply, "The number of iterators in the union is %d", ui->norig);
  }

  RedisModule_Reply_MapEnd(reply);
}

PRINT_PROFILE_FUNC(printIntersectIt) {
  IntersectIterator *ii = (IntersectIterator *)root;

  RedisModule_Reply_Map(reply);

  printProfileType("INTERSECT");

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounter(counter);

  RedisModule_ReplyKV_Array(reply, "Child iterators");
    for (int i = 0; i < ii->num; i++) {
      if (ii->its[i]) {
        printIteratorProfile(reply, ii->its[i], 0, 0, depth + 1, limited, config);
      } else {
        RedisModule_Reply_Null(reply);
      }
    }
  RedisModule_Reply_ArrayEnd(reply);

  RedisModule_Reply_MapEnd(reply);
}

PRINT_PROFILE_FUNC(printMetricIt) {
  MetricIterator *mi = (MetricIterator *)root;

  RedisModule_Reply_Map(reply);

  switch (mi->type) {
    case VECTOR_DISTANCE: {
      printProfileType("METRIC - VECTOR DISTANCE");
      break;
    }
    default: {
      RS_LOG_ASSERT(0, "Invalid type for metric");
      break;
    }
  }

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounter(counter);

  RedisModule_Reply_MapEnd(reply);
}

void PrintIteratorChildProfile(RedisModule_Reply *reply, IndexIterator *root, size_t counter, double cpuTime,
                  int depth, int limited, PrintProfileConfig *config, IndexIterator *child, const char *text) {
  size_t nlen = 0;
  RedisModule_Reply_Map(reply);
    printProfileType(text);
    if (config->printProfileClock) {
      printProfileTime(cpuTime);
    }
    printProfileCounter(counter);

    if (root->type == HYBRID_ITERATOR) {
      HybridIterator *hi = (HybridIterator *)root;
      if (hi->searchMode == VECSIM_HYBRID_BATCHES ||
          hi->searchMode == VECSIM_HYBRID_BATCHES_TO_ADHOC_BF) {
        printProfileNumBatches(hi);
      }
    }

    if (root->type == OPTIMUS_ITERATOR) {
      OptimizerIterator *oi = (OptimizerIterator *)root;
      printProfileOptimizationType(oi);
    }

    if (child) {
      RedisModule_Reply_SimpleString(reply, "Child iterator");
      printIteratorProfile(reply, child, 0, 0, depth + 1, limited, config);
    }
  RedisModule_Reply_MapEnd(reply);
}

#define PRINT_PROFILE_SINGLE_NO_CHILD(name, text)                                      \
  PRINT_PROFILE_FUNC(name) {                                                           \
    PrintIteratorChildProfile(reply, (root), counter, cpuTime, depth, limited, config, \
      NULL, (text));                                                                   \
  }

#define PRINT_PROFILE_SINGLE(name, IterType, text)                                     \
  PRINT_PROFILE_FUNC(name) {                                                           \
    PrintIteratorChildProfile(reply, (root), counter, cpuTime, depth, limited, config, \
      ((IterType *)(root))->child, (text));                                            \
  }

PRINT_PROFILE_SINGLE_NO_CHILD(printWildcardIt,          "WILDCARD");
PRINT_PROFILE_SINGLE_NO_CHILD(printIdListIt,            "ID-LIST");
PRINT_PROFILE_SINGLE_NO_CHILD(printEmptyIt,             "EMPTY");
PRINT_PROFILE_SINGLE(printNotIt, NotIterator,           "NOT");
PRINT_PROFILE_SINGLE(printOptionalIt, OptionalIterator, "OPTIONAL");
PRINT_PROFILE_SINGLE(printHybridIt, HybridIterator,     "VECTOR");
PRINT_PROFILE_SINGLE(printOptimusIt, OptimizerIterator, "OPTIMIZER");

PRINT_PROFILE_FUNC(printProfileIt) {
  ProfileIterator *pi = (ProfileIterator *)root;
  printIteratorProfile(reply, pi->child, pi->counter - pi->eof,
    (double)(pi->cpuTime / CLOCKS_PER_MILLISEC), depth, limited, config);
}

void printIteratorProfile(RedisModule_Reply *reply, IndexIterator *root, size_t counter,
                          double cpuTime, int depth, int limited, PrintProfileConfig *config) {
  if (root == NULL) return;

  // protect against limit of 7 reply layers
  if (depth == REDIS_ARRAY_LIMIT && !isFeatureSupported(NO_REPLY_DEPTH_LIMIT)) {
    RedisModule_Reply_Null(reply);
    return;
  }

  switch (root->type) {
    // Reader
    case READ_ITERATOR:       { printReadIt(reply, root, counter, cpuTime, config);                       break; }
    // Multi values
    case UNION_ITERATOR:      { printUnionIt(reply, root, counter, cpuTime, depth, limited, config);      break; }
    case INTERSECT_ITERATOR:  { printIntersectIt(reply, root, counter, cpuTime, depth, limited, config);  break; }
    // Single value
    case NOT_ITERATOR:        { printNotIt(reply, root, counter, cpuTime, depth, limited, config);        break; }
    case OPTIONAL_ITERATOR:   { printOptionalIt(reply, root, counter, cpuTime, depth, limited, config);   break; }
    case WILDCARD_ITERATOR:   { printWildcardIt(reply, root, counter, cpuTime, depth, limited, config);   break; }
    case EMPTY_ITERATOR:      { printEmptyIt(reply, root, counter, cpuTime, depth, limited, config);      break; }
    case ID_LIST_ITERATOR:    { printIdListIt(reply, root, counter, cpuTime, depth, limited, config);     break; }
    case PROFILE_ITERATOR:    { printProfileIt(reply, root, 0, 0, depth, limited, config);                break; }
    case HYBRID_ITERATOR:     { printHybridIt(reply, root, counter, cpuTime, depth, limited, config);     break; }
    case METRIC_ITERATOR:     { printMetricIt(reply, root, counter, cpuTime, depth, limited, config);     break; }
    case OPTIMUS_ITERATOR:    { printOptimusIt(reply, root, counter, cpuTime, depth, limited, config);    break; }
    case MAX_ITERATOR:        { RS_LOG_ASSERT(0, "nope");   break; }
  }
}

/** Add Profile iterator before any iterator in the tree */
void Profile_AddIters(IndexIterator **root) {
  UnionIterator *ui;
  IntersectIterator *ini;

  if (*root == NULL) return;

  // Add profile iterator before child iterators
  switch((*root)->type) {
    case NOT_ITERATOR:
      Profile_AddIters(&((NotIterator *)((*root)))->child);
      break;
    case OPTIONAL_ITERATOR:
      Profile_AddIters(&((OptionalIterator *)((*root)))->child);
      break;
    case HYBRID_ITERATOR:
      Profile_AddIters(&((HybridIterator *)((*root)))->child);
      break;
    case OPTIMUS_ITERATOR:
      Profile_AddIters(&((OptimizerIterator *)((*root)))->child);
      break;
    case UNION_ITERATOR:
      ui = (UnionIterator*)(*root);
      for (int i = 0; i < ui->norig; i++) {
        Profile_AddIters(&(ui->origits[i]));
      }
      UI_SyncIterList(ui);
      break;
    case INTERSECT_ITERATOR:
      ini = (IntersectIterator*)(*root);
      for (int i = 0; i < ini->num; i++) {
        Profile_AddIters(&(ini->its[i]));
      }
      break;
    case WILDCARD_ITERATOR:
    case READ_ITERATOR:
    case EMPTY_ITERATOR:
    case ID_LIST_ITERATOR:
    case METRIC_ITERATOR:
      break;
    case PROFILE_ITERATOR:
    case MAX_ITERATOR:
      RS_LOG_ASSERT(0, "Error");
  }

  // Create a profile iterator and update outparam pointer
  *root = NewProfileIterator(*root);
}
