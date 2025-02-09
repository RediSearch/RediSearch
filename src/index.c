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

static inline int UI_ReadUnsorted(IndexIterator *base, RSIndexResult **hit);
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
} ProfileIterator, ProfileIteratorCtx;

typedef struct {
  IndexIterator base;
  heap_t *heapMinId;
  size_t nexpected;
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
  AggregateResult_AddChild(CURRENT_RECORD(ui), it->current);
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
  // Quickly remove the iterator by swapping it with the last iterator.
  it->its[badix] = it->its[--it->num]; // Also decrement the number of iterators
  // Repeat the same index again, because we have a new iterator at the same position
  return badix - 1;
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

static inline int UI_SkipAdvanceLagging_Flat(UnionIterator *ui, const t_docId nextId) {
  RSIndexResult *h;
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
    // Look for the minimal LastDocId
    if (ui->base.LastDocId > cur->LastDocId) ui->base.LastDocId = cur->LastDocId;
  }
  return ui->num ? ui->base.LastDocId == nextId ? INDEXREAD_OK : INDEXREAD_NOTFOUND : INDEXREAD_EOF;
}

static inline int UI_SkipAdvanceLagging_Heap(UnionIterator *ui, const t_docId nextId) {
  RSIndexResult *h;
  IndexIterator *cur;
  heap_t *hp = ui->heapMinId;
  while ((cur = heap_peek(hp)) && cur->LastDocId < nextId) {
    int rc = cur->SkipTo(cur, nextId, &h);
    if (rc == INDEXREAD_OK || rc == INDEXREAD_NOTFOUND) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == INDEXREAD_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    ui->base.LastDocId = cur->LastDocId;
    return nextId == cur->LastDocId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
  }
  return INDEXREAD_EOF;
}

static inline int UI_ReadAdvanceLagging_Flat(UnionIterator *ui) {
  RSIndexResult *h;
  const t_docId lastId = ui->base.LastDocId;
  ui->base.LastDocId = UINT64_MAX;
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId == lastId) {
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
  return ui->num ? INDEXREAD_OK : INDEXREAD_EOF;
}

static inline int UI_ReadAdvanceLagging_Heap(UnionIterator *ui) {
  RSIndexResult *h;
  IndexIterator *cur;
  heap_t *hp = ui->heapMinId;
  while ((cur = heap_peek(hp)) && cur->LastDocId == ui->base.LastDocId) {
    int rc = cur->Read(cur, &h);
    if (rc == INDEXREAD_OK) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == INDEXREAD_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    ui->base.LastDocId = cur->LastDocId;
    return INDEXREAD_OK;
  }
  return INDEXREAD_EOF;
}

static inline void UI_SetFullFlat(UnionIterator *ui) {
  AggregateResult_Reset(CURRENT_RECORD(ui));
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId == ui->base.LastDocId) {
      AggregateResult_AddChild(CURRENT_RECORD(ui), cur->current);
    }
  }
}

static inline void UI_SetQuickFlat(UnionIterator *ui) {
  AggregateResult_Reset(CURRENT_RECORD(ui));
  for (int i = 0; i < ui->num; i++) {
    IndexIterator *cur = ui->its[i];
    if (cur->LastDocId == ui->base.LastDocId) {
      AggregateResult_AddChild(CURRENT_RECORD(ui), cur->current);
      return;
    }
  }
}

static inline void UI_SetFullHeap(UnionIterator *ui) {
  AggregateResult_Reset(CURRENT_RECORD(ui));
  heap_cb_root(ui->heapMinId, (HeapCallback)UI_HeapAddChildren, ui);
}

static inline void UI_SetQuickHeap(UnionIterator *ui) {
  AggregateResult_Reset(CURRENT_RECORD(ui));
  IndexIterator *cur = heap_peek(ui->heapMinId);
  AggregateResult_AddChild(CURRENT_RECORD(ui), cur->current);
}

/**
 * Generic function for union reading, given a mode and an algorithm
 * @param mode the mode of the iterator, should be one of [Full, Quick]
 * @param algo the algorithm to use, should be one of [Flat, Heap]
 */
#define UI_READ_GENERATOR(mode, algo)                                      \
static int UI_Read##mode##algo(IndexIterator *base, RSIndexResult **hit) { \
  UnionIterator *ui = (UnionIterator *)base;                               \
  if (!IITER_HAS_NEXT(base)) {                                             \
    return INDEXREAD_EOF;                                                  \
  }                                                                        \
  int rc = UI_ReadAdvanceLagging_##algo(ui);                               \
  if (rc == INDEXREAD_OK) {                                                \
    UI_Set##mode##algo(ui);                                                \
    if (hit) *hit = CURRENT_RECORD(ui);                                    \
  } else if (rc == INDEXREAD_EOF) {                                        \
    IITER_SET_EOF(base);                                                   \
  }                                                                        \
  return rc;                                                               \
}

UI_READ_GENERATOR(Full, Flat)
UI_READ_GENERATOR(Full, Heap)
UI_READ_GENERATOR(Quick, Flat)
UI_READ_GENERATOR(Quick, Heap)

/**
 * Generic function for union skipping, given a mode and an algorithm
 * @param mode the mode of the iterator, should be one of [Full, Quick]
 * @param algo the algorithm to use, should be one of [Flat, Heap]
 */
#define UI_SKIP_GENERATOR(mode, algo)                                                       \
static int UI_SkipTo##mode##algo(IndexIterator *base, t_docId docId, RSIndexResult **hit) { \
  UnionIterator *ui = (UnionIterator *)base;                                                \
  if (!IITER_HAS_NEXT(base)) {                                                              \
    return INDEXREAD_EOF;                                                                   \
  }                                                                                         \
  /* advance lagging iterators to `docId` or above */                                       \
  int rc = UI_SkipAdvanceLagging_##algo(ui, docId);                                         \
  if (rc == INDEXREAD_OK || rc == INDEXREAD_NOTFOUND) {                                     \
    UI_Set##mode##algo(ui);                                                                 \
    if (hit) *hit = CURRENT_RECORD(ui);                                                     \
  } else if (rc == INDEXREAD_EOF) {                                                         \
    IITER_SET_EOF(base);                                                                    \
  }                                                                                         \
  return rc;                                                                                \
}

UI_SKIP_GENERATOR(Full, Flat)
UI_SKIP_GENERATOR(Full, Heap)
UI_SKIP_GENERATOR(Quick, Flat)
UI_SKIP_GENERATOR(Quick, Heap)

IndexIterator *NewUnionIterator(IndexIterator **its, int num, bool quickExit,
                                double weight, QueryNodeType type, const char *qstr, IteratorsConfig *config) {
  // create union context
  UnionIterator *ctx = rm_calloc(1, sizeof(UnionIterator));
  ctx->origits = its;
  ctx->origType = type;
  ctx->num = num;
  ctx->norig = num;
  CURRENT_RECORD(ctx) = NewUnionResult(num, weight);
  ctx->its = rm_calloc(ctx->num, sizeof(*ctx->its));
  ctx->nexpected = 0;
  ctx->currIt = 0;
  ctx->heapMinId = NULL;
  ctx->qstr = qstr;

  // bind the union iterator calls
  IndexIterator *it = &ctx->base;
  it->type = UNION_ITERATOR;
  IITER_CLEAR_EOF(it);
  it->isAborted = false;
  it->LastDocId = 0;
  it->NumEstimated = UI_NumEstimated;
  it->Free = UnionIterator_Free;
  it->Rewind = UI_Rewind;
  UI_SyncIterList(ctx);

  for (size_t i = 0; i < num; ++i) {
    ctx->nexpected += its[i]->NumEstimated(its[i]);
  }

  if (ctx->norig > config->minUnionIterHeap) {
    it->Read = quickExit ? UI_ReadQuickHeap : UI_ReadFullHeap;
    it->SkipTo = quickExit ? UI_SkipToQuickHeap : UI_SkipToFullHeap;
    ctx->heapMinId = rm_malloc(heap_sizeof(num));
    heap_init(ctx->heapMinId, cmpLastDocId, NULL, num);
    resetMinIdHeap(ctx);
  } else {
    it->Read = quickExit ? UI_ReadQuickFlat : UI_ReadFullFlat;
    it->SkipTo = quickExit ? UI_SkipToQuickFlat : UI_SkipToFullFlat;
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
  IITER_SET_EOF(base);
  return INDEXREAD_EOF;
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
  size_t nexpected;
} IntersectIterator;

void IntersectIterator_Free(IndexIterator *base) {
  if (base == NULL) return;
  IntersectIterator *ii = (IntersectIterator *)base;
  for (int i = 0; i < ii->num; i++) {
    if (ii->its[i] != NULL) {
      ii->its[i]->Free(ii->its[i]);
    }
  }

  rm_free(ii->its);
  IndexResult_Free(base->current);
  rm_free(base);
}

static void II_Rewind(IndexIterator *base) {
  IntersectIterator *ii = (IntersectIterator *)base;
  IITER_CLEAR_EOF(base);
  base->LastDocId = 0;

  // rewind all child iterators
  for (int i = 0; i < ii->num; i++) {
    ii->its[i]->Rewind(ii->its[i]);
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

    size_t amount = curit->NumEstimated(curit);
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

  ctx->base.current = NewIntersectResult(num, weight);
  ctx->its = its_;
  ctx->num = num;

  bool allValid = II_SetEstimation(ctx);

  // Sort children iterators from low count to high count which reduces the number of iterations.
  if (!ctx->inOrder && allValid) {
    qsort(ctx->its, ctx->num, sizeof(*ctx->its), (CompareFunc)cmpIter);
  }

  // bind the iterator calls
  IndexIterator *it = &ctx->base;
  it->type = INTERSECT_ITERATOR;
  it->isValid = true;
  it->isAborted = false;
  it->LastDocId = 0;
  it->NumEstimated = II_NumEstimated;
  it->Read = II_ReadSorted;
  it->SkipTo = II_SkipTo;
  it->Free = IntersectIterator_Free;
  it->Rewind = II_Rewind;
  if (!allValid) IndexIterator_Abort(it); // If any of the iterators is NULL, abort the iterator (always EOF)
  return it;
}

static int II_AgreeOnDocId(IntersectIterator *ic) {
  const t_docId docId = ic->base.LastDocId;
  for (int i = 0; i < ic->num; i++) {
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
  // If we need to match slop and order, we do it now, and possibly skip the result
  return ic->maxSlop < 0 || IndexResult_IsWithinRange(ic->base.current, ic->maxSlop, ic->inOrder);
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
      ic->base.LastDocId++; // advance the last docId to the next possible value
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
    // Agreed on docId, but not relevant - need to read the next valid result.
    ic->base.LastDocId++; // advance the last docId to the next possible value
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
  TimeoutCtx timeoutCtx;
} NotIterator, NotContext;

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

static size_t NI_NumEstimated(IndexIterator *base) {
  NotContext *nc = (NotContext *)base;
  return nc->maxDocId - nc->child->NumEstimated(nc->child);
}

static inline int NI_SetReturnOK(NotContext *nc, RSIndexResult **hit, t_docId docId) {
  nc->base.LastDocId = nc->base.current->docId = docId;
  if (hit) *hit = nc->base.current;
  return INDEXREAD_OK;
}

static int NI_ReadSorted_O(IndexIterator *base, RSIndexResult **hit); // forward decl
static int NI_ReadSorted_NO(IndexIterator *base, RSIndexResult **hit); // forward decl

/* SkipTo for NOT iterator - Non-optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static int NI_SkipTo_NO(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;

  // do not skip beyond max doc id
  if (docId > nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  RSIndexResult *cr;

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  // So we are okay!
  if (nc->child->LastDocId > docId || !IITER_HAS_NEXT(nc->child)) {
    return NI_SetReturnOK(nc, hit, docId);
  } else if (nc->child->LastDocId < docId) {
    // read the next entry from the child
    int rc = nc->child->SkipTo(nc->child, docId, &cr);
    if (rc == INDEXREAD_TIMEOUT) return INDEXREAD_TIMEOUT;
    if (rc != INDEXREAD_OK) {
      return NI_SetReturnOK(nc, hit, docId);
    }
  }
  // If the child docId is the one we are looking for, it's an anti match!
  // We need to return NOTFOUND and set hit to the next valid docId
  nc->base.current->docId = base->LastDocId = docId;
  int rc = NI_ReadSorted_NO(base, hit);
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }
  return rc;
}

/* SkipTo for NOT iterator - Optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
int NI_SkipTo_O(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;

  // do not skip beyond max doc id
  if (docId > nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  RSIndexResult *wr;
  int rc = nc->wcii->SkipTo(nc->wcii, docId, &wr);
  if (rc != INDEXREAD_NOTFOUND && rc != INDEXREAD_OK) {
    return rc;
  } else if (rc == INDEXREAD_OK) {
    // A valid wildcard result was found. Let's check if the child has it
    if (nc->child->LastDocId > docId || !IITER_HAS_NEXT(nc->child)) {
      // If the child is ahead of the skipto id, it means the child doesn't have this id.
      // So we are okay!
      return NI_SetReturnOK(nc, hit, docId);
    } else if (nc->child->LastDocId < docId) {
      // read the next entry from the child
      RSIndexResult *cr;
      rc = nc->child->SkipTo(nc->child, docId, &cr);
      if (rc == INDEXREAD_TIMEOUT) return INDEXREAD_TIMEOUT;
      if (rc != INDEXREAD_OK) {
        return NI_SetReturnOK(nc, hit, docId);
      }
    }
  }

  // If the wildcard iterator is missing the docId, or the child iterator has it,
  // We need to return NOTFOUND and set hit to the next valid docId
  rc = NI_ReadSorted_O(base, hit);
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }
  return rc;
}

/* Read from a NOT iterator - Non-Optimized version. This is applicable only if
 * the only or leftmost node of a query is a NOT node. We simply read until max
 * docId, skipping docIds that exist in the child */
static int NI_ReadSorted_NO(IndexIterator *base, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;
  if (!base->isValid || base->LastDocId >= nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  if (base->LastDocId == nc->child->LastDocId) {
    // read next entry from child, or EOF
    RSIndexResult *cr;
    int rc = nc->child->Read(nc->child, &cr);
    if (rc == INDEXREAD_TIMEOUT) return INDEXREAD_TIMEOUT;
  }

  while (base->LastDocId < nc->maxDocId) {
    base->LastDocId++;
    if (base->LastDocId < nc->child->LastDocId || !IITER_HAS_NEXT(nc->child)) {
      nc->timeoutCtx.counter = 0;
      return NI_SetReturnOK(nc, hit, base->LastDocId);
    }
    RSIndexResult *res;
    int rc = nc->child->Read(nc->child, &res);
    if (rc == INDEXREAD_TIMEOUT) return rc;
    // Check for timeout with low granularity (MOD-5512)
    if (TimedOut_WithCtx_Gran(&nc->timeoutCtx, 5000)) {
      IITER_SET_EOF(base);
      return INDEXREAD_TIMEOUT;
    }
  }
  IITER_SET_EOF(base);
  return INDEXREAD_EOF;
}

/* Read from a NOT iterator - Optimized version, utilizing the `existing docs`
 * inverted index. This is applicable only if the only or leftmost node of a
 * query is a NOT node. We simply read until max docId, skipping docIds that
 * exist in the child */
static int NI_ReadSorted_O(IndexIterator *base, RSIndexResult **hit) {
  NotContext *nc = (NotContext *)base;
  if (!base->isValid || base->LastDocId >= nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  if (base->LastDocId == nc->child->LastDocId) {
    // read next entry from child, or EOF
    RSIndexResult *cr;
    int rc = nc->child->Read(nc->child, &cr);
    if (rc == INDEXREAD_TIMEOUT) return INDEXREAD_TIMEOUT;
  }

  RSIndexResult *cr, *wr;
  int rc;
  while ((rc = nc->wcii->Read(nc->wcii, &wr)) == INDEXREAD_OK) {

    if (wr->docId < nc->child->LastDocId || !IITER_HAS_NEXT(nc->child)) {
      nc->timeoutCtx.counter = 0;
      return NI_SetReturnOK(nc, hit, wr->docId);
    }
    // read next entry from child
    // If the child docId is smaller than the wildcard docId, it was cleaned from
    // the `existingDocs` inverted index but not yet from child -> skip it.
    int child_rc;
    do {
      child_rc = nc->child->Read(nc->child, &cr);
      if (child_rc == INDEXREAD_TIMEOUT) return INDEXREAD_TIMEOUT;
    } while (child_rc != INDEXREAD_EOF && cr->docId < wr->docId);

    // Check for timeout
    if (TimedOut_WithCtx_Gran(&nc->timeoutCtx, 5000)) {
      IITER_SET_EOF(base);
      return INDEXREAD_TIMEOUT;
    }
  }
  IITER_SET_EOF(base);
  return rc;
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
  nc->timeoutCtx = (TimeoutCtx){ .timeout = timeout, .counter = 0 };

  ret->type = NOT_ITERATOR;
  IITER_CLEAR_EOF(ret);
  ret->isAborted = false;
  ret->LastDocId = 0;
  ret->NumEstimated = NI_NumEstimated;
  ret->Free = NI_Free;
  ret->Read = optimized ? NI_ReadSorted_O : NI_ReadSorted_NO;
  ret->SkipTo = optimized ? NI_SkipTo_O : NI_SkipTo_NO;
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

static void OI_Rewind(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  IITER_CLEAR_EOF(base);
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
  RSIndexResult *res;

  if (docId > nc->maxDocId || !IITER_HAS_NEXT(base)) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  if (docId > nc->child->LastDocId) {
    int rc = nc->child->SkipTo(nc->child, docId, &res);
    if (rc == INDEXREAD_TIMEOUT) return rc;
  }

  if (docId == nc->child->LastDocId) {
    // Has a real hit on the child iterator
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
  } else {
    nc->virt->docId = docId;
    nc->base.current = nc->virt;
  }
  // Set the current ID
  base->LastDocId = docId;
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
    int crc = nc->child->SkipTo(nc->child, docId, &res);
    if (crc == INDEXREAD_TIMEOUT) return crc;
  }

  // Promote the wildcard iterator to the requested docId if the docId
  int rc = INDEXREAD_OK;
  if (docId > nc->wcii->LastDocId) {
    rc = nc->wcii->SkipTo(nc->wcii, docId, &res);
    if (rc == INDEXREAD_EOF) IITER_SET_EOF(base);
  }

  if (docId == nc->child->LastDocId) {
    // Has a real hit on the child iterator
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
    nc->base.LastDocId = docId;
  } else {
    nc->virt->docId = nc->wcii->LastDocId;
    nc->base.current = nc->virt;
    nc->base.LastDocId = nc->wcii->LastDocId;
  }

  *hit = nc->base.current;
  return rc;
}

static size_t OI_NumEstimated(IndexIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  return nc->maxDocId;
}

// Read from an OPTIONAL iterator - Non-Optimized version.
static int OI_ReadSorted_NO(IndexIterator *base, RSIndexResult **hit) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (base->LastDocId >= nc->maxDocId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  // Increase the size by one
  base->LastDocId++;

  if (base->LastDocId > nc->child->LastDocId && IITER_HAS_NEXT(nc->child)) {
    int rc = nc->child->Read(nc->child, &nc->base.current);
    if (rc == INDEXREAD_TIMEOUT) return rc;
  }

  if (base->LastDocId != nc->child->LastDocId) {
    nc->base.current = nc->virt;
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
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  // Get the next docId
  RSIndexResult *wcii_res = NULL;
  int wcii_rc = nc->wcii->Read(nc->wcii, &wcii_res);
  if (wcii_rc != INDEXREAD_OK) {
    // EOF, set invalid
    IITER_SET_EOF(base);
    return wcii_rc;
  }

  // We loop over this condition, since it reflects that the index is not up to date.
  while (wcii_res->docId > nc->child->LastDocId && IITER_HAS_NEXT(nc->child)) {
    int rc = nc->child->Read(nc->child, &nc->base.current);
    if (rc == INDEXREAD_TIMEOUT) return rc;
  }

  if (wcii_res->docId != nc->child->LastDocId) {
    nc->base.current = nc->virt;
  } else {
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
  }

  base->LastDocId = nc->base.current->docId = wcii_res->docId;
  *hit = nc->base.current;
  return INDEXREAD_OK;
}

IndexIterator *NewOptionalIterator(IndexIterator *it, QueryEvalCtx *q, double weight) {
  OptionalIterator *nc = rm_calloc(1, sizeof(*nc));

  bool optimized = q && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    nc->wcii = NewWildcardIterator(q);
  }
  nc->virt = NewVirtualResult(0, RS_FIELDMASK_ALL);
  nc->virt->freq = 1;
  nc->base.current = nc->virt;
  nc->child = it ? it : NewEmptyIterator();
  nc->maxDocId = q->docTable->maxDocId;
  nc->weight = weight;

  IndexIterator *ret = &nc->base;
  ret->type = OPTIONAL_ITERATOR;
  IITER_CLEAR_EOF(ret);
  ret->isAborted = false;
  ret->LastDocId = 0;
  ret->NumEstimated = OI_NumEstimated;
  ret->Free = OI_Free;
  ret->Read = optimized ? OI_ReadSorted_O : OI_ReadSorted_NO;
  ret->SkipTo = optimized ? OI_SkipTo_O : OI_SkipTo_NO;
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
  if (base->LastDocId >= wi->topId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }
  base->current->docId = ++base->LastDocId;
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

  if (base->LastDocId >= wi->topId) {
    IITER_SET_EOF(base);
    return INDEXREAD_EOF;
  }

  base->LastDocId = docId;
  CURRENT_RECORD(wi)->docId = docId;
  if (hit) {
    *hit = CURRENT_RECORD(wi);
  }
  return INDEXREAD_OK;
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
  ret->isValid = true;
  ret->isAborted = false;
  ret->LastDocId = 0;
  ret->Free = WI_Free;
  ret->Read = WI_Read;
  ret->SkipTo = WI_SkipTo;
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

static IndexIterator eofIterator = {.type = EMPTY_ITERATOR,
                                    .isValid = false,
                                    .isAborted = false,
                                    .LastDocId = 0,
                                    .Read = EOI_Read,
                                    .Free = EOI_Free,
                                    .SkipTo = EOI_SkipTo,
                                    .NumEstimated = EOI_NumEstimated,
                                    .Rewind = EOI_Rewind,
};

IndexIterator *NewEmptyIterator(void) {
  return &eofIterator;
}

/**********************************************************
 * Profile printing functions
 **********************************************************/

static inline void PI_Align(ProfileIterator *pi) {
  pi->base.isValid = pi->child->isValid;
  pi->base.isAborted = pi->child->isAborted;
  pi->base.LastDocId = pi->child->LastDocId;
  pi->base.current = pi->child->current;
}

static int PI_Read(IndexIterator *base, RSIndexResult **e) {
  ProfileIterator *pi = (ProfileIterator *)base;
  pi->counter++;
  clock_t begin = clock();
  int ret = pi->child->Read(pi->child, e);
  PI_Align(pi);
  pi->cpuTime += clock() - begin;
  return ret;
}

static int PI_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  ProfileIterator *pi = (ProfileIterator *)base;
  pi->counter++;
  clock_t begin = clock();
  int ret = pi->child->SkipTo(pi->child, docId, hit);
  PI_Align(pi);
  pi->cpuTime += clock() - begin;
  return ret;
}

static void PI_Free(IndexIterator *it) {
  ProfileIterator *pi = (ProfileIterator *)it;
  pi->child->Free(pi->child);
  rm_free(it);
}

static size_t PI_NumEstimated(IndexIterator *base) {
  ProfileIterator *pi = (ProfileIterator *)base;
  return pi->child->NumEstimated(pi->child);
}

static void PI_Rewind(IndexIterator *base) {
  ProfileIterator *pi = (ProfileIterator *)base;
  pi->child->Rewind(pi->child);
  PI_Align(pi);
}

/* Create a new wildcard iterator */
IndexIterator *NewProfileIterator(IndexIterator *child) {
  ProfileIteratorCtx *pc = rm_calloc(1, sizeof(*pc));
  pc->child = child;
  pc->counter = 0;
  pc->cpuTime = 0;

  IndexIterator *ret = &pc->base;
  ret->type = PROFILE_ITERATOR;
  ret->isValid = true;
  ret->isAborted = false;
  ret->LastDocId = 0;
  ret->Free = PI_Free;
  ret->Read = PI_Read;
  ret->SkipTo = PI_SkipTo;
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
  printIteratorProfile(reply, pi->child, pi->counter - (pi->base.isValid ? 0 : 1),
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
