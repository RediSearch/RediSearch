/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "union_iterator.h"

static int cmpLastDocId(const void *e1, const void *e2, const void *udata) {
  const QueryIterator *it1 = e1, *it2 = e2;
  return (int64_t)(it2->lastDocId - it1->lastDocId);
}

static void resetMinIdHeap(UnionIterator *ui) {
  heap_t *hp = ui->heap_min_id;
  heap_clear(hp);
  for (int i = 0; i < ui->num; i++) {
    heap_offerx(hp, ui->its[i]);
  }
}

static inline void UI_AddChild(UnionIterator *ui, QueryIterator *it) {
  AggregateResult_AddChild(ui->base.current, it->current);
}

static inline void UI_SyncIterList(UnionIterator *ui) {
  ui->num = ui->num_orig;
  memcpy(ui->its, ui->its_orig, sizeof(*ui->its) * ui->num_orig);
  if (ui->heap_min_id) {
    resetMinIdHeap(ui);
  }
}

/**
 * Removes the exhausted iterator from the active list, so that future
 * reads will no longer iterate over it
 */
static inline void UI_RemoveExhausted(UnionIterator *it, int idx) {
  // Quickly remove the iterator by swapping it with the last iterator.
  it->its[idx] = it->its[--it->num]; // Also decrement the number of iterators
}

static size_t UI_NumEstimated(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  return ui->num_results_estimated;
}

static void UI_Rewind(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  QITER_CLEAR_EOF(base);
  base->lastDocId = 0;

  UI_SyncIterList(ui);

  // rewind all child iterators
  for (size_t i = 0; i < ui->num; i++) {
    ui->its[i]->Rewind(ui->its[i]);
  }
}

static inline void UI_SetFullFlat(UnionIterator *ui) {
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->lastDocId == ui->base.lastDocId) {
      UI_AddChild(ui, cur);
    }
  }
}

static inline void UI_QuickSet(UnionIterator *ui, QueryIterator *match) {
  ui->base.lastDocId = match->lastDocId;
  UI_AddChild(ui, match);
}

static inline IteratorStatus UI_Skip_Full_Flat(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  t_docId minId = UINT64_MAX;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->lastDocId < nextId) {
      IteratorStatus rc = cur->SkipTo(cur, nextId);
      if (rc == ITERATOR_OK) {
        UI_AddChild(ui, cur);
      } else if (rc == ITERATOR_EOF) {
        UI_RemoveExhausted(ui, i);
        i--;
        continue;
      } else if (rc != ITERATOR_NOTFOUND) {
        return rc;
      }
    } else if (cur->lastDocId == nextId) {
      UI_AddChild(ui, cur);
    }
    // Look for the minimal lastDocId
    if (minId > cur->lastDocId) minId = cur->lastDocId;
  }

  if (minId == nextId) {
    // We found what we were looking for
    ui->base.lastDocId = minId;
    // Current record was already set while scanning the children
    return ITERATOR_OK;
  } else if (ui->num) {
    // We didn't find the requested ID, but we know the next minimal ID
    base->lastDocId = minId;
    UI_SetFullFlat(ui);
    return ITERATOR_NOTFOUND;
  } else {
    QITER_SET_EOF(base);
    return ITERATOR_EOF;
  }
}

static inline IteratorStatus UI_Read_Full_Flat(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  const t_docId lastId = ui->base.lastDocId;
  t_docId minId = UINT64_MAX;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->lastDocId == lastId) {
      IteratorStatus rc = cur->Read(cur);
      if (rc == ITERATOR_EOF) {
        UI_RemoveExhausted(ui, i);
        i--;
        continue;
      } else if (rc != ITERATOR_OK) {
        return rc;
      }
    }
    if (minId > cur->lastDocId) minId = cur->lastDocId;
  }
  if (ui->num) {
    base->lastDocId = minId;
    UI_SetFullFlat(ui);
    return ITERATOR_OK;
  } else {
    QITER_SET_EOF(base);
    return ITERATOR_EOF;
  }
}

static inline IteratorStatus UI_Skip_Quick_Flat(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  t_docId minId = UINT64_MAX;
  QueryIterator *minIt;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->lastDocId < nextId) {
      IteratorStatus rc = cur->SkipTo(cur, nextId);
      if (rc == ITERATOR_OK) {
        UI_QuickSet(ui, cur);
        return ITERATOR_OK;
      } else if (rc == ITERATOR_EOF) {
        UI_RemoveExhausted(ui, i);
        i--;
        continue;
      } else if (rc != ITERATOR_NOTFOUND) {
        return rc;
      }
    } else if (cur->lastDocId == nextId) {
      UI_QuickSet(ui, cur);
      return ITERATOR_OK;
    }
    // Look for the minimal lastDocId + its iterator
    if (minId > cur->lastDocId) {
      minId = cur->lastDocId;
      minIt = cur;
    }
  }

  if (ui->num) {
    // We didn't find the requested ID, but we set ui->base.lastDocId to the next minimal ID,
    // And `minIt` is set to an iterator holding this ID
    UI_QuickSet(ui, minIt);
    return ITERATOR_NOTFOUND;
  } else {
    QITER_SET_EOF(base);
    return ITERATOR_EOF;
  }
}

static inline IteratorStatus UI_Read_Quick_Flat(QueryIterator *base) {
  IteratorStatus rc = UI_Skip_Quick_Flat(base, base->lastDocId + 1);
  return rc == ITERATOR_NOTFOUND ? ITERATOR_OK : rc;
}

static inline IteratorStatus UI_Skip_Full_Heap(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  QueryIterator *cur;
  heap_t *hp = ui->heap_min_id;
  AggregateResult_Reset(ui->base.current);
  while ((cur = heap_peek(hp)) && cur->lastDocId < nextId) {
    IteratorStatus rc = cur->SkipTo(cur, nextId);
    if (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == ITERATOR_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    ui->base.lastDocId = cur->lastDocId;
    heap_cb_root(hp, (HeapCallback)UI_AddChild, ui);
    return nextId == cur->lastDocId ? ITERATOR_OK : ITERATOR_NOTFOUND;
  }
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}

static inline IteratorStatus UI_Read_Full_Heap(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  QueryIterator *cur;
  heap_t *hp = ui->heap_min_id;
  AggregateResult_Reset(ui->base.current);
  while ((cur = heap_peek(hp)) && cur->lastDocId == base->lastDocId) {
    IteratorStatus rc = cur->Read(cur);
    if (rc == ITERATOR_OK) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == ITERATOR_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    ui->base.lastDocId = cur->lastDocId;
    heap_cb_root(hp, (HeapCallback)UI_AddChild, ui);
    return ITERATOR_OK;
  }
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}

static inline IteratorStatus UI_Skip_Quick_Heap(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  QueryIterator *cur;
  heap_t *hp = ui->heap_min_id;
  AggregateResult_Reset(ui->base.current);
  while ((cur = heap_peek(hp)) && cur->lastDocId < nextId) {
    IteratorStatus rc = cur->SkipTo(cur, nextId);
    if (rc == ITERATOR_OK) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
      UI_QuickSet(ui, cur);
      return ITERATOR_OK;
    } else if (rc == ITERATOR_NOTFOUND) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == ITERATOR_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    UI_QuickSet(ui, cur);
    return nextId == cur->lastDocId ? ITERATOR_OK : ITERATOR_NOTFOUND;
  }
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}

static inline IteratorStatus UI_Read_Quick_Heap(QueryIterator *base) {
  IteratorStatus rc = UI_Skip_Quick_Heap(base, base->lastDocId + 1);
  return rc == ITERATOR_NOTFOUND ? ITERATOR_OK : rc;
}

static void UI_Free(QueryIterator *base) {
  if (base == NULL) return;

  UnionIterator *ui = (UnionIterator *)base;
  for (int i = 0; i < ui->num_orig; i++) {
    QueryIterator *it = ui->its_orig[i];
    if (it) {
      it->Free(it);
    }
  }

  IndexResult_Free(ui->base.current);
  if (ui->heap_min_id) heap_free(ui->heap_min_id);
  rm_free(ui->its);
  rm_free(ui->its_orig);
  rm_free(ui);
}

QueryIterator *IT_V2(NewUnionIterator)(QueryIterator **its, int num, bool quickExit,
                                double weight, QueryNodeType type, const char *q_str, IteratorsConfig *config) {
  // create union context
  UnionIterator *ctx = rm_calloc(1, sizeof(UnionIterator));
  ctx->its_orig = its;
  ctx->type = type;
  ctx->num_orig = num;
  ctx->its = rm_malloc(num * sizeof(*ctx->its));
  ctx->heap_min_id = NULL;
  ctx->q_str = q_str;

  // bind the union iterator calls
  QueryIterator *it = &ctx->base;
  it->type = UNION_ITERATOR;
  QITER_CLEAR_EOF(it);
  it->lastDocId = 0;
  it->current = NewUnionResult(num, weight);
  it->NumEstimated = UI_NumEstimated;
  it->Free = UI_Free;
  it->Rewind = UI_Rewind;

  ctx->num_results_estimated = 0;
  for (size_t i = 0; i < num; ++i) {
    ctx->num_results_estimated += its[i]->NumEstimated(its[i]);
  }

  if (num > config->minUnionIterHeap) {
    it->Read = quickExit ? UI_Read_Quick_Heap : UI_Read_Full_Heap;
    it->SkipTo = quickExit ? UI_Skip_Quick_Heap : UI_Skip_Full_Heap;
    ctx->heap_min_id = rm_malloc(heap_sizeof(num));
    heap_init(ctx->heap_min_id, cmpLastDocId, NULL, num);
  } else {
    it->Read = quickExit ? UI_Read_Quick_Flat : UI_Read_Full_Flat;
    it->SkipTo = quickExit ? UI_Skip_Quick_Flat : UI_Skip_Full_Flat;
  }

  UI_SyncIterList(ctx);
  return it;
}
