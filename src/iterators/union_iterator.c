/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "union_iterator.h"

int cmpLastDocId(const void *e1, const void *e2, const void *udata) {
  const QueryIterator *it1 = e1, *it2 = e2;
  if (it1->LastDocId < it2->LastDocId) {
    return 1;
  } else if (it1->LastDocId > it2->LastDocId) {
    return -1;
  }
  return 0;
}

static void resetMinIdHeap(UnionIterator *ui) {
  heap_t *hp = ui->heapMinId;
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
  if (ui->heapMinId) {
    resetMinIdHeap(ui);
  }
}

/**
 * Removes the exhausted iterator from the active list, so that future
 * reads will no longer iterate over it
 */
static inline int UI_RemoveExhausted(UnionIterator *it, int idx) {
  // Quickly remove the iterator by swapping it with the last iterator.
  it->its[idx] = it->its[--it->num]; // Also decrement the number of iterators
  // Repeat the same index again, because we have a new iterator at the same position
  return idx - 1;
}

static size_t UI_NumEstimated(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  return ui->nExpected;
}

static void UI_Rewind(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  QITER_CLEAR_EOF(base);
  base->LastDocId = 0;

  UI_SyncIterList(ui);

  // rewind all child iterators
  for (size_t i = 0; i < ui->num; i++) {
    ui->its[i]->Rewind(ui->its[i]);
  }
}

static inline void UI_SetFullFlat(UnionIterator *ui) {
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->LastDocId == ui->base.LastDocId) {
      UI_AddChild(ui, cur);
    }
  }
}

static inline void UI_QuickSet(UnionIterator *ui, QueryIterator *match) {
  ui->base.LastDocId = match->LastDocId;
  UI_AddChild(ui, match);
}

static inline IteratorStatus UI_Skip_Full_Flat(QueryIterator *base, const t_docId nextId) {
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  ui->base.LastDocId = UINT64_MAX;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->LastDocId < nextId) {
      int rc = cur->SkipTo(cur, nextId);
      if (rc == ITERATOR_OK) {
        UI_AddChild(ui, cur);
      } else if (rc == ITERATOR_EOF) {
        i = UI_RemoveExhausted(ui, i);
        continue;
      } else if (rc != ITERATOR_NOTFOUND) {
        return rc;
      }
    } else if (cur->LastDocId == nextId) {
      UI_AddChild(ui, cur);
    }
    // Look for the minimal LastDocId
    if (ui->base.LastDocId > cur->LastDocId) ui->base.LastDocId = cur->LastDocId;
  }

  if (ui->base.current->docId) {
    // Current record is set - we found what we were looking for
    return ITERATOR_OK;
  } else if (ui->num) {
    // We didn't find the requested ID, but we set ui->base.LastDocId to the next minimal ID.
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
  const t_docId lastId = ui->base.LastDocId;
  ui->base.LastDocId = UINT64_MAX;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->LastDocId == lastId) {
      int rc = cur->Read(cur);
      if (rc == ITERATOR_EOF) {
        i = UI_RemoveExhausted(ui, i);
        continue;
      } else if (rc != ITERATOR_OK) {
        return rc;
      }
    }
    if (ui->base.LastDocId > cur->LastDocId) ui->base.LastDocId = cur->LastDocId;
  }
  if (ui->num) {
    UI_SetFullFlat(ui);
    return ITERATOR_OK;
  } else {
    QITER_SET_EOF(base);
    return ITERATOR_EOF;
  }
}

static inline IteratorStatus UI_Skip_Quick_Flat(QueryIterator *base, const t_docId nextId) {
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  ui->base.LastDocId = UINT64_MAX;
  QueryIterator *minIt;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->LastDocId < nextId) {
      int rc = cur->SkipTo(cur, nextId);
      if (rc == ITERATOR_OK) {
        UI_QuickSet(ui, cur);
        return ITERATOR_OK;
      } else if (rc == ITERATOR_EOF) {
        i = UI_RemoveExhausted(ui, i);
        continue;
      } else if (rc != ITERATOR_NOTFOUND) {
        return rc;
      }
    } else if (cur->LastDocId == nextId) {
      UI_QuickSet(ui, cur);
      return ITERATOR_OK;
    }
    // Look for the minimal LastDocId + its iterator
    if (ui->base.LastDocId > cur->LastDocId) {
      ui->base.LastDocId = cur->LastDocId;
      minIt = cur;
    }
  }

  if (ui->num) {
    // We didn't find the requested ID, but we set ui->base.LastDocId to the next minimal ID,
    // And `minIt` is set to an iterator holding this ID
    UI_AddChild(ui, minIt);
    return ITERATOR_NOTFOUND;
  } else {
    QITER_SET_EOF(base);
    return ITERATOR_EOF;
  }
}

static inline IteratorStatus UI_Read_Quick_Flat(QueryIterator *base) {
  IteratorStatus rc = UI_Skip_Quick_Flat(base, base->LastDocId + 1);
  return rc == ITERATOR_NOTFOUND ? ITERATOR_OK : rc;
}

static inline IteratorStatus UI_Skip_Full_Heap(QueryIterator *base, const t_docId nextId) {
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  QueryIterator *cur;
  heap_t *hp = ui->heapMinId;
  AggregateResult_Reset(ui->base.current);
  while ((cur = heap_peek(hp)) && cur->LastDocId < nextId) {
    int rc = cur->SkipTo(cur, nextId);
    if (rc == ITERATOR_OK || rc == ITERATOR_NOTFOUND) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == ITERATOR_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    ui->base.LastDocId = cur->LastDocId;
    heap_cb_root(hp, (HeapCallback)UI_AddChild, ui);
    return nextId == cur->LastDocId ? ITERATOR_OK : ITERATOR_NOTFOUND;
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
  heap_t *hp = ui->heapMinId;
  AggregateResult_Reset(ui->base.current);
  while ((cur = heap_peek(hp)) && cur->LastDocId == base->LastDocId) {
    int rc = cur->Read(cur);
    if (rc == ITERATOR_OK) {
      heap_replace(hp, cur); // replace current iterator with itself to update its position
    } else if (rc == ITERATOR_EOF) {
      heap_poll(hp);
    } else {
      return rc;
    }
  }

  if (cur) {
    ui->base.LastDocId = cur->LastDocId;
    heap_cb_root(hp, (HeapCallback)UI_AddChild, ui);
    return ITERATOR_OK;
  }
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}

static inline IteratorStatus UI_Skip_Quick_Heap(QueryIterator *base, const t_docId nextId) {
  UnionIterator *ui = (UnionIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  QueryIterator *cur;
  heap_t *hp = ui->heapMinId;
  AggregateResult_Reset(ui->base.current);
  while ((cur = heap_peek(hp)) && cur->LastDocId < nextId) {
    int rc = cur->SkipTo(cur, nextId);
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
    return nextId == cur->LastDocId ? ITERATOR_OK : ITERATOR_NOTFOUND;
  }
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}

static inline IteratorStatus UI_Read_Quick_Heap(QueryIterator *base) {
  IteratorStatus rc = UI_Skip_Quick_Heap(base, base->LastDocId + 1);
  return rc == ITERATOR_NOTFOUND ? ITERATOR_OK : rc;
}

void UnionIterator_Free(QueryIterator *base) {
  if (base == NULL) return;

  UnionIterator *ui = (UnionIterator *)base;
  for (int i = 0; i < ui->num_orig; i++) {
    QueryIterator *it = ui->its_orig[i];
    if (it) {
      it->Free(it);
    }
  }

  IndexResult_Free(ui->base.current);
  if (ui->heapMinId) heap_free(ui->heapMinId);
  rm_free(ui->its);
  rm_free(ui->its_orig);
  rm_free(ui);
}

QueryIterator *NewUnionIterator(QueryIterator **its, int num, bool quickExit,
                                double weight, QueryNodeType type, const char *q_str, IteratorsConfig *config) {
  // create union context
  UnionIterator *ctx = rm_calloc(1, sizeof(UnionIterator));
  ctx->its_orig = its;
  ctx->origType = type;
  ctx->num_orig = num;
  ctx->its = rm_malloc(num * sizeof(*ctx->its));
  ctx->heapMinId = NULL;
  ctx->q_str = q_str;

  // bind the union iterator calls
  QueryIterator *it = &ctx->base;
  it->type = UNION_ITERATOR;
  QITER_CLEAR_EOF(it);
  it->LastDocId = 0;
  it->current = NewUnionResult(num, weight);
  it->NumEstimated = UI_NumEstimated;
  it->Free = UnionIterator_Free;
  it->Rewind = UI_Rewind;

  ctx->nExpected = 0;
  for (size_t i = 0; i < num; ++i) {
    ctx->nExpected += its[i]->NumEstimated(its[i]);
  }

  if (num > config->minUnionIterHeap) {
    it->Read = quickExit ? UI_Read_Quick_Heap : UI_Read_Full_Heap;
    it->SkipTo = quickExit ? UI_Skip_Quick_Heap : UI_Skip_Full_Heap;
    ctx->heapMinId = rm_malloc(heap_sizeof(num));
    heap_init(ctx->heapMinId, cmpLastDocId, NULL, num);
  } else {
    it->Read = quickExit ? UI_Read_Quick_Flat : UI_Read_Full_Flat;
    it->SkipTo = quickExit ? UI_Skip_Quick_Flat : UI_Skip_Full_Flat;
  }

  UI_SyncIterList(ctx);
  return it;
}
