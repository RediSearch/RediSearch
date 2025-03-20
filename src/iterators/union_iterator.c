/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "union_iterator.h"

static inline int cmpLastDocId(const void *e1, const void *e2, const void *udata) {
  const QueryIterator *it1 = e1, *it2 = e2;
  return (int64_t)(it2->lastDocId - it1->lastDocId);
}

static inline void resetMinIdHeap(UnionIterator *ui) {
  heap_t *hp = ui->heap_min_id;
  heap_clear(hp);
  for (int i = 0; i < ui->num_orig; i++) {
    heap_offerx(hp, ui->its_orig[i]);
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
  RS_ASSERT(0 <= idx && idx < it->num);
  it->its[idx] = it->its[--it->num]; // Also decrement the number of iterators
}

static size_t UI_NumEstimated(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  size_t estimation = 0;
  for (size_t i = 0; i < ui->num_orig; ++i) {
    estimation += ui->its_orig[i]->NumEstimated(ui->its_orig[i]);
  }
  return estimation;
}

static void UI_Rewind(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  base->atEOF = false;
  base->lastDocId = 0;

  // rewind all child iterators
  for (size_t i = 0; i < ui->num_orig; i++) {
    ui->its_orig[i]->Rewind(ui->its_orig[i]);
  }

  UI_SyncIterList(ui);
}

// Collect all children whose current result is `ui->base.lastDocId`.
// Assumes that ui->base.current is already reset
static inline void UI_SetFullFlat(UnionIterator *ui) {
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    if (cur->lastDocId == ui->base.lastDocId) {
      UI_AddChild(ui, cur);
    }
  }
}

// Set `ui` state (lastId, current result) according to a single child iterator
static inline void UI_QuickSet(UnionIterator *ui, QueryIterator *match) {
  ui->base.lastDocId = match->lastDocId;
  UI_AddChild(ui, match);
}

// Skip implementation, for full (no quick exit) mode, using an array.
// In full mode, we should never have child iterators lagging behind the union's last result
static inline IteratorStatus UI_Skip_Full_Flat(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (base->atEOF) {
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
      } else if (rc == ITERATOR_NOTFOUND) {
        // No match. Finish the loop iteration normally
      } else if (rc == ITERATOR_EOF) {
        UI_RemoveExhausted(ui, i);
        i--;
        continue;
      } else {
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
    base->atEOF = true;
    return ITERATOR_EOF;
  }
}

// Read implementation, for full (no quick exit) mode, using an array.
// In full mode, we should never have child iterators lagging behind the union's last result
static inline IteratorStatus UI_Read_Full_Flat(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  const t_docId lastId = ui->base.lastDocId;
  t_docId minId = UINT64_MAX;
  AggregateResult_Reset(ui->base.current);
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    RS_LOG_ASSERT(cur->lastDocId >= lastId,
      "When reading in full mode, we should never have children behind the union last result");
    if (cur->lastDocId == lastId) {
      IteratorStatus rc = cur->Read(cur);
      if (rc == ITERATOR_OK) {
        // Finish the loop iteration normally
      } else if (rc == ITERATOR_EOF) {
        UI_RemoveExhausted(ui, i);
        i--;
        continue;
      } else {
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
    base->atEOF = true;
    return ITERATOR_EOF;
  }
}

// Skip implementation, for quick exit mode, using an array.
// In quick mode, we may have child iterators lagging behind the union's last result, so we cannot assume otherwise
static inline IteratorStatus UI_Skip_Quick_Flat(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  t_docId minId = UINT64_MAX;
  QueryIterator *minIt;
  AggregateResult_Reset(ui->base.current);
  // TODO: performance improvement?
  // We can avoid performing any `SkipTo` call if we first scan an check if we already have a child
  // that matches the required ID.
  for (int i = 0; i < ui->num; i++) {
    QueryIterator *cur = ui->its[i];
    IteratorStatus rc;
    if (cur->lastDocId < nextId) {
      // Actually read from the child
      rc = cur->SkipTo(cur, nextId);
    } else if (cur->lastDocId == nextId) {
      // Current iterator previously matched this ID
      rc = ITERATOR_OK;
    } else { // (cur->lastDocId > nextId)
      // Current iterator previously advanced passed this ID
      rc = ITERATOR_NOTFOUND;
    }
    switch (rc) {
      case ITERATOR_OK:
        UI_QuickSet(ui, cur);
        return ITERATOR_OK;
      case ITERATOR_NOTFOUND:
        // Look for the minimal lastDocId + its iterator
        if (minId > cur->lastDocId) {
          minId = cur->lastDocId;
          minIt = cur;
        }
        break;
      case ITERATOR_EOF:
        UI_RemoveExhausted(ui, i);
        i--;
        break;
      case ITERATOR_TIMEOUT:
        return ITERATOR_TIMEOUT;
    }
  }

  if (ui->num) {
    // We didn't find the requested ID, but we set `minIt` to the next best match.
    UI_QuickSet(ui, minIt);
    return ITERATOR_NOTFOUND;
  } else {
    base->atEOF = true;
    return ITERATOR_EOF;
  }
}

// Read implementation, for quick exit mode, using an array.
// In quick mode, we may have child iterators lagging behind the union's last result, so we cannot assume otherwise.
// Therefore we need to use the children `SkipTo` to quickly get them to where we are. This is exactly
// what we already so in the `SkipTo` implemented above, so we can attempt to skip to the next possible ID.
// We just need to make sure to return `OK` even if we got `NOTFOUND` from the skipping step.
static inline IteratorStatus UI_Read_Quick_Flat(QueryIterator *base) {
  IteratorStatus rc = UI_Skip_Quick_Flat(base, base->lastDocId + 1);
  return rc == ITERATOR_NOTFOUND ? ITERATOR_OK : rc;
}

// Skip implementation, for full (no quick exit) mode, using a heap (many children).
// In full mode, we should never have child iterators lagging behind the union's last result
static inline IteratorStatus UI_Skip_Full_Heap(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (base->atEOF) {
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
    // Set the current union result according to `cur`
    ui->base.lastDocId = cur->lastDocId;
    heap_cb_root(hp, (HeapCallback)UI_AddChild, ui); // Collect all matching children
    return nextId == cur->lastDocId ? ITERATOR_OK : ITERATOR_NOTFOUND;
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

// Read implementation, for full (no quick exit) mode, using a heap (many children).
// In full mode, we should never have child iterators lagging behind the union's last result,
// So we only need a single read on each child that matched on the previous read/skip call.
static inline IteratorStatus UI_Read_Full_Heap(QueryIterator *base) {
  UnionIterator *ui = (UnionIterator *)base;
  if (base->atEOF) {
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
    // Set the current union result according to `cur`
    RS_ASSERT(cur->lastDocId > base->lastDocId);
    ui->base.lastDocId = cur->lastDocId;
    heap_cb_root(hp, (HeapCallback)UI_AddChild, ui); // Collect all matching children
    return ITERATOR_OK;
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

// Skip implementation, for quick exit mode, using a heap (many children).
// In quick mode, we may have child iterators lagging behind the union's last result.
// This implementation is very similar to `UI_Skip_Full_Heap`, beside the quick exit when
// finding an exact match.
static inline IteratorStatus UI_Skip_Quick_Heap(QueryIterator *base, const t_docId nextId) {
  RS_ASSERT(base->lastDocId < nextId);
  UnionIterator *ui = (UnionIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  QueryIterator *cur;
  heap_t *hp = ui->heap_min_id;
  AggregateResult_Reset(ui->base.current);
  // TODO: performance improvement?
  // before attempting to advance lagging iterators, we can perform a quick scan of the heap and check if
  // we already have a matching iterator, saving us from performing any `SkipTo` call, which may be expensive.
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
  base->atEOF = true;
  return ITERATOR_EOF;
}

// Read implementation, for quick exit mode, using a heap.
// In quick mode, we may have child iterators lagging behind the union's last result, so we cannot assume otherwise.
// Therefore we need to use the children `SkipTo` to quickly get them to where we are. This is exactly
// what we already so in the `SkipTo` implemented above, so we can attempt to skip to the next possible ID.
// We just need to make sure to return `OK` even if we got `NOTFOUND` from the skipping step.
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
  UnionIterator *ui = rm_calloc(1, sizeof(UnionIterator));
  ui->its_orig = its;
  ui->type = type;
  ui->num_orig = num;
  ui->its = rm_malloc(num * sizeof(*ui->its));
  ui->heap_min_id = NULL;
  ui->q_str = q_str;

  // bind the union iterator calls
  QueryIterator *base = &ui->base;
  base->type = UNION_ITERATOR;
  base->atEOF = false;
  base->lastDocId = 0;
  base->current = NewUnionResult(num, weight);
  base->NumEstimated = UI_NumEstimated;
  base->Free = UI_Free;
  base->Rewind = UI_Rewind;

  // Choose `Read` and `SkipTo` implementations.
  // We have 2 factors for the choice:
  // 1. quickExit - whether to return after the first match was found without checking if more iterator agree with it,
  //                or should we collect all the results from all the children that agree on the current ID.
  // 2. minUnionIterHeap - choose whether to use a flat array or a heap for tracking the children, according to the number of children
  // Each implementation if fine-tuned for the best performance in its scenario, and relies on the current state
  // of the iterator and how it was left by previous API calls, so we can't change implementation mid-execution.
  if (num > config->minUnionIterHeap) {
    base->Read = quickExit ? UI_Read_Quick_Heap : UI_Read_Full_Heap;
    base->SkipTo = quickExit ? UI_Skip_Quick_Heap : UI_Skip_Full_Heap;
    ui->heap_min_id = rm_malloc(heap_sizeof(num));
    heap_init(ui->heap_min_id, cmpLastDocId, NULL, num);
  } else {
    base->Read = quickExit ? UI_Read_Quick_Flat : UI_Read_Full_Flat;
    base->SkipTo = quickExit ? UI_Skip_Quick_Flat : UI_Skip_Full_Flat;
  }

  UI_SyncIterList(ui);
  return base;
}
