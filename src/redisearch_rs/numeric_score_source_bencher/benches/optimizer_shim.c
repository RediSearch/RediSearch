/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/*
 * C baseline that drives the *real* OptimizerIterator (iterators/optimizer_reader.c),
 * the production counterpart of the Rust NumericTopKIterator / NumericScoreSource.
 *
 * It links against libredisearch_c_bundle.a (bundled by build_utils), which provides
 * NewOptimizerIterator, the Rust-backed child iterators and numeric range iterator,
 * the doc table, and the min-max heap.
 *
 * The search context, index spec, numeric range tree and doc table are all built
 * Rust-side (`rqe_iterators_test_utils::TestContext`) and handed in, so both sides
 * of the benchmark read the very same index.
 */

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "config.h"
#include "query_optimizer.h"
#include "query_types.h"
#include "redisearch.h"
#include "search_ctx.h"
#include "spec.h"
#include "iterators/iterator_api.h"
#include "iterators/optimizer_reader.h"

/* Rust-backed child iterators (src/redisearch_rs/headers/iterators_ffi.h).
 * NewSortedIdListIterator takes ownership of `ids` and frees them with
 * RedisModule_Free, so every buffer handed to it here comes from
 * RedisModule_Alloc rather than the C runtime's allocator. */
QueryIterator *NewSortedIdListIterator(t_docId *ids, uint64_t num, double weight);
QueryIterator *NewWildcardIterator_NonOptimized(t_docId max_id, double weight);
/* Takes ownership of `its` and of every child it holds. */
QueryIterator *NewUnionIterator(QueryIterator **its, int32_t num, bool quick_exit, double weight,
                                QueryNodeType type_, const char *q_str,
                                const IteratorsConfig *config);

/* Sorted-id-list child over every `stride`-th id of `ids`, starting at `offset`.
 *
 * A stride of 1 selects them all. The buffer the child takes ownership of is
 * filled in a single pass, so a child costs one allocation and one copy of the
 * ids it holds — no more than building the equivalent list Rust-side. */
static QueryIterator *make_id_list_child(const t_docId *ids, size_t count, size_t offset,
                                         size_t stride) {
  size_t len = count > offset ? (count - offset + stride - 1) / stride : 0;
  t_docId *part = RedisModule_Alloc((len ? len : 1) * sizeof(*part));
  size_t i = 0;
  for (size_t j = offset; j < count; j += stride) {
    part[i++] = ids[j];
  }
  return NewSortedIdListIterator(part, len, 1.0);
}

/* Spread `ids` round-robin over `arity` sorted-id-list children and union them.
 *
 * Every child then holds ids from across the whole doc-id space, so each step of
 * the union interleaves them — the cost a single id list does not have. The
 * union takes ownership of the array and of its children; `ids` itself is only
 * borrowed.
 */
static QueryIterator *make_union_child(const t_docId *ids, size_t count, size_t arity,
                                       const IteratorsConfig *config) {
  QueryIterator **its = RedisModule_Alloc(arity * sizeof(*its));
  for (size_t slot = 0; slot < arity; slot++) {
    its[slot] = make_id_list_child(ids, count, slot, arity);
  }
  // quick_exit matches the union the query planner builds for a filter child.
  return NewUnionIterator(its, (int32_t)arity, true, 1.0, QN_UNION, NULL, config);
}

/* Read an iterator to depletion and count the results.
 *
 * Results carrying a borrowed document metadata (the optimizer's heap entries)
 * release it here, the way the result-processor pipeline does downstream. */
static size_t drain(QueryIterator *it) {
  size_t count = 0;
  while (it->Read(it) == ITERATOR_OK) {
    count++;
    DMD_Return(it->current->dmd);
  }
  return count;
}

/* Run the real OptimizerIterator over a child filter and count the results.
 *
 * `ids` is borrowed for the duration of the call: sorted ascending, of length
 * `child_count`, and only read from. A NULL `ids` selects a wildcard child
 * spanning doc ids 1..=`child_count`, the unfiltered case.
 *
 * `union_arity` above 1 wraps the same ids in a union of that many sorted id
 * lists, so the child costs more per read, skip and rewind than a single list.
 *
 * The numeric filter spans the whole field and is created by the iterator
 * itself, which also sizes the first window from the child's estimate.
 */
size_t bench_c_optimizer(RedisSearchCtx *sctx, const char *field_name, const t_docId *ids,
                         size_t child_count, size_t union_arity, size_t k, int ascending) {
  IteratorsConfig config;
  iteratorsConfig_init(&config);

  QueryIterator *child;
  if (!ids) {
    child = NewWildcardIterator_NonOptimized(child_count, 1.0);
  } else if (union_arity > 1) {
    child = make_union_child(ids, child_count, union_arity, &config);
  } else {
    child = make_id_list_child(ids, child_count, 0, 1);
  }

  QOptimizer opt = {0};
  opt.sctx = sctx;
  opt.limit = k;
  opt.asc = (bool)ascending;
  opt.fieldName = field_name;
  opt.field = IndexSpec_GetFieldWithLength(sctx->spec, field_name, strlen(field_name));

  QueryIterator *it = NewOptimizerIterator(&opt, child, &config);
  if (it == NULL) {
    child->Free(child);
    return 0;
  }

  size_t count = drain(it);
  it->Free(it);  // also frees the child iterator and the owned numeric filter
  return count;
}
