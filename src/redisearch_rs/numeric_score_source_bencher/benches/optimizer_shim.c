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
#include "redisearch.h"
#include "search_ctx.h"
#include "spec.h"
#include "iterators/iterator_api.h"
#include "iterators/optimizer_reader.h"

/* Rust-backed child iterators (src/redisearch_rs/headers/iterators_ffi.h).
 * NewSortedIdListIterator takes ownership of `ids` and frees them with
 * RedisModule_Free, so the caller must allocate `ids` with RedisModule_Alloc
 * (done Rust-side in the bench, so the alloc/free pair matches the mock
 * allocator). */
QueryIterator *NewSortedIdListIterator(t_docId *ids, uint64_t num, double weight);
QueryIterator *NewWildcardIterator_NonOptimized(t_docId max_id, double weight);

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
 * `ids` is an owning pointer (allocated via RedisModule_Alloc by the caller),
 * sorted ascending, of length `child_count`; ownership is transferred to the
 * child iterator, which frees it via RedisModule_Free. A NULL `ids` selects a
 * wildcard child spanning doc ids 1..=`child_count`, the unfiltered case.
 *
 * The numeric filter spans the whole field and is created by the iterator
 * itself, which also sizes the first window from the child's estimate.
 */
size_t bench_c_optimizer(RedisSearchCtx *sctx, const char *field_name, t_docId *ids,
                         size_t child_count, size_t k, int ascending) {
  IteratorsConfig config;
  iteratorsConfig_init(&config);

  QueryIterator *child = ids ? NewSortedIdListIterator(ids, child_count, 1.0)
                             : NewWildcardIterator_NonOptimized(child_count, 1.0);

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
