/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/*
 * C baseline that drives the *real* HybridIterator (iterators/hybrid_reader.c),
 * the production counterpart of the Rust VectorTopKIterator / VectorScoreSource.
 *
 *
 * It links against libredisearch_all.a (bundled by build_utils), which provides
 * NewHybridVectorIterator, the Rust-backed NewSortedIdListIterator child, the
 * doc table, and the min-max heap.
 *
 * The HybridIterator only touches `sctx` in three spots:
 *   - sctx->time.skipTimeoutChecks   (we set true to disable timeout checks)
 *   - sctx->spec->diskSpec           (zeroed => RAM path)
 *   - sctx->spec->docs.ttl           (zeroed/NULL => field-expiration gate off,
 *                                     so the DocTable expiration calls are
 *                                     never reached)
 * A zeroed IndexSpec plus a tiny RedisSearchCtx therefore suffices; we do not
 * need a fully-initialised index spec, keyspace, or Redis module context.
 */

#include <stddef.h>
#include <stdbool.h>
#include <time.h>

#include "redisearch.h"
#include "search_ctx.h"
#include "spec.h"
#include "iterators/iterator_api.h"
#include "iterators/hybrid_reader.h"
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"

/* Rust-backed child iterator (src/redisearch_rs/headers/iterators_ffi.h).
 * Takes ownership of `ids` and frees them with RedisModule_Free, so the caller
 * must allocate `ids` with RedisModule_Alloc (done Rust-side in the bench, so
 * the alloc/free pair matches the mock allocator). */
QueryIterator *NewSortedIdListIterator(t_docId *ids, uint64_t num, double weight);

/* Run the real HybridIterator over a sorted-id child and count the results.
 *
 * `ids` is an owning pointer (allocated via RedisModule_Alloc by the caller),
 * sorted ascending, of length `child_count`; ownership is transferred to the
 * child iterator, which frees it via RedisModule_Free.
 *
 * force_adhoc: 0 => force VECSIM_HYBRID_BATCHES, non-zero => VECSIM_HYBRID_ADHOC_BF.
 */
size_t bench_c_hybrid(VecSimIndex *index, const void *query_vec, size_t dim, size_t k, t_docId *ids,
                      size_t child_count, int force_adhoc) {
  // Minimal mock context: zeroed spec gives RAM path + no TTL gate.
  IndexSpec spec = {0};
  RedisSearchCtx sctx = {0};
  sctx.spec = &spec;
  sctx.time.skipTimeoutChecks = true;

  QueryIterator *child = NewSortedIdListIterator(ids, child_count, 1.0);

  KNNVectorQuery query = {0};
  query.vector = (void *)query_vec;
  query.vecLen = dim * sizeof(float);
  query.k = k;
  query.order = BY_SCORE;

  VecSimQueryParams qParams = {0};
  // Force the execution mode through the supported path: NewHybridVectorIterator
  // uses qParams.searchMode verbatim when it is non-zero.
  // VecSimSearchMode (vector_index.h) intentionally mirrors VecSearchMode (VecSim);
  // NewHybridVectorIterator performs the same cast internally.
  qParams.searchMode =
      (VecSearchMode)(force_adhoc ? VECSIM_HYBRID_ADHOC_BF : VECSIM_HYBRID_BATCHES);

  FieldFilterContext filterCtx = {
      .field = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX},
      .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT,
  };

  HybridIteratorParams hParams = {
      .sctx = &sctx,
      .index = index,
      .dim = dim,
      .elementType = VecSimType_FLOAT32,
      .spaceMetric = VecSimMetric_L2,
      .query = query,
      .qParams = qParams,
      .vectorScoreField = (char *)"__v_score",
      .canTrimDeepResults = true,
      .childIt = child,
      .timeout = {0, 0},
      .filterCtx = &filterCtx,
  };

  QueryError err = QueryError_Default();
  QueryIterator *it = NewHybridVectorIterator(hParams, &err);
  if (it == NULL || QueryError_HasError(&err)) {
    if (it)
      it->Free(it);
    else if (child)
      child->Free(child);
    QueryError_ClearError(&err);
    return 0;
  }

  size_t count = 0;
  while (it->Read(it) == ITERATOR_OK) {
    count++;
  }

  it->Free(it);  // also frees the child iterator
  return count;
}
