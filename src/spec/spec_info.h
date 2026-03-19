/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_INFO_H
#define SPEC_INFO_H

#include "redismodule.h"
#include "redisearch_api.h"
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;
struct IndexesScanner;

double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, struct IndexesScanner *scanner,
                                     const struct IndexSpec *sp);

/**
 * @return the overhead used by the TAG fields in `sp`, i.e., the size of the
 * TrieMaps used for the `values` and `suffix` fields.
 */
size_t IndexSpec_collect_tags_overhead(const struct IndexSpec *sp);

/**
 * @return the overhead used by the TEXT fields in `sp`, i.e., the size of the
 * sp->terms and sp->suffix Tries.
 */
size_t IndexSpec_collect_text_overhead(const struct IndexSpec *sp);

/**
 * @return the overhead used by the NUMERIC and GEO fields in `sp`, i.e., the accumulated size of all
 * numeric tree structs.
 */
size_t IndexSpec_collect_numeric_overhead(struct IndexSpec *sp);

/**
 * @return all memory used by the index `sp`.
 * Uses the sizes of the doc-table, tag and text overhead if they are not `0`
 * (otherwise compute them in-place). Vector overhead is expected to be passed in as an argument
 * and will not be computed in-place
 */
size_t IndexSpec_TotalMemUsage(struct IndexSpec *sp, size_t doctable_tm_size, size_t tags_overhead,
  size_t text_overhead, size_t vector_overhead);

/* Initialize some index stats that might be useful for scoring functions */
void IndexSpec_GetStats(struct IndexSpec *sp, RSIndexStats *stats);

/* Get the number of indexing failures */
size_t IndexSpec_GetIndexErrorCount(const struct IndexSpec *sp);

/**
 * Exposing all the fields of the index to INFO command.
 * @param ctx - the redis module info context
 * @param sp - the index spec
 * @param obfuscate - if true, obfuscate the index name and field names
 * @param skip_unsafe_ops - if true, skips operations unsafe in signal handler context (allocations, locks)
 */
void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, struct IndexSpec *sp, bool obfuscate, bool skip_unsafe_ops);

#ifdef __cplusplus
}
#endif

#endif // SPEC_INFO_H
