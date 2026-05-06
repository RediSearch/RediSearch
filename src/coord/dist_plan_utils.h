/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "rmutil/args.h"
#include "query_error.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define COLLECT_ARGS_COUNT_BUF_LEN 24  // Enough digits for a 64-bit decimal count plus NUL.

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Returns the number of object slots the caller must reserve in `objs_buf`
 * passed to `buildLocalCollectArgs` or `buildRemoteCollectArgs`.
 *
 * Layout reminder:
 *   - 1 slot for `nargs`
 *   - `argc` slots for the forwarded args
 *   - 2 slots for "AS" + user_alias (only when `has_alias` is true)
 */
static inline size_t collectObjsBufLen(size_t argc, bool has_alias) {
  return argc + 1 + (has_alias ? 2 : 0);
}

/**
 * Parsed and validated LIMIT clause from a COLLECT reducer's argument list.
 */
typedef struct CollectLimit {
  uint64_t offset;
  uint64_t count;
} CollectLimit;

/**
 * Scan `src_args` for a `LIMIT offset count` triplet, parse and validate it.
 *
 * On no LIMIT keyword: returns true and sets *out_present = false.
 * On valid LIMIT:      returns true, sets *out_present = true and *out_limit.
 * On invalid LIMIT (missing tail tokens, non-numeric offset/count, exceeds
 *                   max_results, or offset+count overflow): returns false and
 *                   sets *status.
 *
 * @param src_args    The reducer's parsed args (without the leading nargs).
 * @param max_results Maximum permitted value for offset and count individually,
 *                    and their sum must fit in a signed 64-bit integer.
 * @param out_present Set to true iff a LIMIT triplet was found and valid.
 * @param out_limit   Populated with the parsed values when *out_present is true.
 * @param status      Receives a human-readable error on failure.
 */
bool parseCollectLimit(const ArgsCursor *src_args, uint64_t max_results, bool *out_present,
                       CollectLimit *out_limit, QueryError *status);

#ifdef __cplusplus
}

#include <string>

/**
 * Pre-computed shard-side LIMIT tokens, ready to slot into an objs_buf.
 *
 * The shard request rewrites `LIMIT offset count` to `LIMIT 0 (offset+count)`
 * so the shard streams enough rows for the coordinator to apply the original
 * window. Both members are decimal strings built by rewriteCollectLimit().
 */
struct ShardCollectLimit {
  std::string offset;  // always "0"
  std::string count;   // decimal of original_offset + original_count
};

/**
 * Build a ShardCollectLimit from a validated CollectLimit.
 * The result is safe to pass to buildRemoteCollectArgs().
 */
ShardCollectLimit rewriteCollectLimit(const CollectLimit *limit);

/**
 * Build COLLECT args for the coordinator-side (local) reducer.
 *
 * Layout: [nargs, original_args..., AS, user_alias].
 *
 * The local input source is not encoded in args; it is carried as planner
 * metadata and later resolved into ReducerOptions::input_key.
 *
 * @param objs_buf    Caller-provided buffer;
 *                    size = collectObjsBufLen(src_args->argc, true)
 * @param count_buf   Caller-formatted decimal string of src_args->argc.
 * @param src_args    The original reducer's parsed args (without leading nargs).
 * @param user_alias  User-visible alias to preserve via AS.
 */
ArgsCursor buildLocalCollectArgs(void **objs_buf, const char *count_buf,
                                 const ArgsCursor *src_args, const char *user_alias);

/**
 * Build COLLECT args for the shard-side (remote) reducer, optionally rewriting
 * a pre-parsed LIMIT triplet to `LIMIT 0 (offset + count)`.
 *
 * Layout: [nargs, original_args...].
 *
 * If `rewrite` is non-NULL, scans `src_args` for the LIMIT keyword (case-
 * insensitive) and patches the corresponding slots in `objs_buf` with
 * `rewrite->offset.c_str()` and `rewrite->count.c_str()`. `rewrite` must
 * outlive the returned ArgsCursor (i.e. until copyArgs() is called to
 * deep-copy the strings into the plan's BlkAlloc).
 *
 * Precondition: if `rewrite` is non-NULL, `src_args` must contain a LIMIT
 * keyword (guaranteed when the caller used parseCollectLimit() to detect it).
 *
 * @param objs_buf   Caller-provided buffer;
 *                   size = collectObjsBufLen(src_args->argc, false)
 * @param count_buf  Caller-formatted decimal string of src_args->argc.
 * @param src_args   The original reducer's parsed args (without leading nargs).
 * @param rewrite    Pre-computed shard-side LIMIT tokens, or NULL for no rewrite.
 */
ArgsCursor buildRemoteCollectArgs(void **objs_buf, const char *count_buf,
                                  const ArgsCursor *src_args,
                                  const ShardCollectLimit *rewrite);

#endif
