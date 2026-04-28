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
#include <stdbool.h>
#include <stddef.h>

#define COLLECT_ARGS_COUNT_BUF_LEN 24  // Enough digits for a 64-bit decimal count plus NUL.

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Returns the number of object slots the caller must reserve in `objs_buf`
 * passed to `buildCollectArgs`.
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
 * Build COLLECT args for distributed planning.
 *
 * Remote layout: [nargs, original_args...].
 * Local layout:  [nargs, original_args..., AS, user_alias].
 *
 * Distributed COLLECT splits the innermost GROUPBY reducer pair. The remote
 * COLLECT consumes ordinary item rows on each shard and emits one payload per
 * shard group. The local COLLECT consumes coordinator merge rows, where each
 * row is already a shard group and the collected items are stored under
 * PLN_Reducer.inputAlias. Outer coordinator GROUPBY reducers continue to
 * consume ordinary item rows.
 *
 * The local input source is not encoded in args; it is carried as planner
 * metadata and later resolved into ReducerOptions::input_key.
 *
 * @param objs_buf     Caller-provided buffer; size = collectObjsBufLen(src_args->argc, user_alias != NULL)
 * @param count_buf    Caller-formatted decimal string of `src_args->argc`. Lifetime
 *                     must outlive the returned ArgsCursor (typically a stack buffer
 *                     at the call site, sized to COLLECT_ARGS_COUNT_BUF_LEN).
 * @param src_args     The original reducer's parsed args (without the leading nargs)
 * @param user_alias   User-visible alias to preserve via AS, or NULL for remote args
 */
ArgsCursor buildCollectArgs(void **objs_buf, const char *count_buf, const ArgsCursor *src_args,
                            const char *user_alias);

#ifdef __cplusplus
}
#endif
