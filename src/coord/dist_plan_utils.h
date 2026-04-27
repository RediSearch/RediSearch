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

#define COLLECT_ARGS_COUNT_BUF_LEN 24  // Enough digits for a 64-bit decimal count plus NUL.

#ifdef __cplusplus
extern "C" {
#endif

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
 * PLN_Reducer.sourceAlias. Outer coordinator GROUPBY reducers continue to
 * consume ordinary item rows.
 *
 * The local input source is not encoded in args; it is carried as planner
 * metadata and later resolved into ReducerOptions::source_key.
 *
 * @param objs_buf     Caller-provided buffer; must hold at least src_args->argc + 3 elements
 * @param count_buf    Caller-provided buffer for the count string; must be at least
 *                     COLLECT_ARGS_COUNT_BUF_LEN bytes
 * @param src_args     The original reducer's parsed args (without the leading nargs)
 * @param user_alias   User-visible alias to preserve via AS, or NULL for remote args
 */
ArgsCursor buildCollectArgs(void **objs_buf, char *count_buf, const ArgsCursor *src_args,
                            const char *user_alias);

#ifdef __cplusplus
}
#endif
