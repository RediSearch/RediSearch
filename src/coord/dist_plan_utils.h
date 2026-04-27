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

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Build COLLECT args for distributed planning.
 *
 * Remote layout: [nargs, original_args...].
 * Local layout:  [nargs, original_args..., AS, user_alias].
 *
 * The local input source is not encoded in args; it is carried as planner
 * metadata and later resolved into ReducerOptions::source_key.
 *
 * @param out          Populated with the resulting ArgsCursor
 * @param objs_buf     Caller-provided buffer; must hold at least src_args->argc + 3 elements
 * @param count_buf    Caller-provided buffer for the count string; must be at least 16 bytes
 * @param src_args     The original reducer's parsed args (without the leading nargs)
 * @param user_alias   User-visible alias to preserve via AS, or NULL for remote args
 */
void buildCollectArgs(ArgsCursor *out, void **objs_buf, char *count_buf,
                      const ArgsCursor *src_args, const char *user_alias);

#ifdef __cplusplus
}
#endif
