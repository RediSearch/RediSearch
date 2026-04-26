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
 * Build shard COLLECT args: [nargs, original_args...].
 *
 * @param out       Populated with the resulting ArgsCursor
 * @param objs_buf  Caller-provided buffer; must hold at least src_args->argc + 1 elements
 * @param count_buf Caller-provided buffer for the count string; must be at least 16 bytes
 * @param src_args  The original reducer's parsed args (without the leading nargs)
 */
void buildShardCollectArgs(ArgsCursor *out, void **objs_buf, char *count_buf,
                           const ArgsCursor *src_args);

/**
 * Build coordinator COLLECT args:
 *
 * [nargs, original_args..., __SOURCE__, shard_alias, AS, user_alias]
 * where nargs covers only original_args + __SOURCE__ + shard_alias. `AS`
 * remains outside the counted block for PLNGroupStep_AddReducer.
 *
 * @param out         Populated with the resulting ArgsCursor
 * @param objs_buf    Caller-provided buffer; must hold at least src_args->argc + 5 elements
 * @param count_buf   Caller-provided buffer for the count string; must be at least 16 bytes
 * @param src_args    The original reducer's parsed args (without the leading nargs)
 * @param shard_alias Alias of the shard's COLLECT reducer output column
 * @param user_alias  User-visible alias to preserve via AS
 */
void buildCoordCollectArgs(ArgsCursor *out, void **objs_buf, char *count_buf,
                           const ArgsCursor *src_args,
                           const char *shard_alias, const char *user_alias);

#ifdef __cplusplus
}
#endif
