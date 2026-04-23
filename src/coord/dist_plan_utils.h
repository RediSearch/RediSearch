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
 * Build the ArgsCursor layout for forwarding a COLLECT reducer to a shard.
 *
 * PLNGroupStep_AddReducer expects [nargs, arg1, ..., argN] but the parsed
 * reducer args only contain [arg1, ..., argN] (nargs was consumed). This
 * function prepends the count string.
 *
 * @param out       Populated with the resulting ArgsCursor
 * @param objs_buf  Caller-provided buffer; must hold at least src_args->argc + 1 elements
 * @param count_buf Caller-provided buffer for the count string; must be at least 16 bytes
 * @param src_args  The original reducer's parsed args (without the leading nargs)
 */
void buildShardCollectArgs(ArgsCursor *out, void **objs_buf, char *count_buf,
                           const ArgsCursor *src_args);

/**
 * Build the ArgsCursor layout for forwarding a COLLECT reducer to the coordinator.
 *
 * Layout: [nargs, original_args..., __SOURCE__, shard_alias, AS, user_alias]
 * where nargs = src_args->argc + 2 (covering original_args + __SOURCE__ + shard_alias).
 * The `__SOURCE__` sub-arg is registered with fixed arity (exactly one alias),
 * so the ArgParser does not expect an explicit count token between the sub-arg
 * name and its value — matching the wire format used by `LIMIT` (which is also
 * fixed-arity).
 *
 * AS + user_alias sit outside the counted block so PLNGroupStep_AddReducer
 * picks them up as the explicit alias.
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
