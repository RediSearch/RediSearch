/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "variadic_helper_from_rust.h"


int RedisModule_ReplyWithErrorFormat(struct RedisModuleCtx *ctx, const char *fmt, ...) {
    // possible: Processing of the variadic function arguments
    
    // part of workaround for <https://github.com/rust-lang/rust/issues/44930>
    int arg_from_c_is_return_value=0; // equals REDISMODULE_OK
    // the following function is defined in Rust in the crate redis_mock
    return non_variadic_reply_with_error_format(ctx, fmt, arg_from_c_is_return_value);
}
