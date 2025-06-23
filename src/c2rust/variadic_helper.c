/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <redismodule.h>

#include "variadic_helper_from_rust.h"

int tmp_RedisModule_ReplyWithErrorFormat(RedisModuleCtx *ctx, const char *fmt, ...) {
    // possible: Processing of the variadic templates
    
    // part of workaround for <https://github.com/rust-lang/rust/issues/44930>
    int arg_from_c_is_return_value=REDISMODULE_OK;
    return non_variadic_reply_with_error_format(ctx, fmt, arg_from_c_is_return_value);
}

void cside_mock_setup() {
    RedisModule_ReplyWithErrorFormat = tmp_RedisModule_ReplyWithErrorFormat;
}
