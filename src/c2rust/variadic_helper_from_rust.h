/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once
#include <redismodule.h>

// This files contains the non-variadic function definitions that are implemented in
// Rust.

// this function is defined by rust and needs a fixed number of args, it is called from C.
int non_variadic_reply_with_error_format(RedisModuleCtx *ctx, const char *fmt, int add_args_example);

