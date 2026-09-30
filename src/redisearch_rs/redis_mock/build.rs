/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
//! Compiles the C shims that stand in for variadic Redis module API functions.
//! Stable Rust cannot define C-variadic functions, and calling a fixed-arity
//! function through a variadic pointer is not portable (Apple arm64 passes
//! variadic arguments on the stack), so the variadic entry points live in C
//! and forward to fixed-arity Rust functions.
fn main() {
    println!("cargo:rerun-if-changed=src/variadic_shims.c");
    cc::Build::new()
        .file("src/variadic_shims.c")
        .compile("redis_mock_variadic_shims");
}
