/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

mod collection;
mod comparison;
mod debug;
mod dereference;
mod hash;
mod shared;
mod string;

/// Skips backtrace symbolication for an expected panic, only under
/// `cargo nextest run`: the [panic hook](std::panic::set_hook) is process-global
/// and never restored, which is harmless under nextest's one-process-per-test
/// model but would silence unrelated tests' panics under plain `cargo test`.
pub(crate) fn suppress_panic_backtrace() {
    if std::env::var_os("NEXTEST").is_some() {
        std::panic::set_hook(Box::new(|_| {}));
    }
}
