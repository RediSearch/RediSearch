/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

fn main() {
    // There's no reliable way of determining whether we actually
    // need to build and link to `hiredis` or not
    // (see https://github.com/rust-lang/cargo/issues/4001).
    // For instance, when running `cargo check`, there's no need to build `hiredis`,
    // but when building unit tests, `hiredis` needs to be there.
    // Therefore, we link against `hiredis` only if the `unittest` feature
    // is enabled.

    // Link against libhiredis.a
    // The hiredis library is built by CMake and placed in the hiredis subdirectory
    #[cfg(feature = "unittest")]
    build_utils::link_static_libraries(&[("hiredis", "hiredis")]);
}
