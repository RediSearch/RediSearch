/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations of C functions used across integration tests.
//!
//! These are unified mocks that satisfy the linker for all test modules in this
//! crate. Since all tests share a single binary, each mock symbol must be
//! defined exactly once.

use ffi::RSQueryTerm;

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut RSQueryTerm) {
    // Several tests use stack-allocated RSQueryTerm values, so this must be a
    // no-op rather than panicking on non-null pointers.
}
