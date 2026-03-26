/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{SEARCH_ENTERPRISE_ITERATORS, SearchEnterpriseIterators};

mod timespec;

pub mod empty;
pub mod id_list;
pub mod inverted_index;
pub mod metric;
pub mod not;
pub mod optional;
pub mod profile;
pub mod wildcard;

/// Initialize the global `SEARCH_ENTERPRISE_ITERATORS` with the provided implementation.
///
/// # Safety
/// The caller must ensure it has the same version of the `SearchEnterpriseIterators` trait as this library.
#[unsafe(no_mangle)]
#[expect(
    improper_ctypes_definitions,
    reason = "This function is intended to be called from other Rust code having the same version of the `SearchEnterpriseIterators` trait, and is not intended to be called from C code directly."
)]
pub extern "C" fn redisearch_init_iterators(iters: *mut dyn SearchEnterpriseIterators) {
    let boxed = unsafe { Box::from_raw(iters) };
    let _ = SEARCH_ENTERPRISE_ITERATORS.set(boxed);
}

/// Get the global `SEARCH_ENTERPRISE_ITERATORS` instance.
///
/// # Safety
/// The following safety conditions must be upheld by the caller:
/// - The caller must ensure that `redisearch_init_iterators` has been called before invoking this function.
/// - The caller must ensure it has the same version of the `SearchEnterpriseIterators` trait as this library.
#[unsafe(no_mangle)]
#[expect(
    improper_ctypes_definitions,
    reason = "This function is intended to be called from other Rust code having the same version of the `SearchEnterpriseIterators` trait, and is not intended to be called from C code directly."
)]
pub extern "C" fn redisearch_get_iterators() -> *const dyn SearchEnterpriseIterators {
    SEARCH_ENTERPRISE_ITERATORS.get().map(|b| b.as_ref()).unwrap_or_else(|| {
        panic!("SEARCH_ENTERPRISE_ITERATORS is not initialized. Please ensure that `redisearch_init_iterators` is called before invoking this function.")
    })
}
