/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module provides FFI bindings for the C trie functions used in the [crate::CTrieMap] implementation.

// We hide the fairly large generated bindings in a separate module and reduce warnings.
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(improper_ctypes)]
#[allow(dead_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(clippy::ptr_offset_with_cast)]
#[allow(clippy::upper_case_acronyms)]
#[allow(clippy::useless_transmute)]
#[allow(clippy::multiple_unsafe_ops_per_block)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

// We re-export the required bindings only. This list will be extended as needed.
// It allows us to keep the bindings module private while still exposing the necessary functions and types.
pub(crate) use bindings::{
    NewTrieMap, TrieMap, TrieMap_Add, TrieMap_Delete, TrieMap_ExactMemUsage, TrieMap_Find,
    TrieMap_FindPrefixes, TrieMap_Free,
};
// used in outside binary crate (main.rs)
pub use bindings::TRIEMAP_NOTFOUND;
