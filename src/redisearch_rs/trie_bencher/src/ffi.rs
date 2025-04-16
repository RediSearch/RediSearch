//! This module provides FFI bindings for the C trie functions used in the [crate::CTrieMap] implementation.

// We hide the fairly large generated bindings in a separate module and reduce warnings.
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(improper_ctypes)]
#[allow(dead_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(clippy::all)]
#[allow(clippy::multiple_unsafe_ops_per_block)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

// We re-export the required bindings only. This list will be extended as needed.
// It allows us to keep the bindings module private while still exposing the necessary functions and types.
pub(crate) use bindings::NewTrieMap;
pub(crate) use bindings::TrieMap;
pub(crate) use bindings::TrieMap_Add;
pub(crate) use bindings::TrieMap_Delete;
pub(crate) use bindings::TrieMap_ExactMemUsage;
pub(crate) use bindings::TrieMap_Find;
pub(crate) use bindings::TrieMap_Free;
// used in outside binary crate (main.rs)
pub use bindings::TRIEMAP_NOTFOUND;
