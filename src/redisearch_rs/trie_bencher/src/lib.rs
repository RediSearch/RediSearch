//! Supporting types and functions for benchmarking trie operations.
use std::{
    ffi::{c_char, c_void},
    ptr::NonNull,
};

// Force the compiler to link the symbols defined in `redis_mock`,
// since they are required by `libtrie.a`.
extern crate redis_mock;

pub use bencher::OperationBencher;

pub mod bencher;
pub mod c_map;
pub mod corpus;
pub mod ffi;

// Convenient aliases for the trie types that are being benchmarked.
pub use c_map::CTrieMap;
pub type RustTrieMap = trie_rs::TrieMap<NonNull<c_void>>;

/// Convert a string to a slice of `c_char`, allocated on the heap, which is the expected input for [crate::RustTrieMap].
pub fn str2boxed_c_char(input: &str) -> Box<[c_char]> {
    input.as_bytes().iter().map(|&b| b as c_char).collect()
}
