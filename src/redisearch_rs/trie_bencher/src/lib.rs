//! Supporting types and functions for benchmarking trie operations.
use std::{ffi::{c_char, c_void}, ptr::NonNull};

pub use bencher::OperationBencher;

pub mod bencher;
pub mod corpus;
pub mod ffi;
pub mod cmap;
mod redis_allocator;

// Convenient aliases for the trie types that are being benchmarked.
pub use cmap::CTrieMap;
pub type RustTrieMap = trie_rs::trie::TrieMap<NonNull<c_void>>;

/// Convert a string to a slice of `c_char`, allocated on the heap, which is the expected input for [crate::RustTrieMap].
pub fn str2boxed_c_char(input: &str) -> Box<[c_char]> {
    input.as_bytes().iter().map(|&b| b as c_char).collect()
}