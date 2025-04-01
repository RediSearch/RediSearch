//! Supporting types and functions for benchmarking trie operations.
use std::{
    ffi::{CString, c_char, c_void},
    ptr::NonNull,
};

pub use bencher::OperationBencher;
pub use corpus::download_or_read_corpus;

mod bencher;
mod corpus;
pub mod ffi;
mod redis_allocator;

// Convenient aliases for the trie types that are being benchmarked.
pub type RustNewTrie = trie_rs::trie_new::TrieMap<NonNull<c_void>>;
pub type RustTrieMap = trie_rs::trie::TrieMap<NonNull<c_void>>;
pub type RustRadixTrie = radix_trie::Trie<Vec<u8>, NonNull<c_void>>;
pub type CTrieMap = crate::ffi::TrieMap;

/// Convert a string to a vector of `c_char`.
pub fn str2c_char(input: &str) -> Box<[c_char]> {
    let c_string = CString::new(input).expect("CString conversion failed");
    c_string
        .as_bytes_with_nul()
        .iter()
        .map(|&b| b as c_char)
        .collect()
}

/// Convert a string to a vector of `u8`.
pub fn str2u8(input: &str) -> Vec<u8> {
    let c_string = CString::new(input).expect("CString conversion failed");
    c_string.as_bytes_with_nul().iter().copied().collect()
}
