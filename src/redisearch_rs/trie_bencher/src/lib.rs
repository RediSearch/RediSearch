//! Supporting types and functions for benchmarking trie operations.
use std::{
    ffi::{CString, c_char, c_void},
    ptr::NonNull,
};

pub use bencher::OperationBencher;
pub use corpus::download_or_read_corpus;

mod bencher;
mod c_map;
mod corpus;
pub mod ffi;
mod redis_allocator;

// Convenient aliases for the trie types that are being benchmarked.
pub use c_map::CTrieMap;
pub type RustTrieMap = trie_rs::trie::TrieMap<NonNull<c_void>>;
pub type RustRadixTrie = radix_trie::Trie<Vec<u8>, NonNull<c_void>>;

/// Convert a string to a slice of `c_char`, allocated on the heap.
pub fn str2c_char(input: &str) -> Box<[c_char]> {
    let c_string = CString::new(input).expect("CString conversion failed");
    c_string.as_bytes().iter().map(|&b| b as c_char).collect()
}

/// Convert a string to a vector of `u8`.
pub fn str2u8(input: &str) -> Vec<u8> {
    let c_string = CString::new(input).expect("CString conversion failed");
    c_string.as_bytes().iter().copied().collect()
}

/// Convert a string to a pointer to a `c_char` array.
/// It also returns the length of the string.
///
/// This is the expected input shape for insertions and retrievals on [`CTrieMap`].
pub fn str2c_input(input: &str) -> (*mut c_char, u16) {
    let converted = CString::new(input).expect("CString conversion failed");
    let len: u16 = converted.as_bytes().len().try_into().unwrap();
    (converted.into_raw(), len)
}
