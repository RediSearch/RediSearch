//! Supporting types and functions for benchmarking trie operations.
use std::{
    ffi::{CString, c_char, c_void},
    ptr::NonNull,
};

pub use bencher::OperationBencher;

pub mod bencher;
mod c_map;
pub mod corpus;
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

pub fn strvec2_raw_words(strings: &Vec<String>) -> (Vec<Box<[i8]>>, Vec<(u16, *mut i8)>) {
    // generate null terminated strings for rust trie map
    let c_char_words = strings
        .iter()
        .map(|str| str2c_char(&str))
        .collect::<Vec<_>>();

    // generate non-terminated c-like strings for c trie map
    let c_words = strings
        .iter()
        .map(|s| {
            let cstr = CString::new(s.as_str()).expect("String conversion failed");
            let len: u16 = cstr.as_bytes_with_nul().len().try_into().unwrap();
            let converted = cstr.into_raw();
            (len, converted)
        })
        .collect::<Vec<_>>();

    assert_eq!(c_char_words.len(), c_words.len());
    (c_char_words, c_words)
}
