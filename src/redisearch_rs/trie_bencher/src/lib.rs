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

pub struct TrieInputView<'a> {
    data: &'a CString,
}

impl<'a> TrieInputView<'a> {
    pub fn ptr(&self) -> *mut c_char {
        self.data.as_ptr() as *mut c_char
    }

    pub fn len(&self) -> u16 {
        self.data.as_bytes().len() as u16
    }
}

pub trait AsCstr {
    fn as_cstr(&self) -> CString;
}

pub trait AsTrieInput {
    fn as_trie_input(&self) -> TrieInputView;
}

impl AsCstr for &str {
    fn as_cstr(&self) -> CString {
        CString::new(*self).expect("CString conversion failed")
    }
}

impl AsCstr for String {
    fn as_cstr(&self) -> CString {
        CString::new(self.as_str()).expect("CString conversion failed")
    }
}

impl AsTrieInput for CString {
    fn as_trie_input(&self) -> TrieInputView {
        TrieInputView {
            data: self,
        }
    }
}