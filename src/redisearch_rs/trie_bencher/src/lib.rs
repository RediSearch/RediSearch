//! Supporting types and functions for benchmarking trie operations.
use std::{
    alloc::{Layout, dealloc},
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

/// Stores an input string for the C trie map.
///
/// The String isn't null terminated therefore we store the length and
/// implment the `Drop` trait to free the memory when the struct goes out of scope.
///
/// This encapsulates the expected input shape for insertions and retrievals on [`CTrieMap`].
pub struct StrCInput {
    pub cstr_ptr: *mut c_char,
    pub c_len: u16,
}

impl StrCInput {
    pub fn new(input: &str) -> Self {
        let converted = CString::new(input).expect("CString conversion failed");
        let len: u16 = converted.as_bytes().len().try_into().unwrap();
        Self {
            cstr_ptr: converted.into_raw() as *mut c_char,
            c_len: len,
        }
    }
}

impl Drop for StrCInput {
    fn drop(&mut self) {
        // free the memory allocated for the C string
        unsafe {
            dealloc(
                self.cstr_ptr as *mut u8,
                Layout::from_size_align(self.c_len as usize, std::mem::align_of::<usize>())
                    .unwrap(),
            )
        }
    }
}
