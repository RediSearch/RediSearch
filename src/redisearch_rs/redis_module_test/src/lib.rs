use std::ffi::{CString, c_char};

pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(improper_ctypes)]
    #![allow(dead_code)]
    #![allow(unsafe_op_in_unsafe_fn)]
    #![allow(clippy::useless_transmute)]
    #![allow(clippy::missing_safety_doc)]
    #![allow(clippy::multiple_unsafe_ops_per_block)]
    #![allow(clippy::undocumented_unsafe_blocks)]
    #![allow(clippy::ptr_offset_with_cast)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub mod ffi_polyfill;

/// Convert a string to a slice of `c_char`, allocated on the heap.
pub fn str2c_char(input: &str) -> Box<[c_char]> {
    let c_string = CString::new(input).expect("CString conversion failed");
    c_string.as_bytes().iter().map(|&b| b as c_char).collect()
}

/// Convert a string to a vector of `u8`.
pub fn str2u8(input: &str) -> Vec<u8> {
    let c_string = CString::new(input).expect("CString conversion failed");
    c_string.as_bytes().to_vec()
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
