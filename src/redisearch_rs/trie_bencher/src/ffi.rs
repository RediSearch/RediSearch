///! This module provides FFI bindings for the C trie functions encapsulated in the [CTrieMap] implementation.
///!
///! It adds the [TrieInputView] struct, which provides a view into a `CString` used by [CTrieMap].
///! It also provides the [ToCstr] and [AsTrieView] traits to convert to a `CString` and provide a view on the raw contents of a `CString`.
use std::ffi::{CString, c_char, c_void};

#[repr(transparent)]
/// A thin wrapper around the C TrieMap implementation to ensure that the map is properly initialized and cleaned up.
pub struct CTrieMap(*mut crate::ffi::TrieMap);

impl CTrieMap {
    pub fn new() -> Self {
        Self(unsafe { crate::ffi::NewTrieMap() })
    }

    pub fn insert(&mut self, term: &TrieTermView) -> i32 {
        unsafe {
            crate::ffi::TrieMap_Add(
                self.0,
                term.ptr(),
                term.len(),
                std::ptr::null_mut(),
                Some(do_nothing),
            )
        }
    }

    pub fn find(&self, term: &TrieTermView) -> *mut c_void {
        unsafe { crate::ffi::TrieMap_Find(self.0, term.ptr(), term.len()) }
    }

    pub fn remove(&mut self, term: &TrieTermView) -> i32 {
        unsafe { crate::ffi::TrieMap_Delete(self.0, term.ptr(), term.len(), Some(do_not_free)) }
    }

    pub fn n_nodes(&self) -> usize {
        unsafe { (*self.0).size }
    }

    /// Returns the exact memory usage of the TrieMap in bytes.
    pub fn mem_usage(&self) -> usize {
        unsafe { crate::ffi::TrieMap_ExactMemUsage(self.0) }
    }
}

impl Drop for CTrieMap {
    fn drop(&mut self) {
        unsafe {
            crate::ffi::TrieMap_Free(self.0, Some(do_not_free));
        }
    }
}

unsafe extern "C" fn do_nothing(oldval: *mut c_void, _newval: *mut c_void) -> *mut c_void {
    // Just return the old value, since it's a null pointer and we don't want
    // the C map implementation to try to free it.
    oldval
}

unsafe extern "C" fn do_not_free(_val: *mut c_void) {
    // We're using the null pointer as value, so we don't want to free it.
}

/// Provides a view for a trie term into a CString used for passing to C trie functions in [CTrieMap].
pub struct TrieTermView<'a> {
    data: &'a CString,
}

impl<'a> TrieTermView<'a> {
    /// access to the char pointer
    pub fn ptr(&self) -> *mut c_char {
        self.data.as_ptr() as *mut c_char
    }

    /// the len of the string
    pub fn len(&self) -> u16 {
        self.data.as_bytes().len() as u16
    }
}

/// Extension trait to convert to CString.
pub trait ToCstr {
    /// Convert the implementing type to a `CString`.
    fn to_cstr(&self) -> CString;
}

/// Extension trait to provide that uses a view on a `CString`.
/// This is useful for passing the string to C functions that expect a pointer and a len.
pub trait AsTrieTermView {
    /// Provides a view on the raw contents of a CString
    fn as_view(&self) -> TrieTermView;
}

/// Implements `ToCstr` for any type that can be viewed as a string slice.
///
/// This blanket implementation allows any string-like type to be converted to a `CString`,
/// which is useful for FFI operations.
impl<T: AsRef<str>> ToCstr for T {
    fn to_cstr(&self) -> CString {
        CString::new(self.as_ref()).expect("CString conversion failed")
    }
}

/// Implements `AsTrieView` for `CString`.
impl AsTrieTermView for CString {
    fn as_view(&self) -> TrieTermView {
        TrieTermView { data: self }
    }
}

/// Convert a string to a slice of `c_char`, allocated on the heap, which is the expected input for [crate::RustTrieMap].
pub fn str2boxed_c_char(input: &str) -> Box<[c_char]> {
    input
        .as_bytes()
        .iter()
        .map(|&b| b as c_char)
        .collect()
}


// ---
// Here follow the generated bindings from the C library.
// ---

// We hide the fairly large generated bindings in a separate module and
// reduce warnings.
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(improper_ctypes)]
#[allow(dead_code)]
#[allow(unsafe_op_in_unsafe_fn)]
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
