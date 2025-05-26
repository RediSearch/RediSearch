/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Implementation of [CTrieMap] which wraps the C TrieMap implementation and helper types for data conversion.
//!
//! The [TrieTermView] struct provides a view into a `CString` used by [CTrieMap]. Ensuring Rust ownership of the data
//! and providing access to the raw pointer and length of the string as needed by the C API.
use crate::ffi::{array_free, array_new_sz};
use lending_iterator::prelude::*;
use std::ffi::{CStr, CString, c_char, c_void};

#[repr(transparent)]
/// A thin wrapper around the C TrieMap implementation to ensure that the map is properly initialized and cleaned up.
pub struct CTrieMap(*mut crate::ffi::TrieMap);

#[allow(clippy::undocumented_unsafe_blocks)]
#[allow(clippy::new_without_default)]
impl CTrieMap {
    pub fn new() -> Self {
        Self(unsafe { crate::ffi::NewTrieMap() })
    }

    pub fn insert(&mut self, term: TrieTermView) -> i32 {
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

    pub fn find(&self, term: TrieTermView) -> *mut c_void {
        unsafe { crate::ffi::TrieMap_Find(self.0, term.ptr(), term.len()) }
    }

    pub fn remove(&mut self, term: TrieTermView) -> i32 {
        unsafe { crate::ffi::TrieMap_Delete(self.0, term.ptr(), term.len(), Some(do_not_free)) }
    }

    pub fn find_prefixes(&self, term: TrieTermView) -> PrefixesValues {
        let mut results = {
            // Here we are emulating the behaviour of the `array_new` macro, which we can't
            // invoke directly on the Rust side.
            let raw = unsafe { array_new_sz(std::mem::size_of::<*mut c_void>() as u32, 1, 0) };
            raw as *mut *mut c_void
        };
        let _n_results = unsafe {
            crate::ffi::TrieMap_FindPrefixes(self.0, term.ptr(), term.len(), &mut results)
        };
        PrefixesValues(results)
    }

    pub fn wildcard_iter(&self, pattern: TrieTermView, fixed_length: bool) -> WildcardCIter {
        let iter = unsafe { crate::ffi::TrieMap_Iterate(self.0, pattern.ptr(), pattern.len()) };
        (unsafe { *iter }).mode = if fixed_length {
            crate::ffi::tm_iter_mode_TM_WILDCARD_FIXED_LEN_MODE
        } else {
            crate::ffi::tm_iter_mode_TM_WILDCARD_MODE
        };
        WildcardCIter {
            iterator: iter,
            finished: false,
        }
    }

    pub fn contains_iter(&self, target: TrieTermView) -> ContainsCIter {
        let iter = unsafe { crate::ffi::TrieMap_Iterate(self.0, target.ptr(), target.len()) };
        (unsafe { *iter }).mode = crate::ffi::tm_iter_mode_TM_CONTAINS_MODE;
        ContainsCIter {
            iterator: iter,
            finished: false,
        }
    }

    pub fn range_iter(
        &self,
        min: Option<TrieTermView>,
        max: Option<TrieTermView>,
        include_min: bool,
        include_max: bool,
    ) {
        let min_ptr = min.map(|m| m.ptr()).unwrap_or(std::ptr::null_mut());
        let min_len = min.map(|m| m.len()).unwrap_or(0);
        let max_ptr = max.map(|m| m.ptr()).unwrap_or(std::ptr::null_mut());
        let max_len = max.map(|m| m.len()).unwrap_or(0);
        unsafe {
            crate::ffi::TrieMap_IterateRange(
                self.0,
                min_ptr,
                min_len.into(),
                include_min,
                max_ptr,
                max_len.into(),
                include_max,
                Some(do_nothing_callback),
                std::ptr::null_mut(),
            )
        };
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
        // Safety: The C library is responsible for freeing the memory.
        unsafe {
            crate::ffi::TrieMap_Free(self.0, Some(do_not_free));
        }
    }
}

/// The values attached to the prefixes retrieved by [`CTrieMap::find_prefixes`].
pub struct PrefixesValues(*mut *mut c_void);

impl Drop for PrefixesValues {
    fn drop(&mut self) {
        unsafe { array_free(self.0 as *mut c_void) };
    }
}

pub struct WildcardCIter {
    iterator: *mut crate::ffi::TrieMapIterator,
    finished: bool,
}

#[gat]
// The 'tm lifetime parameter is not actually needless.
#[allow(clippy::needless_lifetimes)]
impl LendingIterator for WildcardCIter {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], *mut c_void);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.finished {
            return None;
        }

        let mut ptr: *mut c_char = std::ptr::null_mut();
        let mut len = 0;
        let mut value: *mut c_void = std::ptr::null_mut();
        let should_continue = unsafe {
            crate::ffi::TrieMapIterator_NextWildcard(self.iterator, &mut ptr, &mut len, &mut value)
        };

        if should_continue != 1 {
            self.finished = false;
        }
        let key: &[u8] = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
        Some((key, value))
    }
}

impl Drop for WildcardCIter {
    fn drop(&mut self) {
        unsafe { crate::ffi::TrieMapIterator_Free(self.iterator) };
    }
}

pub struct ContainsCIter {
    iterator: *mut crate::ffi::TrieMapIterator,
    finished: bool,
}

#[gat]
// The 'tm lifetime parameter is not actually needless.
#[allow(clippy::needless_lifetimes)]
impl LendingIterator for ContainsCIter {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], *mut c_void);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.finished {
            return None;
        }

        let mut ptr: *mut c_char = std::ptr::null_mut();
        let mut len = 0;
        let mut value: *mut c_void = std::ptr::null_mut();
        let should_continue = unsafe {
            crate::ffi::TrieMapIterator_NextContains(self.iterator, &mut ptr, &mut len, &mut value)
        };

        if should_continue != 1 {
            self.finished = false;
        }
        let key: &[u8] = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
        Some((key, value))
    }
}

impl Drop for ContainsCIter {
    fn drop(&mut self) {
        unsafe { crate::ffi::TrieMapIterator_Free(self.iterator) };
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

unsafe extern "C" fn do_nothing_callback(
    _arg1: *const ::std::os::raw::c_char,
    _arg2: usize,
    _arg3: *mut ::std::os::raw::c_void,
    _arg4: *mut ::std::os::raw::c_void,
) {
    // A callback for `TrieMap_IterateRange` that, as the name implies, does nothing with its input.
}

#[allow(clippy::len_without_is_empty)]
#[derive(Copy, Clone)]
/// Provides a view for a trie term into a CString used for passing to C trie functions in [CTrieMap].
pub struct TrieTermView<'a> {
    data: &'a CStr,
}

impl TrieTermView<'_> {
    /// access to the char pointer
    pub fn ptr(&self) -> *mut c_char {
        self.data.as_ptr() as *mut c_char
    }

    /// the len of the string
    pub fn len(&self) -> u16 {
        self.data.to_bytes().len() as u16
    }
}

/// Extension trait to convert to CString.
pub trait IntoCString {
    /// Convert the implementing type to a `CString`.
    fn into_cstring(self) -> CString;
}

/// Extension trait to provide that uses a view on a `CString`.
/// This is useful for passing the string to C functions that expect a pointer and a len.
pub trait AsTrieTermView {
    /// Provides a view on the data for the c-side
    fn as_view(&self) -> TrieTermView;
}

/// Implements [into_cstring] for any type that can be viewed as a string slice.
///
/// This blanket implementation allows any string-like type to be converted to a `CString`,
/// which is useful for FFI operations.
///
/// Panics if the argument contains a null byte.
impl<T: AsRef<[u8]>> IntoCString for T {
    fn into_cstring(self) -> CString {
        CString::new(self.as_ref()).expect("null byte found")
    }
}

/// Implements `AsTrieView` for `CString`.
impl AsTrieTermView for CString {
    fn as_view(&self) -> TrieTermView {
        TrieTermView { data: self }
    }
}
