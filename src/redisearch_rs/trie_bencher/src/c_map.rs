use std::ffi::{c_char, c_void};

#[repr(transparent)]
/// A thin wrapper around the C TrieMap implementation to ensure that the map is properly initialized and cleaned up.
pub struct CTrieMap(*mut crate::ffi::TrieMap);

impl CTrieMap {
    pub fn new() -> Self {
        Self(unsafe { crate::ffi::NewTrieMap() })
    }

    pub fn insert(&mut self, word: *mut c_char, len: u16) -> i32 {
        unsafe {
            crate::ffi::TrieMap_Add(self.0, word, len, std::ptr::null_mut(), Some(do_nothing))
        }
    }

    pub fn find(&self, word: *mut c_char, len: u16) -> *mut c_void {
        unsafe { crate::ffi::TrieMap_Find(self.0, word, len) }
    }

    pub fn remove(&mut self, word: *mut c_char, len: u16) -> i32 {
        unsafe { crate::ffi::TrieMap_Delete(self.0, word, len, Some(do_not_free)) }
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
