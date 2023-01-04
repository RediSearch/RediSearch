use crate::{
    matches_prefixes_iterator::MatchesPrefixesIterator, sub_trie_iterator::SubTrieIterator,
    trie::Node, trie::Trie, trie_iter::TrieIterator,
};
use core::slice;
use std::ffi::{c_char, c_void};

type FreeFunc = Option<extern "C" fn(*mut c_void)>;

#[no_mangle]
pub extern "C" fn RS_NewTrieMap() -> *mut Trie<*mut c_void> {
    Box::into_raw(Box::new(Trie::new()))
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Free(t: *mut Trie<*mut c_void>, free_func: FreeFunc) {
    let t = unsafe { Box::from_raw(t) };
    if let Some(free_func) = free_func {
        t.into_iter().for_each(|d| free_func(d));
    }
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Size(t: *mut Trie<*mut c_void>) -> usize {
    let t = unsafe { &mut *t };
    t.len()
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Add(
    t: *mut Trie<*mut c_void>,
    str: *const c_char,
    len: usize,
    data: *mut c_void,
) -> *mut c_void {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    t.add(key, data).unwrap_or_else(std::ptr::null_mut)
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Delete(
    t: *mut Trie<*mut c_void>,
    str: *const c_char,
    len: usize,
) -> *mut c_void {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    t.del(key).unwrap_or_else(std::ptr::null_mut)
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Get(
    t: *mut Trie<*mut c_void>,
    str: *const c_char,
    len: usize,
) -> *mut c_void {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    t.get(key).copied().unwrap_or_else(std::ptr::null_mut)
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Find(
    t: *mut Trie<*mut c_void>,
    str: *const c_char,
    len: usize,
) -> *mut SubTrieIterator<'static, *mut c_void> {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    Box::into_raw(Box::new(t.find(key)))
}

#[no_mangle]
pub extern "C" fn RS_SubTrieIterator_Next(
    iter: *mut SubTrieIterator<'static, *mut c_void>,
    key: *mut *const c_char,
    size: *mut usize,
    data: *mut *const c_void,
) -> bool {
    let iter = unsafe { &mut *iter };
    let (k, v) = match iter.next() {
        Some(v) => v,
        None => return false,
    };
    unsafe { *key = k.as_ptr() as *const i8 };
    unsafe { *size = k.len() };
    unsafe { *data = *v };
    true
}

#[no_mangle]
pub extern "C" fn RS_SubTrieIterator_Free(iter: *mut SubTrieIterator<'static, *mut c_void>) {
    unsafe { Box::from_raw(iter) };
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_FindPrefixes(
    t: *mut Trie<*mut c_void>,
    str: *const c_char,
    len: usize,
) -> *mut MatchesPrefixesIterator<'static, *mut c_void> {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    Box::into_raw(Box::new(t.find_matches_prefixes(key)))
}

#[no_mangle]
pub extern "C" fn RS_MatchesPrefixesIterator_Next(
    iter: *mut MatchesPrefixesIterator<'static, *mut c_void>,
    key: *mut *const c_char,
    size: *mut usize,
    data: *mut *const c_void,
) -> bool {
    let iter = unsafe { &mut *iter };
    let (k, v) = match iter.next() {
        Some(v) => v,
        None => return false,
    };
    unsafe { *key = k.as_ptr() as *const i8 };
    unsafe { *size = k.len() };
    unsafe { *data = *v };
    true
}

#[no_mangle]
pub extern "C" fn RS_MatchesPrefixesIterator_Free(
    iter: *mut MatchesPrefixesIterator<'static, *mut c_void>,
) {
    unsafe { Box::from_raw(iter) };
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_MemUsage(t: *mut Trie<*mut c_void>) -> usize {
    // todo: come up with better esstimation.
    let t = unsafe { &mut *t };
    t.n_nodes() * std::mem::size_of::<Node<*mut c_void>>()
}
