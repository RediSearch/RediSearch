use crate::{
    matches_prefixes_iterator::MatchesPrefixesIterator, sub_trie_iterator::SubTrieIterator,
    trie::Node, trie::Trie, trie_iter::TrieIterator, wildcard_trie_iterator::WildcardTrieIterator,
    range_trie_iterator::RangeTrieIterator,
};
use core::slice;
use std::ffi::{c_char, c_void};
use std::ptr::NonNull;

type FreeFunc = Option<extern "C" fn(*mut c_void)>;

#[no_mangle]
pub extern "C" fn RS_NewTrieMap() -> *mut Trie<NonNull<c_void>> {
    Box::into_raw(Box::new(Trie::new()))
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Free(t: *mut Trie<NonNull<c_void>>, free_func: FreeFunc) {
    let t = unsafe { Box::from_raw(t) };
    if let Some(free_func) = free_func {
        t.into_iter().for_each(|d| free_func(d.as_ptr()));
    }
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Size(t: *mut Trie<NonNull<c_void>>) -> usize {
    let t = unsafe { &mut *t };
    t.len()
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Add(
    t: *mut Trie<NonNull<c_void>>,
    str: *const c_char,
    len: usize,
    data: NonNull<c_void>,
) -> *mut c_void {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    t.add(key, data).map(|v| v.as_ptr()).unwrap_or_else(std::ptr::null_mut)
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Delete(
    t: *mut Trie<NonNull<c_void>>,
    str: *const c_char,
    len: usize,
) -> *mut c_void {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    t.del(key).map(|v| v.as_ptr()).unwrap_or_else(std::ptr::null_mut)
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Get(
    t: *mut Trie<NonNull<c_void>>,
    str: *const c_char,
    len: usize,
) -> *mut c_void {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    t.get(key).map(|v| v.as_ptr()).unwrap_or_else(std::ptr::null_mut)
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_Find(
    t: *mut Trie<NonNull<c_void>>,
    str: *const c_char,
    len: usize,
) -> *mut SubTrieIterator<'static, NonNull<c_void>> {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    Box::into_raw(Box::new(t.find(key)))
}

#[no_mangle]
pub extern "C" fn RS_SubTrieIterator_Next(
    iter: *mut SubTrieIterator<'static, NonNull<c_void>>,
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
    unsafe { *data = v.as_ptr() };
    true
}

#[no_mangle]
pub extern "C" fn RS_SubTrieIterator_Free(iter: *mut SubTrieIterator<'static, NonNull<c_void>>) {
    unsafe { Box::from_raw(iter) };
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_FindPrefixes(
    t: *mut Trie<NonNull<c_void>>,
    str: *const c_char,
    len: usize,
) -> *mut MatchesPrefixesIterator<'static, NonNull<c_void>> {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    Box::into_raw(Box::new(t.find_matches_prefixes(key)))
}

#[no_mangle]
pub extern "C" fn RS_MatchesPrefixesIterator_Next(
    iter: *mut MatchesPrefixesIterator<'static, NonNull<c_void>>,
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
    unsafe { *data = v.as_ptr() };
    true
}

#[no_mangle]
pub extern "C" fn RS_MatchesPrefixesIterator_Free(
    iter: *mut MatchesPrefixesIterator<'static, NonNull<c_void>>,
) {
    unsafe { Box::from_raw(iter) };
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_FindWildcard(
    t: *mut Trie<NonNull<c_void>>,
    str: *const c_char,
    len: usize,
) -> *mut WildcardTrieIterator<'static, NonNull<c_void>> {
    let t = unsafe { &mut *t };
    let key = unsafe { slice::from_raw_parts(str as *const u8, len) };
    Box::into_raw(Box::new(t.wildcard_search(key)))
}

#[no_mangle]
pub extern "C" fn RS_WildcardIterator_Next(
    iter: *mut WildcardTrieIterator<'static, NonNull<c_void>>,
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
    unsafe { *data = v.as_ptr() };
    true
}

#[no_mangle]
pub extern "C" fn RS_WildcardIterator_Free(iter: *mut WildcardTrieIterator<'static, NonNull<c_void>>) {
    unsafe { Box::from_raw(iter) };
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_FindLexRange(
    t: *mut Trie<NonNull<c_void>>,
    min: *const c_char,
    minlen: usize,
    include_min: bool,
    max: *const c_char,
    maxlen: usize,
    include_max: bool,
) -> *mut RangeTrieIterator<'static, NonNull<c_void>> {
    let t = unsafe { &mut *t };
    let min = if min.is_null() {None} else {unsafe { Some(slice::from_raw_parts(min as *const u8, minlen) )}};
    let max = if max.is_null() {None} else {unsafe { Some(slice::from_raw_parts(max as *const u8, maxlen) )}};
    Box::into_raw(Box::new(t.lex_range(min, include_min, max, include_max)))
}

#[no_mangle]
pub extern "C" fn RS_LexRangeIterator_Next(
    iter: *mut RangeTrieIterator<'static, NonNull<c_void>>,
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
    unsafe { *data = v.as_ptr() };
    true
}

#[no_mangle]
pub extern "C" fn RS_LexRangeIterator_Free(iter: *mut RangeTrieIterator<'static, NonNull<c_void>>) {
    unsafe { Box::from_raw(iter) };
}

#[no_mangle]
pub extern "C" fn RS_TrieMap_MemUsage(t: *mut Trie<NonNull<c_void>>) -> usize {
    // todo: come up with better esstimation.
    let t = unsafe { &mut *t };
    t.n_nodes() * std::mem::size_of::<Node<NonNull<c_void>>>()
}
