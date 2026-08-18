/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The value-iteration entry points, driven through their C out-parameter
//! protocol: what `*value` carries, and that the walk terminates.

use std::ffi::c_char;

use tag_index_ffi::{
    Rust_TagIndex_IterateSuffix, Rust_TagIndex_IterateValues, Rust_TagIndex_ValueIterator_Free,
    Rust_TagIndex_ValueIterator_Next, Rust_TagIndex_ValueIterator_NextKey,
    Rust_TagIndexValue_NumDocs, Rust_TagIndexValue_UniqueId, TagIndexValue, ValueIterator,
};
use triemap_ffi::tm_len_t;

use crate::handle::{index_and_commit, new_in_memory};

/// Drain `iter` through `Rust_TagIndex_ValueIterator_Next`, collecting each tag
/// with the number of documents its posting list holds.
///
/// # Safety
///
/// `iter` must be a live iterator; it is freed here.
unsafe fn drain(iter: *mut ValueIterator) -> Vec<(Vec<u8>, u32)> {
    let mut out = Vec::new();
    let mut ptr: *mut c_char = std::ptr::null_mut();
    let mut len: tm_len_t = 0;
    let mut value: *mut TagIndexValue = std::ptr::null_mut();

    // SAFETY: the caller guarantees `iter` is live, and the out-params are all
    // valid locals.
    while unsafe {
        Rust_TagIndex_ValueIterator_Next(iter, &raw mut ptr, &raw mut len, &raw mut value)
    } != 0
    {
        assert!(!value.is_null(), "memory mode yields the posting list");
        // SAFETY: the iterator wrote `len` readable bytes at `ptr`, valid until
        // the next call — which is why the key is copied here.
        let key = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) }.to_vec();
        // SAFETY: `value` is the live posting list the iterator just yielded.
        out.push((key, unsafe { Rust_TagIndexValue_NumDocs(value) }));
    }

    // SAFETY: the caller transfers ownership of `iter` here.
    unsafe { Rust_TagIndex_ValueIterator_Free(iter) };
    out
}

#[test]
fn iterate_values_walks_every_tag_in_order() {
    let idx = new_in_memory(false);
    index_and_commit(idx, &["red", "blue"], 1);
    index_and_commit(idx, &["red"], 2);

    // SAFETY: `idx` is live and outlives the iterator.
    let entries = unsafe { drain(Rust_TagIndex_IterateValues(idx)) };

    assert_eq!(
        entries,
        vec![(b"blue".to_vec(), 1), (b"red".to_vec(), 2)],
        "tags come back in lexicographical order, each with its document count"
    );

    free(idx);
}

#[test]
fn iterating_an_empty_index_yields_nothing() {
    let idx = new_in_memory(false);

    // SAFETY: `idx` is live and outlives the iterator.
    let entries = unsafe { drain(Rust_TagIndex_IterateValues(idx)) };

    assert!(entries.is_empty());

    free(idx);
}

#[test]
fn next_key_walks_the_same_tags() {
    let idx = new_in_memory(false);
    index_and_commit(idx, &["red", "blue", "green"], 1);

    // SAFETY: `idx` is live and outlives the iterator.
    let iter = unsafe { Rust_TagIndex_IterateValues(idx) };
    let mut keys = Vec::new();
    let mut ptr: *mut c_char = std::ptr::null_mut();
    let mut len: tm_len_t = 0;

    // SAFETY: `iter` is live and the out-params are valid locals.
    while unsafe { Rust_TagIndex_ValueIterator_NextKey(iter, &raw mut ptr, &raw mut len) } != 0 {
        // SAFETY: as in `drain` — the key is copied before the next call.
        keys.push(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) }.to_vec());
    }
    // SAFETY: ownership transfers here.
    unsafe { Rust_TagIndex_ValueIterator_Free(iter) };

    assert_eq!(
        keys,
        vec![b"blue".to_vec(), b"green".to_vec(), b"red".to_vec()]
    );

    free(idx);
}

#[test]
fn unique_ids_distinguish_two_tags_posting_lists() {
    let idx = new_in_memory(false);
    index_and_commit(idx, &["red", "blue"], 1);

    // SAFETY: `idx` is live and outlives the iterator.
    let iter = unsafe { Rust_TagIndex_IterateValues(idx) };
    let mut ptr: *mut c_char = std::ptr::null_mut();
    let mut len: tm_len_t = 0;
    let mut value: *mut TagIndexValue = std::ptr::null_mut();
    let mut ids = Vec::new();

    // SAFETY: `iter` is live and the out-params are valid locals.
    while unsafe {
        Rust_TagIndex_ValueIterator_Next(iter, &raw mut ptr, &raw mut len, &raw mut value)
    } != 0
    {
        // SAFETY: `value` is the live posting list the iterator just yielded.
        ids.push(unsafe { Rust_TagIndexValue_UniqueId(value) });
    }
    // SAFETY: ownership transfers here.
    unsafe { Rust_TagIndex_ValueIterator_Free(iter) };

    assert_eq!(ids.len(), 2);
    assert_ne!(
        ids[0], ids[1],
        "the fork GC tells posting lists apart by this id, not by address"
    );

    free(idx);
}

#[test]
fn iterate_suffix_is_null_without_a_suffix_trie() {
    let idx = new_in_memory(false);

    // SAFETY: `idx` is live.
    let iter = unsafe { Rust_TagIndex_IterateSuffix(idx) };

    assert!(
        iter.is_null(),
        "an index built without WITHSUFFIXTRIE has no suffix entries to walk"
    );

    free(idx);
}

#[test]
fn iterate_suffix_walks_every_suffix() {
    let idx = new_in_memory(true);
    index_and_commit(idx, &["red", "blue"], 1);

    // SAFETY: `idx` is live and outlives the iterator.
    let iter = unsafe { Rust_TagIndex_IterateSuffix(idx) };
    assert!(!iter.is_null());

    let mut ptr: *mut c_char = std::ptr::null_mut();
    let mut len: tm_len_t = 0;
    let mut value: *mut TagIndexValue = std::ptr::null_mut();
    let mut keys: Vec<Vec<u8>> = Vec::new();

    // SAFETY: `iter` is live and the out-params are valid locals.
    while unsafe {
        Rust_TagIndex_ValueIterator_Next(iter, &raw mut ptr, &raw mut len, &raw mut value)
    } != 0
    {
        assert!(
            value.is_null(),
            "suffix entries carry no posting list of their own"
        );
        // SAFETY: as in `drain` — the key is copied before the next call.
        keys.push(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) }.to_vec());
    }
    // SAFETY: ownership transfers here.
    unsafe { Rust_TagIndex_ValueIterator_Free(iter) };

    // The suffix index is keyed by every suffix of every tag — that is what lets
    // `*foo` and `*foo*` queries avoid scanning the whole tag trie.
    let expected: Vec<Vec<u8>> = [
        &b"blue"[..],
        &b"d"[..],
        &b"e"[..],
        &b"ed"[..],
        &b"lue"[..],
        &b"red"[..],
        &b"ue"[..],
    ]
    .iter()
    .map(|s| s.to_vec())
    .collect();
    assert_eq!(keys, expected);

    free(idx);
}

#[test]
fn freeing_a_null_iterator_is_a_no_op() {
    // `Rust_TagIndex_IterateSuffix` returns NULL for an index without a suffix
    // trie, and C frees it unconditionally.
    //
    // SAFETY: NULL is explicitly allowed.
    unsafe { Rust_TagIndex_ValueIterator_Free(std::ptr::null_mut()) };
}

fn free(idx: *mut tag_index_ffi::RustTagIndex) {
    let mut slot = idx;
    // SAFETY: `slot` holds a live handle.
    unsafe { tag_index_ffi::Rust_TagIndex_Free(&raw mut slot) };
}
