/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable bindings for the Rust [`tag_index`] crate.
//!
//! These wrap the safe [`TagIndex`] behind an opaque handle so the C module
//! can create, populate, introspect and tear down a tag index, in **both**
//! storage modes: memory mode keeps the postings inline in the values trie,
//! while disk mode (`diskSpec`, bigredis/Flex) keeps them on disk behind the
//! `SearchDisk_*` API and stores only tag-presence sentinels in the trie. The
//! disk paths (write, read, query expansion, `FT.TAGVALS`) are handled by
//! branching on [`TagIndex::disk_mode`] and delegating to the core crate, which
//! calls into `SearchDisk_*`.
//!
//! This crate is listed in the `redisearch_rs` facade so its `#[no_mangle]`
//! symbols are linked into the module; `tag_index.c` calls them for the paths
//! that have been migrated.

#![allow(non_camel_case_types, non_snake_case)]

use std::{
    ffi::{c_char, c_int, c_longlong, c_void},
    ptr::NonNull,
};

use ffi::{
    DocTable_Exists, QueryError, QueryIterator, RedisModuleCtx, RedisSearchCtx,
    RedisSearchDiskIndexSpec, SearchDiskWriteBatchHandle, t_fieldIndex, timespec,
};
use index_result::RSIndexResult;
use inverted_index::{
    GcApplyInfo, GcScanDelta, InvertedIndex, RepairContext, doc_ids_only::DocIdsOnly,
};
use inverted_index_ffi::fork_gc::{InvertedIndexGCCallback, InvertedIndexGCWriter};
use lending_iterator::LendingIterator as _;
use rqe_core::DocId;
use tag_index::{IterMode, SuffixWildcardPattern, TagIndex, TagValueReader, ValueIterator};
use trie_rs::iter::{RangeBoundary, RangeFilter};
use triemap_ffi::{TrieMapRangeCallback, tm_iter_mode, tm_len_t};

#[unsafe(no_mangle)]
pub extern "C" fn TagIndex2_New(
    id: u32,
    disk_spec: *mut RedisSearchDiskIndexSpec,
    field_index: t_fieldIndex,
    with_suffix: bool,
) -> *mut TagIndex {
    let disk = NonNull::new(disk_spec).map(|ptr| (ptr, field_index));
    let tag_index = TagIndex::new(id, disk, with_suffix);

    Box::into_raw(Box::new(tag_index))
}

/// Return the id that identifies uniquely the given [`TagIndex`]
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TagIndex2_GetId(tag_index: *const TagIndex) -> u32 {
    // Safety: guarantee by 1
    let tag_index = unsafe { &*tag_index };
    tag_index.id()
}

/// Return `true` is the given [`TagIndex`] supports the suffix searches.
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TagIndex2_HasSuffix(tag_index: *const TagIndex) -> bool {
    // Safety: guarantee by 1
    let tag_index = unsafe { &*tag_index };
    tag_index.has_suffix()
}

/// Return `true` is the given [`TagIndex`] is backed by disk.
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TagIndex2_HasDiskSpec(tag_index: *const TagIndex) -> bool {
    // Safety: guarantee by 1
    let tag_index = unsafe { &*tag_index };
    tag_index.disk_mode()
}

/// Return the number of unique values stored in the give [`TagIndex`]
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TagIndex2_NUniqueValues(tag_index: *const TagIndex) -> usize {
    // Safety: guarantee by 1
    let tag_index = unsafe { &*tag_index };
    tag_index.unique_values()
}

/// Create a [`QueryIterator`] over the documents matching the given tag.
///
/// Port of the C `TagIndex_GetIteratorFromTrieMapValue`. In memory mode it
/// reads from the inverted index `ptr` already resolved from the values trie
/// (e.g. during a range iteration). In disk mode `ptr` is the NULL disk
/// sentinel and is ignored: the reader is opened by tag string through the
/// disk API, populating `status` on a disk-creation failure.
///
/// Returns NULL when, in memory mode, `ptr` is NULL or the inverted index holds
/// no documents; or when the disk backend yields no iterator.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`] and must outlive the returned iterator. It must not
///    be mutated while the iterator is in use, except under the standard
///    revalidation protocol (mutations happen while the query is yielded under
///    the write lock, and the iterator's `Revalidate` callback runs before any
///    further read).
/// 2. `tag` must point to `len` readable bytes.
/// 3. In memory mode `ptr`, when non-NULL, must be the `InvertedIndex`
///    currently stored in `tag_index`'s values trie for `tag`. In disk mode it
///    is ignored.
/// 4. `sctx` must be a valid, non-NULL pointer to a
///    [`RedisSearchCtx`] whose `spec` is valid; both must outlive the
///    returned iterator.
/// 5. The caller owns the returned iterator and must free it through its
///    `Free` callback (i.e. `it->Free(it)`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_GetIteratorFromTrieMapValue(
    tag_index: *const TagIndex,
    sctx: *const RedisSearchCtx,
    tag: *const u8,
    len: usize,
    ptr: *const c_void,
    weight: f64,
    field_index: t_fieldIndex,
    status: *mut QueryError,
) -> *mut ffi::QueryIterator {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");

    // SAFETY: 1. guarantees tag_index is valid and non-null.
    let tag_index = unsafe { &*tag_index };
    // SAFETY: 2. guarantees tag points to len readable bytes.
    let tag = unsafe { std::slice::from_raw_parts(tag, len) };
    // SAFETY: 4. guarantees sctx is valid and non-null.
    let sctx = unsafe { NonNull::new_unchecked(sctx.cast_mut()) };

    // Disk mode: `ptr` is the NULL disk sentinel (the trie holds no in-memory
    // value), so ignore it and open the reader by tag string via the disk API
    // — mirroring master's `tagRangeIterCb`/`TagIndex_OpenReader` disk branch.
    if tag_index.disk_mode() {
        // Safety: status is valid
        return unsafe { tag_index.open_reader(sctx, tag, weight, field_index, status) }
            .map_or(std::ptr::null_mut(), NonNull::as_ptr);
    }

    if ptr.is_null() {
        return std::ptr::null_mut();
    }

    // SAFETY: 3. guarantees ptr is the trie's DocIdsOnly inverted index for tag.
    let ii = unsafe { &*ptr.cast::<InvertedIndex<DocIdsOnly>>() };

    // SAFETY: 1., 3. and 4. cover the method's contract (index and sctx/spec
    // outlive the iterator, ii is the trie's current value for tag); 5. hands
    // ownership of the result to the caller.
    unsafe { tag_index.query_iterator_for_value(sctx, tag, ii, weight, field_index) }
}

/// Result of [`TagIndex2_GC`], mirroring the numeric GC's `SingleNodeGcResult`.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TagGcResult {
    /// GC stats to fold into the spec (freed/allocated bytes, entries removed,
    /// block-count delta, ...). Exposed to C as `II_GCScanStats`.
    pub info: GcApplyInfo,
    /// `false` when the delta was stale — the tag was removed, or its inverted
    /// index replaced, since the child scanned it. The fork-GC parent maps this
    /// to `FGC_PARENT_ERROR`. When `false`, `info` is zeroed.
    pub applied: bool,
}

/// Apply a GC scan delta to the inverted index stored for the given tag.
///
/// Returns the [`TagGcResult`] the fork-GC parent needs: the applied stats and
/// whether the delta was applied at all (see [`TagGcResult::applied`]). When the
/// posting list becomes empty the tag is dropped from the values trie and the
/// suffix trie, and the returned stats already account for freeing it.
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`], not in disk mode, with no live references into it.
/// 2. `tag` must point to `len` readable bytes.
/// 3. `delta` must be a valid, non-NULL pointer to a heap-allocated
///    [`GcScanDelta`]. Ownership is transferred to this function (it is consumed
///    on every path, including when the delta turns out to be stale).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_GC(
    tag_index: *mut TagIndex,
    tag: *const u8,
    len: usize,
    value: *const c_void,
    delta: *mut GcScanDelta,
) -> TagGcResult {
    // SAFETY: 1. guarantees tag_index is valid, non-null and unaliased.
    let tag_index = unsafe { &mut *tag_index };
    // SAFETY: 2. guarantees tag points to len readable bytes. The empty-string
    // tag (INDEXEMPTY) arrives as a zero-length buffer whose pointer may be
    // null (e.g. from `FGC_recvBuffer`); `from_raw_parts` forbids a null
    // pointer even for length 0, so map that to an empty slice.
    let tag = if len == 0 {
        &[]
    } else {
        // Safety: guarantee 2.
        unsafe { std::slice::from_raw_parts(tag, len) }
    };

    let value = value as *const InvertedIndex<DocIdsOnly>;
    // SAFETY: 3. guarantees delta is heap-allocated and owned by us now.
    let delta = unsafe { Box::from_raw(delta) };
    let delta = *delta;

    match tag_index.gc(tag, value, delta) {
        Some(info) => TagGcResult {
            info,
            applied: true,
        },
        None => TagGcResult {
            info: GcApplyInfo::default(),
            applied: false,
        },
    }
}

#[repr(transparent)]
pub struct TagIndexValue(InvertedIndex<DocIdsOnly>);

/// Advance a [`ValueIterator`] and yield the next tag together with its value
/// pointer: in memory mode the tag's heap [`InvertedIndex<DocIdsOnly>`] (cast to
/// [`TagIndexValue`]), otherwise NULL (disk and suffix-trie entries carry no
/// in-memory value). Returns 1 when an entry was written, 0 at the end of the
/// iteration or when the timeout is reached.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `iter` must be a valid, non-NULL pointer to a [`ValueIterator`] obtained
///    from one of the `TagIndex2_Iterate*` functions.
/// 2. `ptr`, `len` and `value` must be valid, well-aligned, writable pointers.
///    The key written to `ptr` is invalidated by the next call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_ValueIterator_Next<'tag_index>(
    iter: *mut ValueIterator<'tag_index>,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
    value: *mut *mut TagIndexValue,
) -> c_int {
    // SAFETY: 1. guarantees `iter` is a valid, non-null `ValueIterator`.
    let iter = unsafe { &mut *iter };

    let Some((k, v)) = iter.advance() else {
        return 0;
    };

    // Memory-mode entries yield the tag's inverted index; disk and suffix-trie
    // entries carry no in-memory value, so C sees NULL. `TagIndexValue` is
    // `#[repr(transparent)]` over `InvertedIndex<DocIdsOnly>`, so the cast
    // preserves the address; the shared borrow's provenance is unchanged and
    // the contract is read-only (C must not write through it).
    let value_ptr = match v {
        Some(ii) => std::ptr::from_ref(ii).cast_mut().cast::<TagIndexValue>(),
        None => std::ptr::null_mut(),
    };

    // SAFETY: 2. guarantees `ptr` is a mutable, well-aligned `c_char *` slot.
    unsafe {
        ptr.write(k.as_ptr().cast::<c_char>().cast_mut());
    }
    // SAFETY: 2. guarantees `len` is a mutable, well-aligned `tm_len_t` slot.
    unsafe {
        len.write(k.len() as tm_len_t);
    }
    // SAFETY: 2. guarantees `value` is a mutable, well-aligned `TagIndexValue *`
    // slot.
    unsafe {
        value.write(value_ptr);
    }

    1
}

/// Advance a [`ValueIterator`], yielding only the next tag key (no value). Used
/// by the filter and suffix iterators, whose callers do not read the value
/// pointer. Returns 1 when a key was written, 0 at the end of the iteration or
/// when the timeout is reached.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `iter` must be a valid, non-NULL pointer to a [`ValueIterator`] obtained
///    from one of the `TagIndex2_Iterate*` functions.
/// 2. `ptr` and `len` must be valid, well-aligned, writable pointers. The key
///    written to `ptr` is invalidated by the next call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_ValueIterator_NextKey<'tag_index>(
    iter: *mut ValueIterator<'tag_index>,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
) -> c_int {
    // SAFETY: 1. guarantees `iter` is a valid, non-null `ValueIterator`.
    let iter = unsafe { &mut *iter };

    let Some((k, _)) = iter.advance() else {
        return 0;
    };

    // SAFETY: 2. guarantees `ptr` is a mutable, well-aligned `c_char *` slot.
    unsafe {
        ptr.write(k.as_ptr().cast::<c_char>().cast_mut());
    }
    // SAFETY: 2. guarantees `len` is a mutable, well-aligned `tm_len_t` slot.
    unsafe {
        len.write(k.len() as tm_len_t);
    }

    1
}

/// Set the timeout deadline used while iterating (affix queries). It is checked
/// in [`TagIndex2_ValueIterator_Next`] / [`TagIndex2_ValueIterator_NextKey`],
/// which return 0 once it is reached. A zero timeout (`0/0`) clears it.
///
/// # Safety
///
/// `iter` must be a valid, non-NULL pointer to a [`ValueIterator`] obtained from
/// one of the `TagIndex2_Iterate*` functions.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_ValueIterator_SetTimeout(
    iter: *mut ValueIterator,
    timeout: timespec,
) {
    debug_assert!(!iter.is_null(), "iter must not be null");

    // SAFETY: the caller guarantees `iter` is a valid, non-null `ValueIterator`.
    let iter = unsafe { &mut *iter };

    iter.set_timeout(timeout);
}

/// C-facing handle wrapping the crate's [`TagValueReader`]. Kept as a distinct
/// `#[repr(Rust)]` newtype so the generated C header keeps naming this opaque
/// type `TagIndexValueIter`.
pub struct TagIndexValueIter<'trie>(TagValueReader<'trie>);

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndexValue_Iter<'trie>(
    tag_index_value: *const TagIndexValue,
) -> *mut TagIndexValueIter<'trie> {
    debug_assert!(!tag_index_value.is_null(), "tag_index must not be null");

    // SAFETY: 1. guarantees tag_index is valid, non-null, and outlives the
    // returned iterator.
    let tag_index_value: &'trie TagIndexValue = unsafe { &*tag_index_value };

    Box::into_raw(Box::new(TagIndexValueIter(TagValueReader::new(
        &tag_index_value.0,
    ))))
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndexValueIter_Next<'trie>(
    tag_index_value: *mut TagIndexValueIter<'trie>,
    res: *mut RSIndexResult<'trie>,
) -> bool {
    debug_assert!(!tag_index_value.is_null(), "ir must not be null");
    debug_assert!(!res.is_null(), "res must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let tag_index_value = unsafe { &mut *tag_index_value };

    // SAFETY: The caller must ensure that `res` is a valid pointer to a `RSIndexResult`
    let res = unsafe { &mut *res };

    tag_index_value.0.next(res)
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndexValueIter_Free<'trie>(
    tag_index_value: *mut TagIndexValueIter<'trie>,
) {
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );

    // Safety: TODO
    let _ = unsafe { Box::from_raw(tag_index_value) };
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_ValueIterator_Free<'trie>(
    tag_index_iter: *mut ValueIterator<'trie>,
) {
    debug_assert!(!tag_index_iter.is_null(), "tag_index_iter must not be null");

    // Safety: TODO
    let _ = unsafe { Box::from_raw(tag_index_iter) };
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TagIndexValue_NumDocs(tag_index_value: *const TagIndexValue) -> u32 {
    // Safety: TODO
    let tag_index_value = unsafe { &*tag_index_value };

    tag_index_value.0.unique_docs()
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndexValue_NumBlocks(tag_index_value: *const TagIndexValue) -> usize {
    // Safety: TODO
    let tag_index_value = unsafe { &*tag_index_value };

    tag_index_value.0.number_of_blocks()
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndexValue_NumDocs_GcDelta_Scan(
    wr: *mut InvertedIndexGCWriter,
    sctx: *mut RedisSearchCtx,
    idx: *const TagIndexValue,
    cb: *mut InvertedIndexGCCallback,
) -> bool {
    use serde::Serialize;

    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!cb.is_null(), "cb must not be null");
    debug_assert!(!wr.is_null(), "wr must not be null");

    // SAFETY: The caller must ensure `sctx` is a valid pointer to a `RedisSearchCtx`
    let sctx = unsafe { &*sctx };

    debug_assert!(!sctx.spec.is_null(), "sctx.spec must not be null");

    // SAFETY: The caller must ensure the `spec` field of the `RedisSearchCtx` is a valid
    // pointer to an `IndexSpec`
    let spec = unsafe { &*sctx.spec };
    let doc_table = spec.docs;

    // SAFETY: We know `doc_table` is a valid `DocTable` because it just got it off the spec
    let doc_exists = |id| unsafe { DocTable_Exists(&doc_table, id) };

    // SAFETY: The caller must ensure `idx` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*idx };

    let Ok(deltas) = ii.0.scan_gc(
        doc_exists,
        None::<fn(&index_result::RSIndexResult, &RepairContext<'_>)>,
    ) else {
        return false;
    };

    let Some(deltas) = deltas else {
        return false;
    };

    // SAFETY: The caller must ensure `cb` is a valid pointer to an `InvertedIndexGCCallback`
    let cb = unsafe { &*cb };
    let cb_call = cb.call;
    cb_call(cb.ctx);

    // SAFETY: The caller must ensure `wr` is a valid pointer to a `InvertedIndexGCWriter`
    let wr = unsafe { &mut *wr };

    deltas
        .serialize(&mut rmp_serde::Serializer::new(wr))
        .is_ok()
}

/// Iterate over all the tag values stored in the index, in lexicographical
/// order.
///
/// Port of the C `TagIndex_IterateValues`, in both modes.
///
/// Drive the returned iterator with `TagIndex2_ValueIterator_Next`, which yields
/// each tag together with a value pointer: in memory mode the tag's
/// [`InvertedIndex<DocIdsOnly>`] stored in the values trie, in disk mode NULL
/// (the postings live on disk).
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]. It must outlive the returned iterator and must not
///    be mutated while the iterator is in use; the value pointers yielded by
///    `TagIndex2_ValueIterator_Next` are only valid under the same condition.
/// 2. The caller owns the returned iterator and must free it with
///    `TagIndex2_ValueIterator_Free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_IterateValues<'tm>(
    tag_index: *const TagIndex,
) -> *mut ValueIterator<'tm> {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: 1. guarantees tag_index is valid, non-null, and outlives the
    // returned iterator.
    let tag_index: &'tm TagIndex = unsafe { &*tag_index };

    Box::into_raw(Box::new(tag_index.value_iter()))
}

/// Iterate over the tag values matching the given pattern, in lexicographical
/// order. Depending on `iter_mode`, a tag matches when it starts with, ends
/// with, contains, or wildcard-matches the pattern.
///
/// Port of the C `TagIndex_IterateValuesWithFilter`, in both modes.
///
/// Drive the returned iterator with `TagIndex2_ValueIterator_NextKey`, which
/// yields each matching tag key. The value pointer is not exposed for this
/// iterator (callers resolve each reader by tag string). Use
/// `TagIndex2_ValueIterator_SetTimeout` to bound long affix expansions.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]. It must outlive the returned iterator and must not
///    be mutated while the iterator is in use.
/// 2. `pattern` must point to `len` readable bytes. It may only be NULL when
///    `len == 0`.
/// 3. For the prefix, contains, and wildcard modes, `pattern` must not be
///    freed or modified while the iterator lives (the suffix mode copies it).
/// 4. The caller owns the returned iterator and must free it with
///    `TagIndex2_ValueIterator_Free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_IterateValuesWithFilter<'tm>(
    tag_index: *const TagIndex,
    pattern: *const c_char,
    len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> *mut ValueIterator<'tm> {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: 1. guarantees tag_index is valid, non-null, and outlives the
    // returned iterator.
    let tag_index: &'tm TagIndex = unsafe { &*tag_index };

    let pattern: &'tm [u8] = if len > 0 {
        debug_assert!(!pattern.is_null(), "pattern cannot be NULL if len > 0");
        // SAFETY: 2. guarantees pattern points to len readable bytes; 3.
        // guarantees it stays valid for the iterator's lifetime where the
        // mode retains it.
        unsafe { std::slice::from_raw_parts(pattern.cast(), len as usize) }
    } else {
        &[]
    };

    let mode = match iter_mode {
        tm_iter_mode::TM_PREFIX_MODE => IterMode::Prefix,
        tm_iter_mode::TM_CONTAINS_MODE => IterMode::Contains,
        tm_iter_mode::TM_SUFFIX_MODE => IterMode::Suffix,
        tm_iter_mode::TM_WILDCARD_MODE => IterMode::Wildcard,
    };

    Box::into_raw(Box::new(tag_index.value_iter_filtered(pattern, mode)))
}

/// Iterate over the tag values within the specified range, invoking `callback`
/// for each `(tag, tag length, ctx, inverted index)` found.
///
/// Port of the C `TagIndex_IterateRangeValues`, in both modes; the boundary
/// semantics match `TrieMap_IterateRange`: a length of `-1` leaves that end of
/// the range open, a length of `0` stands for the empty string, and
/// `includeMin` / `includeMax` determine whether an exact boundary match is
/// yielded.
///
/// The pointer handed to the callback is, in memory mode, the
/// [`InvertedIndex<DocIdsOnly>`] stored in the index's values trie for that tag
/// (as expected by [`TagIndex2_GetIteratorFromTrieMapValue`]) and stays valid
/// as long as `tag_index` is not mutated; in disk mode it is NULL (the callback
/// resolves each reader by tag string).
///
/// Panics in debug builds when `callback` is NULL.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`] and must not be mutated by the callback.
/// 2. `min` may be NULL only when `minlen <= 0`; otherwise it must point to
///    `minlen` readable bytes. The same holds for `max` / `maxlen`.
/// 3. `callback` must be a valid function of type [`TrieMapRangeCallback`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_IterateRange(
    tag_index: *const TagIndex,
    min: *const c_char,
    minlen: c_int,
    includeMin: bool,
    max: *const c_char,
    maxlen: c_int,
    includeMax: bool,
    callback: TrieMapRangeCallback,
    ctx: *mut c_void,
) {
    let Some(callback) = callback else {
        #[cfg(debug_assertions)]
        {
            panic!("TagIndex2_IterateRange with a NULL callback");
        }
        #[cfg(not(debug_assertions))]
        {
            return; // It makes no sense to iterate without a callback
        }
    };

    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    let min: Option<&[u8]> = match minlen {
        ..0 => None,
        0 => Some([].as_slice()),
        1.. => {
            debug_assert!(!min.is_null(), "min cannot be NULL if minlen > 0");
            // SAFETY: 2. guarantees min points to minlen readable bytes.
            Some(unsafe { std::slice::from_raw_parts(min.cast(), minlen as usize) })
        }
    };

    let max: Option<&[u8]> = match maxlen {
        ..0 => None,
        0 => Some([].as_slice()),
        1.. => {
            debug_assert!(!max.is_null(), "max cannot be NULL if maxlen > 0");
            // SAFETY: 2. guarantees max points to maxlen readable bytes.
            Some(unsafe { std::slice::from_raw_parts(max.cast(), maxlen as usize) })
        }
    };

    // SAFETY: 1. guarantees tag_index is valid and non-null.
    let tag_index = unsafe { &*tag_index };

    let filter = RangeFilter {
        min: min.map(|m| RangeBoundary {
            value: m,
            is_included: includeMin,
        }),
        max: max.map(|m| RangeBoundary {
            value: m,
            is_included: includeMax,
        }),
    };

    // In disk mode the values trie holds only tag presence, so the callback
    // receives a NULL value pointer per key and resolves each reader by tag
    // string via the disk API (mirroring master's `tagRangeIterCb`). In memory
    // mode it receives the tag's inverted-index pointer directly.
    if tag_index.disk_mode() {
        tag_index
            .disk_range_iter_values(filter)
            .fuse()
            .for_each(|(key, ())| {
                // SAFETY: 3. guarantees callback is a valid TrieMapRangeCallback.
                unsafe {
                    (callback)(key.as_ptr().cast(), key.len(), ctx, std::ptr::null_mut());
                }
            });
    } else {
        tag_index
            .range_iter_values(filter)
            .fuse()
            .for_each(|(key, value)| {
                // SAFETY: 3. guarantees callback is a valid TrieMapRangeCallback.
                unsafe {
                    (callback)(
                        key.as_ptr().cast(),
                        key.len(),
                        ctx,
                        std::ptr::from_ref(value).cast_mut().cast(),
                    );
                }
            });
    }
}

/// Iterate over all the entries of the suffix index, in lexicographical
/// order, or return NULL when the index was created without `WITHSUFFIXTRIE`.
///
/// Port of the memory-mode `TagIndex_IterateSuffix`.
///
/// Drive the returned iterator with `TagIndex2_ValueIterator_NextKey`. Only the
/// keys are meaningful; the entry's internal bookkeeping value is not exposed.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `tag_index` must be a valid, non-NULL pointer to a [`TagIndex`] created
///    by [`TagIndex2_New`]. It must outlive the returned iterator and must
///    not be mutated while the iterator is in use.
/// 2. The caller owns the returned iterator and must free it with
///    `TagIndex2_ValueIterator_Free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_IterateSuffix<'tm>(
    tag_index: *const TagIndex,
) -> *mut ValueIterator<'tm> {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: 1. guarantees tag_index is valid, non-null, and outlives the
    // returned iterator.
    let tag_index: &'tm TagIndex = unsafe { &*tag_index };

    match tag_index.suffix_value_iter() {
        None => std::ptr::null_mut(),
        Some(iter) => Box::into_raw(Box::new(iter)),
    }
}

/// Statistics deltas from indexing a document's tags, for the caller to fold
/// into the spec stats. Mirrors the C `TagIndex_WritePostings` accounting.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TagIndexWriteResult {
    /// Bytes by which the inverted-index memory grew.
    pub size_delta: usize,
    /// Number of new (tag, doc) postings written (multi-value duplicates for a
    /// document are counted once).
    pub num_records: u32,
    /// Number of inverted-index blocks allocated.
    pub blocks_added: u32,
    /// `false` when a disk-mode write failed (`SearchDisk_IndexTags` returned
    /// false); the caller propagates it as the C `TagIndex_Index` `bool`.
    /// Always `true` in memory mode, which is infallible.
    pub ok: bool,
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_Index(
    tag_index: *mut TagIndex,
    ctx: *const RedisModuleCtx,
    batch: *const SearchDiskWriteBatchHandle,
    values: *const *const c_char,
    n: usize,
    doc_id: DocId,
) -> TagIndexWriteResult {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");
    debug_assert!(
        n == 0 || !values.is_null(),
        "values must not be null if n > 0"
    );

    // SAFETY: the caller guarantees `values` points to `n` readable, non-null
    // pointers, each addressing a valid, NUL-terminated C string.
    let value_ptrs = if n == 0 {
        &[]
    } else {
        // Safety: TODO
        unsafe { std::slice::from_raw_parts(values, n) }
    };

    // A `&[&[u8]]` can't alias the C `char*` array directly: the C array stores
    // bare pointers, while a `&[u8]` is a (pointer, length) fat pointer. So we
    // materialize the fat pointers into an owned `Vec` and pass a slice of it.
    let tags: Vec<&[u8]> = value_ptrs
        .iter()
        .map(|&ptr| {
            debug_assert!(!ptr.is_null(), "tag value pointers must not be null");
            // SAFETY: each `ptr` is a valid, NUL-terminated C string that
            // outlives this call, so the borrowed bytes are valid for the
            // duration of `index`.
            unsafe { std::ffi::CStr::from_ptr(ptr) }.to_bytes()
        })
        .collect();

    // SAFETY: 1. guarantees tag_index is valid, non-null, and outlives the
    // returned iterator. The caller must not alias it for the duration of the
    // call, so the exclusive borrow is sound.
    let tag_index = unsafe { &mut *tag_index };

    // In disk mode `ctx`/`batch` are the valid disk write context and batch
    // handle for this document write (contract), and each tag borrows a
    // NUL-terminated C string (built just above).
    let result = tag_index.index(ctx, batch, &tags, doc_id);
    match result {
        Some(delta) => TagIndexWriteResult {
            size_delta: delta.size_delta,
            num_records: delta.num_records,
            blocks_added: delta.blocks_added,
            ok: true,
        },
        None => TagIndexWriteResult {
            ok: false,
            ..Default::default()
        },
    }
}

/// Apply phase-3 metadata updates for the given tags and return the number of
/// records to fold into the spec statistics (committed tag values in disk mode,
/// `0` in memory mode where they were already counted at index time). Port of
/// the C `TagIndex_Commit` accounting.
/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_Commit(
    tag_index: *mut TagIndex,
    values: *const *const char,
    n: usize,
) -> u32 {
    let value_ptrs = if n == 0 {
        &[]
    } else {
        // Safety: TODO
        unsafe { std::slice::from_raw_parts(values, n) }
    };

    // `commit` feeds the suffix trie, whose keys are NUL-terminated (see
    // `TagSuffixIndex::add`), so keep the trailing NUL here — unlike
    // `TagIndex2_Index`, which keys the NUL-free values trie.
    let tags: Vec<&[u8]> = value_ptrs
        .iter()
        .map(|&ptr| {
            debug_assert!(!ptr.is_null(), "tag value pointers must not be null");
            // SAFETY: TODO
            unsafe { std::ffi::CStr::from_ptr(ptr.cast()) }.to_bytes_with_nul()
        })
        .collect();

    // Safety: TODO
    let tag_index = unsafe { &mut *tag_index };

    tag_index.commit(&tags)
}

/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_OpenReader(
    tag_index: *const TagIndex,
    sctx: *mut RedisSearchCtx,
    value: *const c_char,
    len: usize,
    weight: f64,
    field_index: t_fieldIndex,
    status: *mut QueryError,
) -> *mut QueryIterator {
    let tag: &[u8] = if len == 0 {
        &[]
    } else {
        // Safety: TODO
        unsafe { std::slice::from_raw_parts(value.cast(), len) }
    };

    // Safety: TODO
    let tag_index = unsafe { &*tag_index };

    let sctx = NonNull::new(sctx).expect("msg");

    // Safety: status is valid
    let iter = unsafe { tag_index.open_reader(sctx, tag, weight, field_index, status) };
    match iter.map(|p| p.as_ptr()) {
        None => std::ptr::null_mut(),
        Some(ptr) => ptr,
    }
}

/// TODO
/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TagIndex2_GetOverhead(tag_index: *const TagIndex) -> usize {
    // Safety: TODO
    let tag_index = unsafe { &*tag_index };

    tag_index.get_overhead()
}

/// TODO
/// # Safety
/// TODO
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_Free(tag_index: *mut *mut TagIndex) {
    debug_assert!(!tag_index.is_null(), "tag_index cannot be NULL");

    // Safety: TODO
    let tag_index = unsafe { &mut *tag_index };
    if tag_index.is_null() {
        return;
    }

    // Safety: TODO
    drop(unsafe { Box::from_raw(*tag_index) });
    *tag_index = std::ptr::null_mut();
}

/// C `BAD_POINTER` sentinel (`src/redisearch.h`). [`TagIndex2_GetSuffixWildcardMatches`]
/// returns it when the pattern has no literal token usable as a suffix-trie
/// anchor, telling the caller to fall back to a brute-force scan.
const BAD_POINTER: usize = 0xBAAAAAAD;

/// Build a C `arr.h` array (`arrayof`) of pointer-sized elements from `elems`,
/// returning the `array_t` (a pointer to the element buffer) that C drives with
/// `array_len` / `array_free`.
///
/// # Safety
///
/// The returned array is owned by the caller and must be released with
/// `array_free`. The pointer values are copied verbatim; the array does not own
/// their pointees.
unsafe fn build_ptr_array(elems: &[*const c_void]) -> *mut c_void {
    // elem_sz = pointer size, remain_cap = 0, len = elems.len().
    // Safety: TODO
    let arr = unsafe {
        ffi::array_new_sz(
            std::mem::size_of::<*const c_void>() as u16,
            0,
            elems.len() as u32,
        )
    }
    .cast::<*const c_void>();
    for (i, &elem) in elems.iter().enumerate() {
        // SAFETY: `array_new_sz` allocated `elems.len()` pointer-sized slots.
        let ptr = unsafe { arr.add(i) };
        // SAFETY: TODO
        unsafe { ptr.write(elem) };
    }
    arr.cast()
}

/// Return the terms whose suffix/contains entry matches `value`, as the flat C
/// `arrayof(char *)` the query layer consumes. Returns NULL when there are no
/// matches (C behaviour). The term strings are borrowed from the suffix trie and
/// must not be freed by the caller — only the array itself is owned by the caller
/// and freed with `array_free`.
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL [`TagIndex`] created by
///    [`TagIndex2_New`], with a suffix trie, and must outlive the returned
///    strings.
/// 2. `value` must point to `len` readable bytes (may be NULL when `len == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_GetSuffixMatches(
    tag_index: *const TagIndex,
    value: *const c_char,
    len: usize,
    prefix: bool,
    timeout: timespec,
    skip_timeout_checks: bool,
) -> *mut *mut c_char {
    let tag: &[u8] = if len == 0 {
        &[]
    } else {
        // SAFETY: 2. guarantees `value` points to `len` readable bytes.
        unsafe { std::slice::from_raw_parts(value.cast(), len) }
    };

    let timeout = (!skip_timeout_checks).then_some(timeout);

    // SAFETY: 1. guarantees `tag_index` is valid and non-null.
    let tag_index = unsafe { &*tag_index };

    // Drain the lazily-yielded matches into the flat pointer array C consumes. The
    // pointers borrow the suffix trie, which must outlive the array (guarantee 1).
    let elems: Vec<*const c_void> = tag_index
        .suffix_trie_map(tag, prefix, timeout)
        .map(|t| t.as_ptr().cast())
        .collect();
    if elems.is_empty() {
        return std::ptr::null_mut();
    }

    // SAFETY: `build_ptr_array` produces an owned arr.h array freed by the caller.
    unsafe { build_ptr_array(&elems) }.cast()
}

/// Return the terms matching the wildcard `pattern`, as the C `arrayof(char *)`
/// the query layer consumes. Returns [`BAD_POINTER`] when the pattern has no
/// usable anchor token (caller brute-forces), NULL when the anchor matched no
/// terms, otherwise the flat array. The term strings are borrowed from the
/// suffix trie; only the array is owned by the caller and freed with
/// `array_free`.
///
/// # Safety
///
/// 1. `tag_index` must be a valid, non-NULL [`TagIndex`] created by
///    [`TagIndex2_New`], with a suffix trie, and must outlive the returned
///    strings.
/// 2. `value` must point to `len` readable bytes (may be NULL when `len == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TagIndex2_GetSuffixWildcardMatches(
    tag_index: *const TagIndex,
    value: *const c_char,
    len: usize,
    timeout: timespec,
    max_prefix_expansions: c_longlong,
    skip_timeout_checks: bool,
) -> *mut *mut c_char {
    let tag: &[u8] = if len == 0 {
        &[]
    } else {
        // SAFETY: 2. guarantees `value` points to `len` readable bytes.
        unsafe { std::slice::from_raw_parts(value.cast(), len) }
    };

    // Choosing the anchor token also decides the brute-force fallback: no usable
    // token => C BAD_POINTER.
    let pattern = match SuffixWildcardPattern::new(tag) {
        Ok(pattern) => pattern,
        Err(_) => return BAD_POINTER as *mut *mut c_char,
    };

    let timeout = (!skip_timeout_checks).then_some(timeout);

    // SAFETY: 1. guarantees `tag_index` is valid and non-null.
    let tag_index = unsafe { &*tag_index };

    // Drain the lazily-yielded matches into the flat pointer array C consumes.
    // The pointers borrow the suffix trie, which must outlive the array (guarantee 1).
    let elems: Vec<*const c_void> = tag_index
        .suffix_wildcard(&pattern, timeout, max_prefix_expansions as u64)
        .map(|t| t.as_ptr().cast())
        .collect();

    // Anchor token had no matching terms: C returns NULL.
    if elems.is_empty() {
        return std::ptr::null_mut();
    }

    // SAFETY: `build_ptr_array` produces an owned arr.h array freed by the caller.
    unsafe { build_ptr_array(&elems) }.cast()
}
