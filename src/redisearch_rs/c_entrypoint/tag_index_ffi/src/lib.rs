/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for the [`tag_index`] crate.
//!
//! # Mode erasure
//!
//! C knows a single tag-index pointer, while Rust has two types —
//! [`tag_index::TagIndex`] parameterised by [`InMemoryMode`] or [`OnDiskMode`].
//! [`RustTagIndex`], the handle this crate hands to C, erases that difference
//! behind a discriminant and a union.
//!
//! It is deliberately *not* an `enum`. Matching on an enum needs a reference to
//! the handle, and the pointer to the in-memory index would then be derived from
//! that reference — which is exactly what [`TrieLookup::new`]'s first contract
//! forbids, because [`Rust_TagIndex_GC`] takes a `&mut` through the same pointer
//! and would revoke it. Every entry point therefore reaches its payload with
//! [`RustTagIndex::in_memory_ptr`] / [`RustTagIndex::on_disk_ptr`], raw place
//! projections that create no reference to the handle and so preserve the
//! provenance C owns.
//!
//! # Symbol naming
//!
//! Every exported function is prefixed `Rust_`, and the handle type is
//! [`RustTagIndex`] rather than `TagIndex`, so this implementation and the C one
//! in `src/tag_index.c` can be linked into the same binary — and appear in the
//! same generated headers — while the switch is in progress.

#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{CStr, c_char};
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

use ffi::{
    QueryError, QueryIterator, RedisSearchCtx, RedisSearchDiskIndexSpec,
    SearchDiskWriteBatchHandle, t_fieldIndex, timespec,
};
use index_result::RSIndexResult;
use inverted_index::{
    DocId, GcApplyInfo, GcScanDelta, IndexBlock, IndexUniqueId, InvertedIndex, RepairContext,
    doc_ids_only::DocIdsOnly,
};
use inverted_index_ffi::fork_gc::{InvertedIndexGCCallback, InvertedIndexGCWriter};
use redis_module::RedisModuleCtx;
use rqe_iterators::interop::RQEIteratorWrapper;
use serde::Serialize as _;
use tag_index::{
    DiskTagIndexIterator, InMemoryMode, IterMode, MemTagIndexIterator, OnDiskMode,
    SuffixEntryIterator, SuffixQuery, SuffixWildcardPattern, Tag, TagValueReader, TrieLookup,
};
use triemap_ffi::{tm_iter_mode, tm_len_t};

/// Which of [`Storage`]'s fields is live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    InMemory,
    OnDisk,
}

/// The two index types, overlaid. Which field is live is recorded by
/// [`RustTagIndex::mode`]; neither is dropped automatically, so [`Rust_TagIndex_Free`]
/// drops the live one by hand.
union Storage {
    in_memory: ManuallyDrop<tag_index::TagIndex<InMemoryMode>>,
    on_disk: ManuallyDrop<tag_index::TagIndex<OnDiskMode>>,
}

/// The mode-erased tag index C holds, as described in the [module docs](self).
pub struct RustTagIndex {
    mode: Mode,
    storage: Storage,
}

impl RustTagIndex {
    /// The live storage mode, read without forming a reference to the handle.
    ///
    /// Reading through a `&RustTagIndex` would freeze the whole handle for the
    /// duration of the borrow, and any mutable pointer derived from it — such as
    /// the one [`in_memory_ptr`](Self::in_memory_ptr) hands to [`TrieLookup`] —
    /// would be invalid to write through. A raw read keeps the handle untagged.
    ///
    /// # Safety
    ///
    /// `handle` must point to a live [`RustTagIndex`].
    const unsafe fn mode(handle: *const RustTagIndex) -> Mode {
        // SAFETY: the caller guarantees `handle` points to a live `RustTagIndex`, so
        // the place expression is valid. Taking its address is not a read.
        let mode = unsafe { &raw const (*handle).mode };
        // SAFETY: as above — the `mode` field of a live handle is initialised.
        unsafe { mode.read() }
    }

    /// The in-memory index inside `handle`.
    ///
    /// This is a raw place projection, not a reborrow: no reference to the handle
    /// exists at any point, so the result carries `handle`'s own provenance. That
    /// is what lets [`TrieLookup::new`]'s first contract be met — see the
    /// [module docs](self).
    ///
    /// # Safety
    ///
    /// `handle` must point to a live [`RustTagIndex`] whose mode is
    /// [`Mode::InMemory`].
    const unsafe fn in_memory_ptr(
        handle: *const RustTagIndex,
    ) -> *mut tag_index::TagIndex<InMemoryMode> {
        // SAFETY: the caller guarantees the handle is live and in memory mode, so
        // `storage.in_memory` is the initialised union field. `ManuallyDrop<T>` is
        // `repr(transparent)`, so the cast is a no-op on the address.
        unsafe { &raw mut (*handle.cast_mut()).storage.in_memory }.cast()
    }

    /// The on-disk index inside `handle`, projected as
    /// [`in_memory_ptr`](Self::in_memory_ptr) does.
    ///
    /// # Safety
    ///
    /// `handle` must point to a live [`RustTagIndex`] whose mode is [`Mode::OnDisk`].
    const unsafe fn on_disk_ptr(
        handle: *const RustTagIndex,
    ) -> *mut tag_index::TagIndex<OnDiskMode> {
        // SAFETY: as `in_memory_ptr`, for the other union field.
        unsafe { &raw mut (*handle.cast_mut()).storage.on_disk }.cast()
    }

    /// Borrow the in-memory index inside `handle`.
    ///
    /// # Safety
    ///
    /// 1. As [`in_memory_ptr`](Self::in_memory_ptr).
    /// 2. The index must not be mutated for `'a`.
    unsafe fn in_memory<'a>(handle: *const RustTagIndex) -> &'a tag_index::TagIndex<InMemoryMode> {
        // SAFETY: contract 1.
        let idx = unsafe { Self::in_memory_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &*idx }
    }

    /// Borrow the in-memory index inside `handle` exclusively.
    ///
    /// # Safety
    ///
    /// 1. As [`in_memory_ptr`](Self::in_memory_ptr).
    /// 2. No other reference to the index may be live for `'a`.
    unsafe fn in_memory_mut<'a>(
        handle: *mut RustTagIndex,
    ) -> &'a mut tag_index::TagIndex<InMemoryMode> {
        // SAFETY: contract 1.
        let idx = unsafe { Self::in_memory_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &mut *idx }
    }

    /// Borrow the on-disk index inside `handle`, as
    /// [`in_memory`](Self::in_memory) does.
    ///
    /// # Safety
    ///
    /// As [`in_memory`](Self::in_memory), for the other union field.
    unsafe fn on_disk<'a>(handle: *const RustTagIndex) -> &'a tag_index::TagIndex<OnDiskMode> {
        // SAFETY: contract 1.
        let idx = unsafe { Self::on_disk_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &*idx }
    }

    /// Borrow the on-disk index inside `handle` exclusively, as
    /// [`in_memory_mut`](Self::in_memory_mut) does.
    ///
    /// # Safety
    ///
    /// As [`in_memory_mut`](Self::in_memory_mut), for the other union field.
    unsafe fn on_disk_mut<'a>(
        handle: *mut RustTagIndex,
    ) -> &'a mut tag_index::TagIndex<OnDiskMode> {
        // SAFETY: contract 1.
        let idx = unsafe { Self::on_disk_ptr(handle) };
        // SAFETY: contracts 1 and 2.
        unsafe { &mut *idx }
    }
}

/// Dispatch `$body` over both modes, binding `$idx` to a shared reference to the
/// concrete index.
///
/// # Safety
///
/// The caller must uphold [`RustTagIndex::mode`]'s contract for `$handle`, and no
/// mutable pointer minted from the handle may be live across the expansion.
macro_rules! dispatch {
    ($handle:expr, |$idx:ident| $body:expr) => {{
        let handle = $handle;
        // SAFETY: upheld by this macro's caller.
        match unsafe { RustTagIndex::mode(handle) } {
            Mode::InMemory => {
                // SAFETY: the discriminant says the in-memory field is live.
                let $idx = unsafe { RustTagIndex::in_memory(handle) };
                $body
            }
            Mode::OnDisk => {
                // SAFETY: the discriminant says the on-disk field is live.
                let $idx = unsafe { RustTagIndex::on_disk(handle) };
                $body
            }
        }
    }};
}

/// Create a tag index, in memory or on disk depending on `disk_spec`.
///
/// A NULL `disk_spec` selects [`InMemoryMode`], any other value [`OnDiskMode`].
/// `field_index` is only read in disk mode, which needs it for the disk API
/// calls. `with_suffix` enables the suffix index (`WITHSUFFIXTRIE`).
///
/// The returned handle is owned by the caller and must be released with
/// [`Rust_TagIndex_Free`].
///
/// # Safety
///
/// If `disk_spec` is non-NULL it must point to a valid `RedisSearchDiskIndexSpec`
/// that stays valid for the whole lifetime of the returned index: the disk paths
/// hand it to the RSE API, which dereferences it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_New(
    disk_spec: *mut RedisSearchDiskIndexSpec,
    field_index: t_fieldIndex,
    with_suffix: bool,
) -> *mut RustTagIndex {
    let handle = match NonNull::new(disk_spec) {
        Some(disk_spec) => RustTagIndex {
            mode: Mode::OnDisk,
            storage: Storage {
                // SAFETY: the caller guarantees `disk_spec` is valid and outlives
                // the index.
                on_disk: ManuallyDrop::new(unsafe {
                    tag_index::TagIndex::<OnDiskMode>::new(disk_spec, field_index, with_suffix)
                }),
            },
        },
        None => RustTagIndex {
            mode: Mode::InMemory,
            storage: Storage {
                in_memory: ManuallyDrop::new(tag_index::TagIndex::<InMemoryMode>::new(with_suffix)),
            },
        },
    };

    Box::into_raw(Box::new(handle))
}

/// Free the tag index behind `tag_index` and NULL the caller's pointer.
///
/// A NULL `*tag_index` is a no-op, so freeing twice through the same slot is
/// safe.
///
/// # Safety
///
/// 1. `tag_index` must be a valid pointer to a writable `RustTagIndex *` slot.
/// 2. `*tag_index` must be NULL or a handle from [`Rust_TagIndex_New`] that has
///    not been freed.
/// 3. No iterator, reader, or lookup derived from the index may still be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_Free(tag_index: *mut *mut RustTagIndex) {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: contract 1 — the slot is valid and writable.
    let slot = unsafe { &mut *tag_index };
    let Some(handle) = NonNull::new(*slot) else {
        return;
    };

    // Drop the live union field by hand: a union has no drop glue.
    //
    // SAFETY: contract 2 — the handle came from `Rust_TagIndex_New`, so the
    // discriminant matches the initialised field, and contract 3 says nothing
    // borrows it any more.
    // SAFETY: contract 2 — the handle came from `Rust_TagIndex_New`.
    match unsafe { RustTagIndex::mode(handle.as_ptr()) } {
        Mode::InMemory => {
            // SAFETY: the discriminant says this is the initialised field.
            let idx = unsafe { RustTagIndex::in_memory_ptr(handle.as_ptr()) };
            // SAFETY: contract 3 — nothing borrows the payload any more, and it
            // is dropped exactly once, since the handle is freed right after.
            unsafe { std::ptr::drop_in_place(idx) };
        }
        Mode::OnDisk => {
            // SAFETY: as above, for the other field.
            let idx = unsafe { RustTagIndex::on_disk_ptr(handle.as_ptr()) };
            // SAFETY: as above.
            unsafe { std::ptr::drop_in_place(idx) };
        }
    }

    // The payload is gone; release the handle allocation. `RustTagIndex` itself has
    // no drop glue — `Mode` is a plain enum and a union never has any — so this
    // only frees the box.
    //
    // SAFETY: contract 2 — the handle came from `Box::into_raw` in
    // `Rust_TagIndex_New` and has not been freed.
    drop(unsafe { Box::from_raw(handle.as_ptr()) });

    *slot = std::ptr::null_mut();
}

/// The index's unique id, as reported to the fork GC to detect that a field's
/// index was replaced between a scan and its apply.
///
/// # Safety
///
/// `tag_index` must point to a live index from [`Rust_TagIndex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_GetId(tag_index: *const RustTagIndex) -> u32 {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live.
    dispatch!(tag_index, |idx| idx.id().into())
}

/// Whether the index was created `WITHSUFFIXTRIE`.
///
/// # Safety
///
/// `tag_index` must point to a live index from [`Rust_TagIndex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_HasSuffix(tag_index: *const RustTagIndex) -> bool {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live.
    dispatch!(tag_index, |idx| idx.has_suffix())
}

/// Whether the index keeps its postings on disk.
///
/// # Safety
///
/// `tag_index` must point to a live index from [`Rust_TagIndex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_HasDiskSpec(tag_index: *const RustTagIndex) -> bool {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live.
    matches!(unsafe { RustTagIndex::mode(tag_index) }, Mode::OnDisk)
}

/// How many distinct tags the index holds.
///
/// # Safety
///
/// `tag_index` must point to a live index from [`Rust_TagIndex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_NUniqueValues(tag_index: *const RustTagIndex) -> usize {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live.
    dispatch!(tag_index, |idx| idx.n_tags())
}

/// Bytes the index's tries occupy, as reported by `FT.INFO`.
///
/// This excludes the postings themselves, which are accounted incrementally
/// through [`TagIndexWriteResult::size_delta`] and [`GcApplyInfo::bytes_freed`].
///
/// # Safety
///
/// `tag_index` must point to a live index from [`Rust_TagIndex_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_GetOverhead(tag_index: *const RustTagIndex) -> usize {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live.
    dispatch!(tag_index, |idx| idx.mem_usage())
}

/// What indexing a document's tags changed, for the caller to fold into the
/// spec statistics.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TagIndexWriteResult {
    /// Bytes by which the inverted-index memory grew. Always 0 in disk mode.
    pub size_delta: usize,
    /// New `(tag, doc)` postings written. Always 0 in disk mode, where records
    /// are tallied by [`Rust_TagIndex_Commit`] instead.
    pub num_records: u32,
    /// Inverted-index blocks allocated. Always 0 in disk mode.
    pub blocks_added: u32,
    /// Whether the write succeeded. Always `true` in memory mode, where indexing
    /// is infallible.
    pub ok: bool,
}

/// Borrow the C strings out of `values`, skipping NULL entries.
///
/// NULL entries are not a caller error: `TagIndex_Preprocess` appends one per
/// tokenized value whose field is `INDEXEMPTY` and whose text is neither empty
/// nor separator-terminated, so a multi-value field interleaves them between the
/// real tags. Both C write paths skipped NULLs wherever they fell, and dropping
/// them here keeps that behaviour — a NULL is the absence of a tag, not the empty
/// tag, which arrives as `""`.
///
/// # Safety
///
/// `values` must point to `n` consecutive pointers, each either NULL or a
/// NUL-terminated string valid for `'a`.
unsafe fn borrow_values<'a>(values: *const *const c_char, n: usize) -> Vec<&'a CStr> {
    if n == 0 {
        return Vec::new();
    }
    debug_assert!(!values.is_null(), "values must not be null when n > 0");

    // The C array holds `char *`, not lengths, so there is no way to alias it as
    // a `&[&[u8]]` — each string has to be measured before it can be borrowed.
    //
    // SAFETY: the caller guarantees `values` holds `n` readable pointers.
    unsafe { std::slice::from_raw_parts(values, n) }
        .iter()
        .filter(|v| !v.is_null())
        // SAFETY: the caller guarantees every non-NULL element is a valid C string.
        .map(|&v| unsafe { CStr::from_ptr(v) })
        .collect()
}

/// Reinterpret C strings as [`Tag`]s.
///
/// The bytes come from [`CStr::to_bytes`], which stops at the terminator, so the
/// no-interior-NUL invariant holds by construction.
fn as_tags<'a>(values: &[&'a CStr]) -> Vec<Tag<'a>> {
    values
        .iter()
        // SAFETY: `to_bytes()` excludes the terminator and stops at it, so the
        // result cannot contain an interior NUL.
        .map(|v| unsafe { Tag::new_unchecked(v.to_bytes()) })
        .collect()
}

/// Index `doc_id` under each of the `n` tags in `values`.
///
/// In memory mode the postings are written inline and the returned deltas are
/// non-zero. In disk mode they are staged onto `batch` for `commitDocument`, and
/// only [`TagIndexWriteResult::ok`] is meaningful; `has_field_expiration` is
/// ignored, as the disk backend takes no such flag.
///
/// # Safety
///
/// 1. `tag_index` must point to a live index from [`Rust_TagIndex_New`], and no
///    iterator or reader derived from it may be alive.
/// 2. `values` must point to `n` valid, non-NULL, NUL-terminated strings, valid
///    for the duration of the call. It may be NULL only when `n` is 0.
/// 3. In disk mode, `ctx` and `batch` must be the valid Redis module context and
///    write-batch handle for the document write in progress.
/// 4. In memory mode, `doc_id` must be greater than or equal to every `doc_id`
///    already passed to this function for `tag_index`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_Index(
    tag_index: *mut RustTagIndex,
    ctx: *const RedisModuleCtx,
    batch: *const SearchDiskWriteBatchHandle,
    values: *const *const c_char,
    n: usize,
    doc_id: DocId,
    has_field_expiration: bool,
) -> TagIndexWriteResult {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: contract 2.
    let values = unsafe { borrow_values(values, n) };

    // SAFETY: contract 1 — the handle is live, so the discriminant is readable.
    match unsafe { RustTagIndex::mode(tag_index) } {
        Mode::InMemory => {
            let tags = as_tags(&values);
            // SAFETY: contract 1 — the in-memory field is live and unaliased.
            let idx = unsafe { RustTagIndex::in_memory_mut(tag_index) };
            // SAFETY: contract 4.
            let delta = unsafe { idx.index(&tags, doc_id, has_field_expiration) };
            TagIndexWriteResult {
                size_delta: delta.size_delta,
                num_records: delta.num_records,
                blocks_added: delta.blocks_added,
                ok: true,
            }
        }
        Mode::OnDisk => {
            // SAFETY: contract 1 — the on-disk field is live and unaliased.
            let idx = unsafe { RustTagIndex::on_disk_mut(tag_index) };
            // SAFETY: contract 3 — `ctx` and `batch` belong to the ongoing write.
            let ok = unsafe { idx.index(ctx, batch, &values, doc_id) };
            TagIndexWriteResult {
                ok,
                ..Default::default()
            }
        }
    }
}

/// Make the `n` tags in `values` visible to query expansion, and return how many
/// records that added to the spec statistics.
///
/// Disk mode inserts the presence sentinels its postings were written against and
/// counts them here; memory mode already counted them in [`Rust_TagIndex_Index`]
/// and returns 0. Both modes populate the suffix index.
///
/// # Safety
///
/// As [`Rust_TagIndex_Index`]'s contracts 1 and 2.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_Commit(
    tag_index: *mut RustTagIndex,
    values: *const *const c_char,
    n: usize,
) -> u32 {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: `Rust_TagIndex_Index` contract 2.
    let values = unsafe { borrow_values(values, n) };
    let tags = as_tags(&values);

    // SAFETY: `Rust_TagIndex_Index` contract 1.
    match unsafe { RustTagIndex::mode(tag_index) } {
        Mode::InMemory => {
            // SAFETY: the discriminant says the in-memory field is live and it is
            // unaliased.
            let idx = unsafe { RustTagIndex::in_memory_mut(tag_index) };
            idx.commit(&tags)
        }
        Mode::OnDisk => {
            // SAFETY: as above, for the other field.
            let idx = unsafe { RustTagIndex::on_disk_mut(tag_index) };
            idx.commit(&tags)
        }
    }
}

/// Open a reader over the documents carrying `value`, or NULL when the tag is
/// absent, holds no documents, or the disk backend fails to build its iterator.
///
/// The returned iterator is owned by the caller and released through its own
/// `Free` callback (`it->Free(it)`).
///
/// # Safety
///
/// 1. `tag_index` must point to a live index from [`Rust_TagIndex_New`].
///
///    In memory mode it must further be *the* pointer the owning field spec
///    holds in `tagOpts.tagIndex` — the one [`Rust_TagIndex_GC`] is called with —
///    and not a copy derived from a reference to the index. The iterator keeps it
///    for revalidation, and the collector's `&mut` would revoke anything derived
///    above it. See [`TrieLookup::new`].
/// 2. The index must outlive the returned iterator, and may be mutated while it
///    is alive only under the standard revalidation protocol.
/// 3. `sctx` and `sctx.spec` must be valid and outlive the returned iterator. In
///    disk mode `sctx.diskSnapshot` must additionally be a valid snapshot handle
///    for this index's disk spec.
/// 4. `value` must point to `len` readable bytes, or may be NULL when `len` is 0.
/// 5. `status`, when non-NULL, must point to a valid [`QueryError`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_OpenReader(
    tag_index: *const RustTagIndex,
    sctx: *mut RedisSearchCtx,
    value: *const c_char,
    len: usize,
    weight: f64,
    field_index: t_fieldIndex,
    status: *mut QueryError,
) -> *mut QueryIterator {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");

    // SAFETY: contract 4.
    let tag = unsafe { borrow_tag(value.cast(), len) };
    // SAFETY: contract 3 — `sctx` is valid and non-null.
    let sctx = unsafe { NonNull::new_unchecked(sctx) };

    // SAFETY: contract 1 — the handle is live.
    match unsafe { RustTagIndex::mode(tag_index) } {
        Mode::InMemory => {
            // Mint the lookup from the argument itself, before any reference to
            // the index exists: copying a raw pointer is not an access, so the
            // lookup carries the provenance C owns — which is what keeps it alive
            // across the collector's `&mut`.
            //
            // SAFETY: contract 1 — the in-memory field is live.
            let idx = unsafe { RustTagIndex::in_memory_ptr(tag_index) };
            // SAFETY: `in_memory_ptr` never returns null for a live handle.
            let owner = unsafe { NonNull::new_unchecked(idx) };
            // SAFETY: contract 1's second paragraph is `TrieLookup::new`'s first
            // contract; contract 2 is its second and third.
            let lookup = unsafe { TrieLookup::new(owner) };

            // Only now that the lookup holds the owner's pointer is it safe to
            // form a reference: it dies with this call, and nothing reads through
            // it afterwards.
            //
            // SAFETY: contract 2 — the index is live and outlives the reader.
            let idx = unsafe { &*idx };
            // SAFETY: contracts 2 and 3; `lookup` resolves `idx` by construction.
            let reader = unsafe { idx.open_reader(sctx, tag, weight, field_index, lookup) };
            reader.map_or(std::ptr::null_mut(), RQEIteratorWrapper::boxed_new)
        }
        Mode::OnDisk => {
            // SAFETY: the discriminant says the on-disk field is live.
            let idx = unsafe { RustTagIndex::on_disk(tag_index) };
            // SAFETY: contracts 2 and 3, including the snapshot on `sctx`.
            match unsafe { idx.open_reader(sctx, tag, weight, field_index) } {
                Ok(reader) => RQEIteratorWrapper::boxed_new(reader),
                Err(err) => {
                    // Report the failure so the query aborts through the existing
                    // `QueryError_HasError` check rather than silently reading as
                    // an empty tag.
                    //
                    // SAFETY: contract 5.
                    if let Some(status) = unsafe {
                        query_error::QueryError::from_opaque_mut_ptr(
                            status.cast::<query_error::opaque::OpaqueQueryError>(),
                        )
                    } {
                        status.set_error(
                            query_error::QueryErrorCode::DiskIteratorCreation,
                            &format!("Failed to create a disk tag iterator: {err}"),
                        );
                    }
                    std::ptr::null_mut()
                }
            }
        }
    }
}

/// Borrow `len` bytes from `ptr`.
///
/// `len == 0` yields an empty slice without dereferencing `ptr`, which
/// [`std::slice::from_raw_parts`] would reject for NULL even at length 0. C
/// passes exactly that for the empty tag written under `INDEXEMPTY`.
///
/// # Safety
///
/// `ptr` must point to `len` readable bytes, unless `len` is 0.
unsafe fn borrow_tag<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    debug_assert!(!ptr.is_null(), "tag must not be null when len > 0");
    // SAFETY: the caller guarantees `len` readable bytes at `ptr`.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// A tag's posting list.
///
/// Tag postings are always `DocIdsOnly` encoded, so — unlike the general
/// inverted-index FFI — no encoding dispatch is needed here.
#[repr(transparent)]
pub struct TagIndexValue(InvertedIndex<DocIdsOnly>);

/// A walk over a tag index's values, erasing the two modes' iterator types and
/// the suffix index's, which yields keys only.
pub enum ValueIterator<'ti> {
    InMemory(MemTagIndexIterator<'ti>),
    OnDisk(DiskTagIndexIterator<'ti>),
    Suffix(SuffixEntryIterator<'ti>),
}

impl<'ti> ValueIterator<'ti> {
    /// The next tag, with its posting list when the mode has one in memory.
    ///
    /// The key is borrowed from trie-internal storage and is invalidated by the
    /// next call.
    fn advance(&mut self) -> Option<(&[u8], Option<&'ti TagIndexValue>)> {
        match self {
            // The postings live behind a `Box` in the trie, so their address is
            // stable across the walk and may outlive the key's borrow.
            Self::InMemory(it) => it.advance().map(|(k, ii)| {
                let value: *const TagIndexValue = std::ptr::from_ref(ii).cast();
                // SAFETY: `TagIndexValue` is `repr(transparent)` over the
                // inverted index, which the index owns for at least `'ti`.
                (k, Some(unsafe { &*value }))
            }),
            // Disk-mode and suffix entries carry no in-memory posting list.
            Self::OnDisk(it) => it.advance().map(|k| (k, None)),
            Self::Suffix(it) => it.advance().map(|k| (k, None)),
        }
    }

    fn set_timeout(&mut self, timeout: Option<timespec>) {
        match self {
            Self::InMemory(it) => it.set_timeout(timeout),
            Self::OnDisk(it) => it.set_timeout(timeout),
            Self::Suffix(it) => it.set_timeout(timeout),
        }
    }
}

/// Translate C's iteration mode. The two enums are kept in lockstep by
/// `tag_iter_mode` in `src/tag_index.h`.
const fn iter_mode(mode: tm_iter_mode) -> IterMode {
    match mode {
        tm_iter_mode::TM_PREFIX_MODE => IterMode::Prefix,
        tm_iter_mode::TM_CONTAINS_MODE => IterMode::Contains,
        tm_iter_mode::TM_SUFFIX_MODE => IterMode::Suffix,
        tm_iter_mode::TM_WILDCARD_MODE => IterMode::Wildcard,
    }
}

/// Walk every tag in the index, in lexicographical order.
///
/// The iterator is owned by the caller and released with
/// [`Rust_TagIndex_ValueIterator_Free`].
///
/// # Safety
///
/// `tag_index` must point to a live index from [`Rust_TagIndex_New`], which must
/// outlive the returned iterator and must not be mutated while it is alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_IterateValues<'ti>(
    tag_index: *const RustTagIndex,
) -> *mut ValueIterator<'ti> {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live and outlives the iterator.
    let iter = match unsafe { RustTagIndex::mode(tag_index) } {
        Mode::InMemory => {
            // SAFETY: the discriminant says the in-memory field is live.
            let idx = unsafe { RustTagIndex::in_memory(tag_index) };
            ValueIterator::InMemory(idx.value_iter())
        }
        Mode::OnDisk => {
            // SAFETY: as above, for the other field.
            let idx = unsafe { RustTagIndex::on_disk(tag_index) };
            ValueIterator::OnDisk(idx.value_iter())
        }
    };

    Box::into_raw(Box::new(iter))
}

/// Walk the tags matching `pattern` under `mode`, in lexicographical order.
///
/// # Safety
///
/// 1. As [`Rust_TagIndex_IterateValues`].
/// 2. `pattern` must point to `len` readable bytes, or may be NULL when `len` is
///    0.
/// 3. `pattern` must stay valid and unmodified for the lifetime of the returned
///    iterator: every mode but [`IterMode::Suffix`] borrows it rather than
///    copying it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_IterateValuesWithFilter<'ti>(
    tag_index: *const RustTagIndex,
    pattern: *const c_char,
    len: tm_len_t,
    mode: tm_iter_mode,
) -> *mut ValueIterator<'ti> {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: contracts 2 and 3 — the bytes are readable and outlive the iterator.
    let pattern = unsafe { borrow_tag(pattern.cast(), len as usize) };
    let mode = iter_mode(mode);

    // SAFETY: contract 1.
    let iter = match unsafe { RustTagIndex::mode(tag_index) } {
        Mode::InMemory => {
            // SAFETY: the discriminant says the in-memory field is live.
            let idx = unsafe { RustTagIndex::in_memory(tag_index) };
            ValueIterator::InMemory(idx.value_iter_filtered(pattern, mode))
        }
        Mode::OnDisk => {
            // SAFETY: as above, for the other field.
            let idx = unsafe { RustTagIndex::on_disk(tag_index) };
            ValueIterator::OnDisk(idx.value_iter_filtered(pattern, mode))
        }
    };

    Box::into_raw(Box::new(iter))
}

/// Walk the suffix index's entries, or return NULL when the index was created
/// without `WITHSUFFIXTRIE`.
///
/// # Safety
///
/// As [`Rust_TagIndex_IterateValues`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_IterateSuffix<'ti>(
    tag_index: *const RustTagIndex,
) -> *mut ValueIterator<'ti> {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");

    // SAFETY: the caller guarantees the handle is live and outlives the iterator.
    let iter = dispatch!(tag_index, |idx| idx.suffix_value_iter());
    let Some(iter) = iter else {
        return std::ptr::null_mut();
    };

    Box::into_raw(Box::new(ValueIterator::Suffix(iter)))
}

/// Advance `iter`, writing the tag and its posting list through the out
/// parameters. Returns 1 while there was an entry, 0 once the walk is over or
/// the deadline has passed.
///
/// `*value` is set to NULL for disk-mode and suffix entries, which have no
/// in-memory posting list. The key written to `*ptr` is borrowed from the index
/// and is invalidated by the next call.
///
/// # Safety
///
/// 1. `iter` must point to a live iterator from one of this crate's
///    `Rust_TagIndex_Iterate*` functions.
/// 2. `ptr`, `len`, and `value` must be valid, writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_ValueIterator_Next<'ti>(
    iter: *mut ValueIterator<'ti>,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
    value: *mut *mut TagIndexValue,
) -> std::ffi::c_int {
    debug_assert!(!iter.is_null(), "iter must not be null");
    debug_assert!(!ptr.is_null(), "ptr must not be null");
    debug_assert!(!len.is_null(), "len must not be null");
    debug_assert!(!value.is_null(), "value must not be null");

    // SAFETY: contract 1.
    let Some((key, tag_value)) = (unsafe { &mut *iter }).advance() else {
        return 0;
    };

    debug_assert!(
        u16::try_from(key.len()).is_ok(),
        "tag length must fit `tm_len_t`"
    );
    // SAFETY: contract 2 — `ptr` is writable.
    unsafe { *ptr = key.as_ptr().cast::<c_char>().cast_mut() };
    // SAFETY: contract 2 — `len` is writable.
    unsafe { *len = key.len() as tm_len_t };
    // SAFETY: contract 2 — `value` is writable.
    unsafe {
        *value = tag_value.map_or(std::ptr::null_mut(), |v| std::ptr::from_ref(v).cast_mut());
    }

    1
}

/// Advance `iter`, writing only the tag, as [`Rust_TagIndex_ValueIterator_Next`]
/// does. For callers that never look at the posting list.
///
/// # Safety
///
/// 1. As [`Rust_TagIndex_ValueIterator_Next`]'s contract 1.
/// 2. `ptr` and `len` must be valid, writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_ValueIterator_NextKey<'ti>(
    iter: *mut ValueIterator<'ti>,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
) -> std::ffi::c_int {
    debug_assert!(!iter.is_null(), "iter must not be null");
    debug_assert!(!ptr.is_null(), "ptr must not be null");
    debug_assert!(!len.is_null(), "len must not be null");

    // SAFETY: contract 1.
    let Some((key, _)) = (unsafe { &mut *iter }).advance() else {
        return 0;
    };

    debug_assert!(
        u16::try_from(key.len()).is_ok(),
        "tag length must fit `tm_len_t`"
    );
    // SAFETY: contract 2 — `ptr` is writable.
    unsafe { *ptr = key.as_ptr().cast::<c_char>().cast_mut() };
    // SAFETY: contract 2 — `len` is writable.
    unsafe { *len = key.len() as tm_len_t };

    1
}

/// Bound the remaining walk by `timeout`, the deadline used for affix expansions
/// elsewhere in the query.
///
/// # Safety
///
/// As [`Rust_TagIndex_ValueIterator_Next`]'s contract 1.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_ValueIterator_SetTimeout(
    iter: *mut ValueIterator,
    timeout: timespec,
) {
    debug_assert!(!iter.is_null(), "iter must not be null");

    // SAFETY: the caller guarantees `iter` is live.
    unsafe { &mut *iter }.set_timeout(Some(timeout));
}

/// Release an iterator from one of this crate's `Rust_TagIndex_Iterate*`
/// functions. A NULL `iter` is a no-op.
///
/// # Safety
///
/// `iter` must be NULL, or a live iterator from one of those functions that has
/// not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_ValueIterator_Free(iter: *mut ValueIterator) {
    if iter.is_null() {
        return;
    }
    // SAFETY: the caller guarantees `iter` came from `Box::into_raw` in one of
    // the `Rust_TagIndex_Iterate*` functions and has not been freed.
    drop(unsafe { Box::from_raw(iter) });
}

/// A reader over one tag's postings.
///
/// A `repr(Rust)` newtype on purpose: the generated header keeps the opaque name
/// rather than exposing the reader's layout.
pub struct TagIndexValueIter<'trie>(TagValueReader<'trie>);

/// Open a reader over `tag_index_value`'s postings.
///
/// The reader is owned by the caller and released with
/// [`Rust_TagIndexValueIter_Free`].
///
/// # Safety
///
/// `tag_index_value` must point to a live posting list, obtained from
/// [`Rust_TagIndex_ValueIterator_Next`], which must outlive the returned reader
/// and must not be mutated while it is alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValue_Iter<'trie>(
    tag_index_value: *const TagIndexValue,
) -> *mut TagIndexValueIter<'trie> {
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );

    // SAFETY: the caller guarantees the posting list is live and outlives the
    // reader.
    let ii = unsafe { &(*tag_index_value).0 };
    Box::into_raw(Box::new(TagIndexValueIter(TagValueReader::new(ii))))
}

/// Read the next posting into `res`, returning whether there was one.
///
/// # Safety
///
/// 1. `iter` must point to a live reader from [`Rust_TagIndexValue_Iter`].
/// 2. `res` must point to a valid, writable [`RSIndexResult`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValueIter_Next<'trie>(
    iter: *mut TagIndexValueIter<'trie>,
    res: *mut RSIndexResult<'trie>,
) -> bool {
    debug_assert!(!iter.is_null(), "iter must not be null");
    debug_assert!(!res.is_null(), "res must not be null");

    // SAFETY: contract 1.
    let iter = unsafe { &mut *iter };
    // SAFETY: contract 2.
    let res = unsafe { &mut *res };
    iter.0.next_record(res)
}

/// Release a reader from [`Rust_TagIndexValue_Iter`]. A NULL `iter` is a no-op.
///
/// # Safety
///
/// `iter` must be NULL, or a live reader from [`Rust_TagIndexValue_Iter`] that
/// has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValueIter_Free(iter: *mut TagIndexValueIter) {
    if iter.is_null() {
        return;
    }
    // SAFETY: the caller guarantees `iter` came from `Box::into_raw` in
    // `Rust_TagIndexValue_Iter` and has not been freed.
    drop(unsafe { Box::from_raw(iter) });
}

/// How many documents carry this tag.
///
/// # Safety
///
/// `tag_index_value` must point to a live posting list from
/// [`Rust_TagIndex_ValueIterator_Next`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValue_NumDocs(tag_index_value: *const TagIndexValue) -> u32 {
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );

    // SAFETY: the caller guarantees the posting list is live.
    unsafe { &(*tag_index_value).0 }.unique_docs()
}

/// How many blocks the posting list occupies.
///
/// # Safety
///
/// As [`Rust_TagIndexValue_NumDocs`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValue_NumBlocks(
    tag_index_value: *const TagIndexValue,
) -> usize {
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );

    // SAFETY: the caller guarantees the posting list is live.
    unsafe { &(*tag_index_value).0 }.number_of_blocks()
}

/// The posting list's `block_idx`-th block, or NULL when it is out of range.
///
/// Exposed for the fork-GC tests, which assert on how a collection cycle
/// rewrites individual blocks. The block accessors themselves live in
/// `inverted_index_ffi`.
///
/// # Safety
///
/// As [`Rust_TagIndexValue_NumDocs`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValue_BlockRef<'a>(
    tag_index_value: *const TagIndexValue,
    block_idx: usize,
) -> Option<&'a IndexBlock> {
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );

    // SAFETY: the caller guarantees the posting list is live.
    let ii: &'a _ = unsafe { &(*tag_index_value).0 };
    ii.block_ref(block_idx)
}

/// The posting list's unique id, which [`Rust_TagIndex_GC`] takes to detect that
/// the list it is applying a delta to is no longer the one that was scanned.
///
/// Comparing addresses would not do: between a fork-GC scan and its apply the
/// tag's posting list can be freed and a new one allocated at the same address.
///
/// # Safety
///
/// As [`Rust_TagIndexValue_NumDocs`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValue_UniqueId(
    tag_index_value: *const TagIndexValue,
) -> IndexUniqueId {
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );

    // SAFETY: the caller guarantees the posting list is live.
    unsafe { &(*tag_index_value).0 }.unique_id()
}

/// What applying a fork-GC delta to a tag's postings changed.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TagGcResult {
    /// Statistics for the caller to fold into the spec's totals. Meaningful only
    /// when `applied`.
    pub info: GcApplyInfo,
    /// Whether the delta was applied. `false` means the scanned posting list is
    /// no longer the tag's current one — it was removed or replaced since the
    /// scan — and the delta was discarded.
    pub applied: bool,
}

/// Scan `tag_index_value` for postings whose documents are gone and write the
/// resulting delta to `wr`, returning whether that succeeded.
///
/// Runs in the fork-GC child. `cb` is invoked once the scan has produced a delta
/// and before it is serialized, which is what lets the parent be notified in the
/// right order.
///
/// # Safety
///
/// 1. `wr` and `cb` must be valid, non-NULL pointers to the fork GC's writer and
///    callback.
/// 2. `sctx` must be a valid, non-NULL [`RedisSearchCtx`] whose `spec` is
///    likewise valid.
/// 3. `tag_index_value` must point to a live posting list from
///    [`Rust_TagIndex_ValueIterator_Next`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndexValue_GcDelta_Scan(
    wr: *mut InvertedIndexGCWriter,
    sctx: *mut RedisSearchCtx,
    tag_index_value: *const TagIndexValue,
    cb: *mut InvertedIndexGCCallback,
) -> bool {
    debug_assert!(!wr.is_null(), "wr must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(
        !tag_index_value.is_null(),
        "tag_index_value must not be null"
    );
    debug_assert!(!cb.is_null(), "cb must not be null");

    // SAFETY: contract 2.
    let sctx = unsafe { &*sctx };
    debug_assert!(!sctx.spec.is_null(), "sctx.spec must not be null");
    // SAFETY: contract 2 — the spec is valid.
    let doc_table = unsafe { &*sctx.spec }.docs;
    // SAFETY: `doc_table` was just read off the spec, so it is a valid `DocTable`.
    let doc_exists = |id| unsafe { ffi::DocTable_Exists(&doc_table, id) };

    // SAFETY: contract 3.
    let ii = unsafe { &(*tag_index_value).0 };
    let Ok(Some(deltas)) = ii.scan_gc(doc_exists, None::<fn(&RSIndexResult, &RepairContext<'_>)>)
    else {
        return false;
    };

    // SAFETY: contract 1.
    let cb = unsafe { &*cb };
    let cb_call = cb.call;
    cb_call(cb.ctx);

    // SAFETY: contract 1.
    let wr = unsafe { &mut *wr };
    deltas
        .serialize(&mut rmp_serde::Serializer::new(wr))
        .is_ok()
}

/// Apply the fork-GC `delta` to `tag`'s posting list.
///
/// Runs in the parent. Beyond applying the delta this checks that the list is
/// still the one the child scanned — see [`Rust_TagIndexValue_UniqueId`] — and,
/// when the list ends up empty, drops the tag from both the values trie and the
/// suffix index.
///
/// Disk-mode indexes have no in-memory postings to collect; calling this on one
/// discards `delta` and reports it as not applied.
///
/// # Safety
///
/// 1. `tag_index` must point to a live index from [`Rust_TagIndex_New`], with no
///    reader or iterator over it alive except under the revalidation protocol.
/// 2. `tag` must point to `len` readable bytes, or may be NULL when `len` is 0 —
///    which is how the empty tag written under `INDEXEMPTY` arrives.
/// 3. `delta` must be a non-NULL delta from `InvertedIndex_GcDelta_Read`.
///    Ownership transfers to this call, which consumes it on every path.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_GC(
    tag_index: *mut RustTagIndex,
    tag: *const u8,
    len: usize,
    unique_id: IndexUniqueId,
    delta: *mut GcScanDelta,
) -> TagGcResult {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");
    debug_assert!(!delta.is_null(), "delta must not be null");

    // SAFETY: contract 3 — ownership transfers here, so the box is dropped on
    // every path out of this function, including the disk-mode one below.
    let delta = *unsafe { Box::from_raw(delta) };
    // SAFETY: contract 2.
    let tag = unsafe { borrow_tag(tag, len) };

    // SAFETY: contract 1 — the handle is live.
    let Mode::InMemory = (unsafe { RustTagIndex::mode(tag_index) }) else {
        return TagGcResult::default();
    };

    // SAFETY: contract 1 — the in-memory field is live and unaliased.
    let idx = unsafe { RustTagIndex::in_memory_mut(tag_index) };
    idx.gc(tag, unique_id, delta)
        .map_or_else(TagGcResult::default, |info| TagGcResult {
            info,
            applied: true,
        })
}

/// The `BAD_POINTER` sentinel from `src/redisearch.h`, which
/// [`Rust_TagIndex_GetSuffixWildcardMatches`] returns to ask its caller to fall
/// back to a brute-force scan.
const BAD_POINTER: usize = 0xBAAA_AAAD;

/// Copy `elems` into a C `arr.h` array, which the caller frees with `array_free`.
///
/// # Safety
///
/// `array_new_sz` must be callable, i.e. the module's allocator is initialised.
unsafe fn build_ptr_array(elems: &[*const c_char]) -> *mut *mut c_char {
    // SAFETY: the element size and length are exact, and the caller guarantees
    // the allocator is up.
    let arr =
        unsafe { ffi::array_new_sz(size_of::<*const c_char>() as u16, 0, elems.len() as u32) }
            .cast::<*mut c_char>();

    for (i, &elem) in elems.iter().enumerate() {
        // SAFETY: `array_new_sz` allocated room for exactly `elems.len()`
        // elements, and `i` is in range.
        let slot = unsafe { arr.add(i) };
        // SAFETY: as above — `slot` is in bounds and aligned.
        unsafe { slot.write(elem.cast_mut()) };
    }

    arr
}

/// Collect a suffix expansion into a C array, or NULL when nothing matched.
///
/// The terms are borrowed from the suffix index — each is NUL-terminated there,
/// so the pointers are usable as C strings — and only the array itself is owned
/// by the caller.
///
/// # Safety
///
/// As [`build_ptr_array`].
unsafe fn collect_matches<'a>(matches: impl Iterator<Item = &'a [u8]>) -> *mut *mut c_char {
    let elems: Vec<*const c_char> = matches.map(|term| term.as_ptr().cast()).collect();
    if elems.is_empty() {
        return std::ptr::null_mut();
    }
    // SAFETY: this function's contract.
    unsafe { build_ptr_array(&elems) }
}

/// Expand `value` through the suffix index, returning the matching tags.
///
/// `prefix` selects a *contains* (`*foo*`) expansion rather than a *suffix*
/// (`*foo`) one, mirroring the flag C passes down from the query node.
/// `skip_timeout_checks` runs the walk unbounded; otherwise it stops at
/// `timeout` and the caller keeps the partial result.
///
/// Returns NULL when nothing matched. The result is an `arr.h` array the caller
/// frees with `array_free`; the strings inside it are borrowed from the index and
/// must not be freed.
///
/// # Safety
///
/// 1. `tag_index` must point to a live index from [`Rust_TagIndex_New`] that was
///    created `WITHSUFFIXTRIE`, and must outlive the returned array's use.
/// 2. `value` must point to `len` readable bytes, or may be NULL when `len` is 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_GetSuffixMatches(
    tag_index: *const RustTagIndex,
    value: *const c_char,
    len: usize,
    prefix: bool,
    timeout: timespec,
    skip_timeout_checks: bool,
) -> *mut *mut c_char {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");
    debug_assert!(
        // SAFETY: contract 1 — the handle is live.
        unsafe { Rust_TagIndex_HasSuffix(tag_index) },
        "suffix expansion needs an index created WITHSUFFIXTRIE"
    );

    // SAFETY: contract 2.
    let tag = unsafe { borrow_tag(value.cast(), len) };
    let Some(tag) = Tag::new(tag) else {
        // A tag with an interior NUL cannot be in the index: the tries are keyed
        // by NUL-free bytes.
        return std::ptr::null_mut();
    };

    let query = if prefix {
        SuffixQuery::Contains(tag)
    } else {
        SuffixQuery::Suffix(tag)
    };
    let deadline = (!skip_timeout_checks).then_some(timeout);

    // SAFETY: contract 1 — the handle is live and has a suffix index, so
    // `suffix_expand` will not panic.
    let matches = dispatch!(tag_index, |idx| idx.suffix_expand(query, deadline));
    // SAFETY: the module allocator is up by the time queries run.
    unsafe { collect_matches(matches) }
}

/// Expand the wildcard `value` through the suffix index, returning the matching
/// tags, capped at `max_prefix_expansions`.
///
/// Returns [`BAD_POINTER`] when the pattern has no literal token to anchor on
/// (`*`, `?*`, and friends): there is nothing for the suffix index to look up, so
/// the caller must fall back to a brute-force scan of the tag trie. NULL means
/// the pattern was anchorable but matched nothing.
///
/// Otherwise as [`Rust_TagIndex_GetSuffixMatches`], including who owns what.
///
/// # Safety
///
/// As [`Rust_TagIndex_GetSuffixMatches`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_TagIndex_GetSuffixWildcardMatches(
    tag_index: *const RustTagIndex,
    value: *const c_char,
    len: usize,
    timeout: timespec,
    max_prefix_expansions: std::ffi::c_longlong,
    skip_timeout_checks: bool,
) -> *mut *mut c_char {
    debug_assert!(!tag_index.is_null(), "tag_index must not be null");
    debug_assert!(
        // SAFETY: contract 1 — the handle is live.
        unsafe { Rust_TagIndex_HasSuffix(tag_index) },
        "suffix expansion needs an index created WITHSUFFIXTRIE"
    );

    // SAFETY: contract 2.
    let pattern = unsafe { borrow_tag(value.cast(), len) };
    let Ok(pattern) = SuffixWildcardPattern::new(pattern) else {
        return BAD_POINTER as *mut *mut c_char;
    };

    let query = SuffixQuery::Wildcard {
        pattern: &pattern,
        max_prefix_expansions: max_prefix_expansions as u64,
    };
    let deadline = (!skip_timeout_checks).then_some(timeout);

    // SAFETY: contract 1 — the handle is live and has a suffix index.
    let matches = dispatch!(tag_index, |idx| idx.suffix_expand(query, deadline));
    // SAFETY: the module allocator is up by the time queries run.
    unsafe { collect_matches(matches) }
}
