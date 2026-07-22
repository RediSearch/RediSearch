/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`IndexRef`] — non-owning borrow of a `VecSimIndex` — plus the two
//! adhoc-BF helpers it can produce: [`SharedLockGuard`] for RAM indexes and
//! [`AdhocBfCtx`] for disk indexes.

use std::{ffi::c_void, marker::PhantomData, ptr::NonNull};

use crate::{BatchIterator, QueryError, QueryReply, ReplyOrder};
use ffi::{
    VecSim_Normalize, VecSimAdhocBfCtx, VecSimBool_VecSimBool_TRUE, VecSimIndex,
    VecSimIndex_AdhocBfCtx_Free, VecSimIndex_AdhocBfCtx_GetDistanceFrom,
    VecSimIndex_AdhocBfCtx_GetExactDistances, VecSimIndex_AdhocBfCtx_New, VecSimIndex_BasicInfo,
    VecSimIndex_GetDistanceFrom_Unsafe, VecSimIndex_IndexSize, VecSimIndex_PreferAdHocSearch,
    VecSimIndex_TopKQuery, VecSimMetric_VecSimMetric_Cosine, VecSimParams_GetQueryBlobSize,
    VecSimQueryParams, VecSimTieredIndex_AcquireSharedLocks, VecSimTieredIndex_ReleaseSharedLocks,
    VecSimType_VecSimType_BFLOAT16, VecSimType_VecSimType_FLOAT16, VecSimType_VecSimType_FLOAT32,
    VecSimType_VecSimType_FLOAT64, VecSimType_VecSimType_INT8, VecSimType_VecSimType_INT32,
    VecSimType_VecSimType_INT64, VecSimType_VecSimType_UINT8,
};
use rqe_core::DocId;

/// A non-owning reference to a `VecSimIndex`.
///
/// The C side owns the index and must keep it live for at least `'index`.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct IndexRef<'index> {
    /// Borrowed VecSim index pointer.
    ///
    /// # Invariant
    ///
    /// Valid and not freed for the entire `'index` lifetime.
    inner: NonNull<VecSimIndex>,
    _marker: PhantomData<&'index VecSimIndex>,
}

/// A query blob whose length matches the index it was built for: exactly
/// `dim × bytes-per-element` bytes, both taken from the index's immutable
/// metadata.
///
/// The length match is a type invariant that safe query methods rely on
/// without re-validating, so [`QueryVector::new`] is `unsafe`.
///
/// `'index` ties the blob to the index *lifetime*, not its *identity*: it does
/// not prevent using the blob with a different index of the same lifetime.
pub struct QueryVector<'index> {
    /// Query blob, laid out as the index expects.
    blob: Vec<u8>,
    _marker: PhantomData<&'index VecSimIndex>,
}

impl<'index> QueryVector<'index> {
    /// Wrap `blob` as a query vector for `index`.
    ///
    /// # Safety
    ///
    /// `blob` must have the length required by [`QueryVector`] for `index`, laid
    /// out as the index expects (e.g. `f32`s for a 32-bit-float index).
    pub unsafe fn new(index: IndexRef<'index>, blob: Vec<u8>) -> Self {
        debug_assert_eq!(
            blob.len(),
            expected_blob_len(index.inner),
            "query_vector length does not match the index's expected blob size"
        );
        Self {
            blob,
            _marker: PhantomData,
        }
    }

    /// The query blob, laid out as the index expects.
    pub fn as_bytes(&self) -> &[u8] {
        &self.blob
    }
}

/// Returns the byte length every [`QueryVector`] must have for `ptr`.
///
/// Multiplies the index's immutable `dim` by its per-element size. Intended for
/// `debug_assert!` checks only; not for use on hot paths in release builds.
fn expected_blob_len(ptr: NonNull<VecSimIndex>) -> usize {
    // SAFETY: `ptr` is a valid VecSimIndex for at least as long as the caller's borrow.
    let info = unsafe { VecSimIndex_BasicInfo(ptr.as_ptr()) };
    let bytes_per_elem: usize = match info.type_ {
        t if t == VecSimType_VecSimType_FLOAT32 => 4,
        t if t == VecSimType_VecSimType_FLOAT64 => 8,
        t if t == VecSimType_VecSimType_BFLOAT16 => 2,
        t if t == VecSimType_VecSimType_FLOAT16 => 2,
        t if t == VecSimType_VecSimType_INT8 => 1,
        t if t == VecSimType_VecSimType_UINT8 => 1,
        t if t == VecSimType_VecSimType_INT32 => 4,
        t if t == VecSimType_VecSimType_INT64 => 8,
        _ => panic!("unknown VecSimType: {}", info.type_),
    };
    info.dim * bytes_per_elem
}

/// Query blob for per-id distance lookups.
enum PreparedAdhocQueryBlob {
    /// Cosine: a freshly normalized copy owned by the guard.
    Normalized(Vec<u8>),
    /// Non-cosine: aliases the caller's blob with no copy, valid for the guard's
    /// lifetime per `acquire_shared_locks`' safety contract.
    Borrowed(NonNull<u8>),
}

impl PreparedAdhocQueryBlob {
    /// Pointer to the query blob, valid for the bytes VecSim reads on a distance
    /// lookup. For [`Borrowed`](Self::Borrowed) this is the caller's blob, valid
    /// per the `acquire_shared_locks` safety contract.
    const fn ptr(&self) -> *const c_void {
        match self {
            PreparedAdhocQueryBlob::Normalized(blob) => blob.as_ptr().cast::<c_void>(),
            PreparedAdhocQueryBlob::Borrowed(ptr) => ptr.as_ptr().cast_const().cast::<c_void>(),
        }
    }
}

impl<'index> IndexRef<'index> {
    /// Create an `IndexRef` from a non-null pointer.
    ///
    /// # Safety
    ///
    /// The caller must establish the [`inner`](Self::inner) field invariant
    /// for the chosen `'index` lifetime: `ptr` must point to a valid VecSim
    /// index that is not freed for the entire `'index` lifetime.
    pub const unsafe fn from_raw(ptr: NonNull<VecSimIndex>) -> Self {
        Self {
            inner: ptr,
            _marker: PhantomData,
        }
    }

    /// Number of vectors currently in the index.
    pub fn size(&self) -> usize {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimIndex_IndexSize(self.inner.as_ptr()) }
    }

    /// Whether VecSim stores this index's vectors on disk.
    pub fn is_disk(&self) -> bool {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimIndex_BasicInfo(self.inner.as_ptr()) }.isDisk
    }

    /// Whether the disk-HNSW query in `params` requests reranking of the
    /// adhoc-BF top-k with exact distances, or `None` if this is not a disk
    /// index.
    ///
    /// Only an explicitly enabled preference yields `Some(true)`; a disabled or
    /// unset preference yields `Some(false)`.
    pub fn disk_rerank(&self, params: &VecSimQueryParams) -> Option<bool> {
        if !self.is_disk() {
            return None;
        }
        // SAFETY: `is_disk()` holds, so `hnswDiskRuntimeParams` is the live
        // variant of the runtime-params union.
        let raw = unsafe { params.__bindgen_anon_1.hnswDiskRuntimeParams.shouldRerank };
        Some(raw == VecSimBool_VecSimBool_TRUE)
    }

    /// VecSim heuristic: should adhoc-BF be preferred over batch iteration
    /// for the given filter-subset size and `k`?
    pub fn prefer_adhoc(&self, subset_size: usize, k: usize, initial_check: bool) -> bool {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimIndex_PreferAdHocSearch(self.inner.as_ptr(), subset_size, k, initial_check) }
    }

    /// Run a single-shot top-`k` query.
    ///
    /// `params` is mutated by VecSim (e.g. it reads/writes `timeoutCtx`). See
    /// [`QueryReply::from_raw_checked`] for the timeout/null handling contract.
    pub fn top_k_query(
        &self,
        query_vector: &QueryVector<'index>,
        k: usize,
        params: &mut VecSimQueryParams,
        order: ReplyOrder,
    ) -> Result<Option<QueryReply>, QueryError> {
        // SAFETY:
        // 1. `self.inner` upholds its invariant.
        // 2. `query_vector`'s blob is sized to the index by the `QueryVector`
        //    invariant, so the pointer is valid for the bytes VecSim reads.
        // 3. `params` is exclusively borrowed for this call.
        let raw = unsafe {
            VecSimIndex_TopKQuery(
                self.inner.as_ptr(),
                query_vector.as_bytes().as_ptr().cast::<c_void>(),
                k,
                params,
                order.as_raw(),
            )
        };
        // SAFETY: `raw` is the freshly returned reply pointer from VecSim.
        unsafe { QueryReply::from_raw_checked(raw, order) }
    }

    /// Create a batch iterator, taking `params` as a raw pointer so the caller
    /// picks `'params`. Use this to store the iterator alongside the
    /// `VecSimQueryParams` it was built from.
    ///
    /// # Safety
    ///
    /// - `params` must point to a valid `VecSimQueryParams` for this call.
    /// - The `timeoutCtx` referenced by `*params` must remain valid for the
    ///   whole of `'params`, since the iterator copies it and reads it on every
    ///   [`BatchIterator::next`] call.
    pub unsafe fn batch_iterator_unchecked<'params>(
        &self,
        query_vector: &QueryVector<'index>,
        params: *mut VecSimQueryParams,
    ) -> Option<BatchIterator<'index, 'params>> {
        // SAFETY:
        // 1. `self.inner` upholds its invariant.
        // 2. `query_vector`'s blob is sized to the index by the `QueryVector`
        //    invariant, so the pointer is valid for the bytes VecSim reads.
        // 3. `params` is a valid pointer for this call per the caller's obligation.
        let raw = unsafe {
            ffi::VecSimBatchIterator_New(
                self.inner.as_ptr(),
                query_vector.as_bytes().as_ptr().cast::<c_void>(),
                params,
            )
        };
        let ptr = NonNull::new(raw)?;
        // SAFETY: satisfies `BatchIterator::from_raw`:
        // 1. `inner` invariant: `ptr` is non-null and freshly returned by
        //    `VecSimBatchIterator_New`.
        // 2. `'index` is bounded by the index lifetime.
        // 3. `'params` is caller-chosen; the caller guarantees the `timeoutCtx`
        //    referent outlives it.
        Some(unsafe { BatchIterator::from_raw(ptr) })
    }

    /// Acquire VecSim's shared read locks for ad-hoc distance lookups against
    /// `query_vector`, returning a [`SharedLockGuard`].
    ///
    /// VecSim only takes real locks for tiered indexes; on any other index the
    /// underlying `VecSimTieredIndex_AcquireSharedLocks` is a no-op.
    ///
    /// The query is normalized for cosine indexes, so the caller passes a plain
    /// query just like for [`top_k_query`](Self::top_k_query).
    ///
    /// # Safety
    ///
    /// Only non-cosine indexes rely on this — they borrow the blob without
    /// copying (see [`PreparedAdhocQueryBlob::Borrowed`]); cosine indexes own a normalized
    /// copy and are unaffected. For a non-cosine index, `query_vector`'s blob
    /// must uphold [`prepare_query`]'s conditions for the entire lifetime of the
    /// returned [`SharedLockGuard`]:
    ///
    /// 1. stay allocated;
    /// 2. keep a stable address (never reallocated);
    /// 3. stay unmodified.
    pub unsafe fn acquire_shared_locks(
        &self,
        query_vector: &QueryVector<'index>,
    ) -> SharedLockGuard<'index> {
        // SAFETY: the caller upholds conditions (1)–(3) on `query_vector` for
        // the guard's lifetime, which is exactly what `prepare_query`'s
        // `Borrowed` result requires.
        let query = unsafe { self.prepare_query(query_vector) };
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimTieredIndex_AcquireSharedLocks(self.inner.as_ptr()) };
        SharedLockGuard {
            index: *self,
            query,
        }
    }

    /// Create an [`AdhocBfCtx`] that preprocesses `query_vector` once for
    /// repeated ad-hoc distance lookups.
    ///
    /// Returns `None` when the index type does not implement ad-hoc BF, i.e.
    /// VecSim's `newAdhocBfCtx` returns null.
    pub fn adhoc_bf_ctx(&self, query_vector: &QueryVector<'index>) -> Option<AdhocBfCtx<'index>> {
        // SAFETY:
        // 1. `self.inner` upholds its invariant.
        // 2. `query_vector`'s blob is sized to the index by the `QueryVector`
        //    invariant, so the pointer is valid for the bytes VecSim reads.
        let raw = unsafe {
            VecSimIndex_AdhocBfCtx_New(
                self.inner.as_ptr(),
                query_vector.as_bytes().as_ptr().cast::<c_void>(),
            )
        };
        let ptr = NonNull::new(raw)?;
        // SAFETY: `ptr` is non-null and freshly returned by
        // `VecSimIndex_AdhocBfCtx_New`, so the `AdhocBfCtx::inner` invariant
        // holds.
        Some(unsafe { AdhocBfCtx::from_raw(ptr) })
    }

    /// Returns a query blob ready for direct distance lookups against this index.
    ///
    /// Cosine indexes require normalized queries, so cosine inputs are normalized
    /// into an owned [`Normalized`](PreparedAdhocQueryBlob::Normalized) copy (see
    /// [`VecSim_Normalize`]); all other metrics borrow `query_vector` unchanged as
    /// [`Borrowed`](PreparedAdhocQueryBlob::Borrowed), copying nothing.
    ///
    /// # Safety
    ///
    /// The non-cosine [`Borrowed`](PreparedAdhocQueryBlob::Borrowed) result aliases
    /// `query_vector` by raw pointer. For as long as the returned [`PreparedAdhocQueryBlob`] is
    /// used, that blob must:
    ///
    /// 1. stay allocated — VecSim reads it on every distance lookup;
    /// 2. keep a stable address — the stored pointer is never refreshed, so moving
    ///    the buffer (e.g. a reallocation) would dangle it;
    /// 3. stay unmodified — lookups must observe the original query bytes.
    ///
    /// The cosine [`Normalized`](PreparedAdhocQueryBlob::Normalized) result owns its copy and
    /// imposes none of these.
    unsafe fn prepare_query(&self, query_vector: &QueryVector) -> PreparedAdhocQueryBlob {
        let query_vector = query_vector.as_bytes();
        let ptr = self.inner;
        // SAFETY: `self.inner` upholds its invariant.
        let info = unsafe { VecSimIndex_BasicInfo(ptr.as_ptr()) };
        if info.metric != VecSimMetric_VecSimMetric_Cosine {
            // No copy: alias the caller's blob. `NonNull::dangling` is only reached
            // for an empty blob (len 0), which is never dereferenced.
            let ptr = NonNull::new(query_vector.as_ptr().cast_mut()).unwrap_or(NonNull::dangling());
            return PreparedAdhocQueryBlob::Borrowed(ptr);
        }

        // SAFETY: reads only immutable index metadata.
        let blob_len = unsafe { VecSimParams_GetQueryBlobSize(info.type_, info.dim, info.metric) };
        // The normalized blob is at least as large as the input; the extra tail (if
        // any) is the appended norm slot, left zeroed for `VecSim_Normalize` to fill.
        debug_assert!(blob_len >= query_vector.len());
        let mut blob = vec![0u8; blob_len];
        blob[..query_vector.len()].copy_from_slice(query_vector);
        // SAFETY:
        // 1. `blob` is `blob_len` bytes, the size VecSim requires for a normalized
        //    query of this `dim`/`type`, so the in-place normalization (and any
        //    appended norm) stays in bounds.
        // 2. `info.dim` and `info.type_` come from this index's own metadata.
        unsafe { VecSim_Normalize(blob.as_mut_ptr().cast::<c_void>(), info.dim, info.type_) };
        PreparedAdhocQueryBlob::Normalized(blob)
    }
}

/// RAII guard holding the tiered-index shared locks acquired by
/// [`IndexRef::acquire_shared_locks`], bound to the query vector it was
/// acquired for.
///
/// While the guard exists, [`get_distance_from`](Self::get_distance_from) is
/// safe to call; the locks are released when the guard is dropped.
#[must_use = "the shared locks are released when the guard is dropped"]
pub struct SharedLockGuard<'index> {
    index: IndexRef<'index>,
    /// Query blob reused by every [`get_distance_from`](Self::get_distance_from)
    /// call: an owned normalized copy for cosine indexes, or a borrow of the
    /// caller's blob (no copy) for non-cosine — see [`PreparedAdhocQueryBlob`].
    query: PreparedAdhocQueryBlob,
}

impl SharedLockGuard<'_> {
    /// Look up the distance from the indexed vector with label `doc_id` to the
    /// query vector this guard was acquired for. Returns `None` if VecSim
    /// returns `NaN` (label not found).
    pub fn get_distance_from(&self, doc_id: DocId) -> Option<f64> {
        // SAFETY:
        // 1. `self.index.inner` upholds its invariant.
        // 2. The tiered-index shared locks are held for the lifetime of
        //    `self`, satisfying the `_Unsafe` precondition.
        // 3. `self.query` was sized and (for cosine) normalized to match the
        //    index in `acquire_shared_locks`. For the non-cosine `Borrowed`
        //    variant the pointer aliases the caller's blob, whose validity for
        //    `self`'s lifetime is guaranteed by `acquire_shared_locks`' contract.
        let distance = unsafe {
            VecSimIndex_GetDistanceFrom_Unsafe(
                self.index.inner.as_ptr(),
                doc_id as usize,
                self.query.ptr(),
            )
        };
        (!distance.is_nan()).then_some(distance)
    }
}

impl Drop for SharedLockGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.index.inner` upholds its invariant; the locks were
        // acquired in `IndexRef::acquire_shared_locks`.
        unsafe { VecSimTieredIndex_ReleaseSharedLocks(self.index.inner.as_ptr()) };
    }
}

/// Owned disk-index adhoc-BF context, borrowing the index it was created from
/// for `'index`. Freed on [`Drop`].
pub struct AdhocBfCtx<'index> {
    /// Owned VecSim adhoc-BF context handle.
    ///
    /// # Invariant
    ///
    /// Valid (returned by `VecSimIndex_AdhocBfCtx_New`) and not yet freed,
    /// from construction until [`Drop`].
    inner: NonNull<VecSimAdhocBfCtx>,
    /// Ties the context to the `'index` lifetime of the index it was created
    /// from: disk contexts perform label distance lookups against that index
    /// on every [`get_distance_from`](Self::get_distance_from) call, so the
    /// index must outlive the context.
    _marker: PhantomData<&'index VecSimIndex>,
}

impl<'index> AdhocBfCtx<'index> {
    /// # Safety
    ///
    /// The caller must:
    /// 1. establish the [`inner`](Self::inner) field invariant;
    /// 2. ensure the index `ptr` was created from stays valid for the chosen
    ///    `'index` lifetime.
    const unsafe fn from_raw(ptr: NonNull<VecSimAdhocBfCtx>) -> Self {
        Self {
            inner: ptr,
            _marker: PhantomData,
        }
    }

    /// Look up the distance from `doc_id` to the preprocessed query vector.
    /// Returns `None` if VecSim returns `NaN` (label not found).
    pub fn get_distance_from(&self, doc_id: DocId) -> Option<f64> {
        // SAFETY: `self.inner` upholds its invariant.
        let distance =
            unsafe { VecSimIndex_AdhocBfCtx_GetDistanceFrom(self.inner.as_ptr(), doc_id as usize) };
        (!distance.is_nan()).then_some(distance)
    }

    /// Fetch exact FP32 distances for a batch of `labels`, writing one distance
    /// per label into `distances_out`. For disk indexes this reads the full-
    /// precision vectors from disk, recomputing distances that the adhoc scan
    /// approximated from SQ8-quantized data. Entries whose label is not found
    /// are written as `NaN`.
    ///
    /// `labels` and `distances_out` must have the same length; the slice
    /// lengths bound the C call, so it never reads or writes past either.
    pub fn get_exact_distances(&self, labels: &[usize], distances_out: &mut [f64]) {
        debug_assert_eq!(
            labels.len(),
            distances_out.len(),
            "labels and distances_out must have equal length"
        );
        // SAFETY: `self.inner` upholds its invariant. `labels` and
        // `distances_out` are valid for `count` reads/writes respectively, and
        // `count` is their (equal) length, so the C call stays in bounds.
        unsafe {
            VecSimIndex_AdhocBfCtx_GetExactDistances(
                self.inner.as_ptr(),
                labels.as_ptr(),
                distances_out.as_mut_ptr(),
                labels.len(),
            );
        }
    }
}

impl Drop for AdhocBfCtx<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimIndex_AdhocBfCtx_Free(self.inner.as_ptr()) };
    }
}
