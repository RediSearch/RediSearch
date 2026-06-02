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

use std::{ffi::c_void, marker::PhantomData, num::NonZeroUsize, ptr::NonNull};

use crate::{BatchIterator, QueryError, QueryReply, ReplyOrder};
use ffi::{
    VecSim_Normalize, VecSimAdhocBfCtx, VecSimIndex, VecSimIndex_AdhocBfCtx_Free,
    VecSimIndex_AdhocBfCtx_GetDistanceFrom, VecSimIndex_AdhocBfCtx_New, VecSimIndex_BasicInfo,
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
/// The C side owns the index and must keep it live for at least `'idx`.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct IndexRef<'idx> {
    /// Borrowed VecSim index pointer.
    ///
    /// # Invariant
    ///
    /// Valid and not freed for the entire `'idx` lifetime.
    inner: NonNull<VecSimIndex>,
    _marker: PhantomData<&'idx VecSimIndex>,
}

/// A query blob whose length matches the index it was built for: exactly
/// `dim × bytes-per-element` bytes (`dim` and the per-element size both come
/// from the index's immutable metadata).
///
/// The length match is a type invariant: every safe method taking a
/// `QueryVector` (e.g. [`IndexRef::top_k_query`], [`IndexRef::batch_iterator`])
/// hands the blob to VecSim, which reads exactly that many bytes, and none of
/// them re-validate. Constructing the value is therefore the sole checkpoint,
/// and [`QueryVector::new`] is `unsafe` for that reason.
///
/// Note: `'idx` brands the blob with the index *lifetime*, not its *identity*.
/// A `QueryVector` built for one index is accepted by any other index of the
/// same layout, so the caller must use it with the intended index.
pub struct QueryVector<'idx> {
    /// Query blob, laid out as the index expects.
    blob: Vec<u8>,
    _marker: PhantomData<&'idx VecSimIndex>,
}

impl<'idx> QueryVector<'idx> {
    /// Wrap `blob` as a query vector for `index`.
    ///
    /// # Safety
    ///
    /// `blob` must have the length required by [`QueryVector`] for `index`, laid
    /// out as the index expects (e.g. `f32`s for a float32 index). A shorter
    /// blob is read out of bounds by VecSim in every safe query method that
    /// consumes the returned value. A `debug_assert!` checks the length in debug
    /// builds only; release builds trust this precondition.
    pub unsafe fn new(index: IndexRef<'idx>, blob: Vec<u8>) -> Self {
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

/// Prepare a query vector for direct distance lookups against this index.
///
/// Unlike a top-k or batch query, a direct distance lookup applies no
/// preprocessing to the query: it compares the bytes as given. Cosine indexes
/// assume both vectors are normalized, so an un-normalized query would produce
/// wrong distances. We therefore normalize the query once, up front, and reuse
/// the result for every per-document lookup.
///
/// For non-cosine metrics the query is returned unchanged. For cosine the
/// result may be slightly larger than the input, because normalization can
/// append bookkeeping (e.g. the precomputed norm) that the index expects.
fn prepare_query(ptr: NonNull<VecSimIndex>, query_vector: &[u8]) -> Vec<u8> {
    // SAFETY: `ptr` is a valid VecSimIndex for at least as long as the caller's borrow.
    let info = unsafe { VecSimIndex_BasicInfo(ptr.as_ptr()) };
    if info.metric != VecSimMetric_VecSimMetric_Cosine {
        return query_vector.to_vec();
    }

    // SAFETY: reads only immutable index metadata.
    let blob_len = unsafe { VecSimParams_GetQueryBlobSize(info.type_, info.dim, info.metric) };
    // The normalized blob is at least as large as the input; the extra tail (if
    // any) is the appended norm slot, left zeroed for `VecSim_Normalize` to fill.
    debug_assert!(blob_len >= query_vector.len());
    let mut blob = vec![0u8; blob_len.max(query_vector.len())];
    blob[..query_vector.len()].copy_from_slice(query_vector);
    // SAFETY:
    // - `blob` is `blob_len` bytes, the size VecSim requires for a normalized
    //   query of this `dim`/`type`, so the in-place normalization (and any
    //   appended norm) stays in bounds.
    // - `info.dim` and `info.type_` come from this index's own metadata.
    unsafe { VecSim_Normalize(blob.as_mut_ptr().cast::<c_void>(), info.dim, info.type_) };
    blob
}

impl<'idx> IndexRef<'idx> {
    /// Create an `IndexRef` from a non-null pointer.
    ///
    /// # Safety
    ///
    /// The caller must establish the [`inner`](Self::inner) field invariant
    /// for the chosen `'idx` lifetime.
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
        query_vector: &QueryVector<'idx>,
        k: NonZeroUsize,
        params: &mut VecSimQueryParams,
        order: ReplyOrder,
    ) -> Result<Option<QueryReply>, QueryError> {
        // SAFETY:
        // - `self.inner` upholds its invariant.
        // - `query_vector`'s blob is sized to the index by the `QueryVector`
        //   invariant, so the pointer is valid for the bytes VecSim reads.
        // - `params` is exclusively borrowed for this call.
        let raw = unsafe {
            VecSimIndex_TopKQuery(
                self.inner.as_ptr(),
                query_vector.as_bytes().as_ptr().cast::<c_void>(),
                k.get(),
                params,
                order.as_raw(),
            )
        };
        // SAFETY: `raw` is the freshly returned reply pointer from VecSim.
        unsafe { QueryReply::from_raw_checked(raw, order) }
    }

    /// Create a batch iterator scoped to `query_vector` and `params`.
    ///
    /// Returns `None` if VecSim returns a null iterator pointer.
    ///
    /// The returned [`BatchIterator`] is bounded by both `'idx` (the index
    /// must remain valid) and `'params` (the `timeoutCtx` referenced inside
    /// `params` must remain valid for the full lifetime of the iterator).
    pub fn batch_iterator<'params>(
        &self,
        query_vector: &QueryVector<'idx>,
        params: &'params mut VecSimQueryParams,
    ) -> Option<BatchIterator<'idx, 'params>> {
        // SAFETY: same argument as `top_k_query`.
        let raw = unsafe {
            ffi::VecSimBatchIterator_New(
                self.inner.as_ptr(),
                query_vector.as_bytes().as_ptr().cast::<c_void>(),
                params,
            )
        };
        let ptr = NonNull::new(raw)?;
        // SAFETY:
        // - `ptr` is non-null and freshly returned by `VecSimBatchIterator_New`,
        //   so the `BatchIterator::inner` invariant holds.
        // - `'idx` is the index lifetime from `IndexRef<'idx>`; the HNSW batch
        //   iterator stores the raw index pointer and uses it on every Next call.
        // - `'params` is bounded by the exclusive borrow of `params`; the iterator
        //   copies `params.timeoutCtx` at construction and reads it on every Next
        //   call, so the timeout-context referent must outlive the iterator.
        // - `query_vector` needs no lifetime tracking: VecSimBatchIterator_New
        //   force-copies the blob (see `BatchIterator::from_raw` safety docs).
        Some(unsafe { BatchIterator::from_raw(ptr) })
    }

    /// Like [`batch_iterator`](Self::batch_iterator), but takes the query
    /// params as a raw pointer so the caller chooses `'params` instead of
    /// borrowing the params storage.
    ///
    /// This is the escape hatch for owners that store the returned iterator
    /// alongside the `VecSimQueryParams` it was built from: a safe
    /// `&'params mut` borrow would tie `'params` to that owner's own fields,
    /// which Rust cannot express. Here the caller picks `'params` and takes on
    /// the obligation below.
    ///
    /// # Safety
    ///
    /// - `params` must point to a valid `VecSimQueryParams` for this call.
    /// - The `timeoutCtx` referenced by `*params` must remain valid for the
    ///   whole of `'params`, since the iterator copies it and reads it on every
    ///   [`BatchIterator::next`] call.
    pub unsafe fn batch_iterator_unchecked<'params>(
        &self,
        query_vector: &QueryVector<'idx>,
        params: *mut VecSimQueryParams,
    ) -> Option<BatchIterator<'idx, 'params>> {
        // SAFETY:
        // - `self.inner` upholds its invariant.
        // - `query_vector`'s blob is sized to the index by the `QueryVector`
        //   invariant, so the pointer is valid for the bytes VecSim reads.
        // - `params` is a valid pointer for this call per the caller's obligation.
        let raw = unsafe {
            ffi::VecSimBatchIterator_New(
                self.inner.as_ptr(),
                query_vector.as_bytes().as_ptr().cast::<c_void>(),
                params,
            )
        };
        let ptr = NonNull::new(raw)?;
        // SAFETY:
        // - `ptr` is non-null and freshly returned by `VecSimBatchIterator_New`,
        //   so the `BatchIterator::inner` invariant holds.
        // - `'idx` is the index lifetime from `IndexRef<'idx>`; the HNSW batch
        //   iterator stores the raw index pointer and uses it on every Next call.
        // - `'params` is chosen by the caller, who guarantees the `timeoutCtx`
        //   referent outlives it.
        // - `query_vector` needs no lifetime tracking: VecSimBatchIterator_New
        //   force-copies the blob (see `BatchIterator::from_raw` safety docs).
        Some(unsafe { BatchIterator::from_raw(ptr) })
    }

    /// Acquire the tiered-index shared locks required for unsafe RAM adhoc
    /// distance lookups against `query_vector`. The returned guard releases the
    /// locks on drop.
    ///
    /// RAM/tiered indexes only: the guard's lookups use the RAM-only distance
    /// path. Disk indexes must use [`adhoc_bf_ctx`](Self::adhoc_bf_ctx) instead,
    /// which preprocesses the query and registers the marker disk lookups need.
    ///
    /// `query_vector` is normalized once here for cosine indexes (see
    /// [`prepare_query`]) and the prepared blob is reused by every
    /// [`get_distance_from`](SharedLockGuard::get_distance_from) call, so the
    /// caller passes a plain (un-normalized) query just like for
    /// [`top_k_query`](Self::top_k_query).
    ///
    /// The guard's lifetime is the index's `'idx`, not the borrow of `self`,
    /// so it can be stored alongside the [`IndexRef`].
    pub fn acquire_ram_shared_locks(
        &self,
        query_vector: &QueryVector<'idx>,
    ) -> SharedLockGuard<'idx> {
        debug_assert!(
            // SAFETY: `self.inner` upholds its invariant.
            !unsafe { VecSimIndex_BasicInfo(self.inner.as_ptr()) }.isDisk,
            "acquire_ram_shared_locks called on a disk index; use adhoc_bf_ctx instead"
        );
        let query = prepare_query(self.inner, query_vector.as_bytes());
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimTieredIndex_AcquireSharedLocks(self.inner.as_ptr()) };
        SharedLockGuard {
            index: *self,
            query,
        }
    }

    /// Create a disk-index adhoc-BF context that preprocesses `query_vector`
    /// once for repeated distance lookups.
    ///
    /// Returns `None` when VecSim does not support adhoc-BF for this index
    /// type (e.g. RAM indexes) and yields a null context.
    ///
    /// The context's lifetime is the index's `'idx`, not the borrow of `self`,
    /// so it can be stored alongside the [`IndexRef`]. For disk indexes the
    /// context keeps performing label distance lookups against the backing
    /// index, so it must not outlive it.
    pub fn adhoc_bf_ctx(&self, query_vector: &QueryVector<'idx>) -> Option<AdhocBfCtx<'idx>> {
        // SAFETY: same argument as `top_k_query`.
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
}

/// RAII guard holding the tiered-index shared locks acquired by
/// [`IndexRef::acquire_ram_shared_locks`], bound to the query vector it was
/// acquired for.
///
/// While the guard exists, [`get_distance_from`](Self::get_distance_from) is
/// safe to call; the locks are released when the guard is dropped.
#[must_use = "the shared locks are released when the guard is dropped"]
pub struct SharedLockGuard<'idx> {
    index: IndexRef<'idx>,
    /// Query blob, normalized once for cosine indexes at construction, reused
    /// by every [`get_distance_from`](Self::get_distance_from) call.
    query: Vec<u8>,
}

impl SharedLockGuard<'_> {
    /// Look up the distance from the indexed vector with label `doc_id` to the
    /// query vector this guard was acquired for. Returns `None` if VecSim
    /// returns `NaN` (label not found).
    pub fn get_distance_from(&self, doc_id: DocId) -> Option<f64> {
        // SAFETY:
        // - `self.index.inner` upholds its invariant.
        // - The tiered-index shared locks are held for the lifetime of
        //   `self`, satisfying the `_Unsafe` precondition.
        // - `self.query` was sized and (for cosine) normalized to match the
        //   index in `acquire_ram_shared_locks`.
        let distance = unsafe {
            VecSimIndex_GetDistanceFrom_Unsafe(
                self.index.inner.as_ptr(),
                doc_id as usize,
                self.query.as_ptr().cast::<c_void>(),
            )
        };
        (!distance.is_nan()).then_some(distance)
    }
}

impl Drop for SharedLockGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.index.inner` upholds its invariant; the locks were
        // acquired in `IndexRef::acquire_ram_shared_locks`.
        unsafe { VecSimTieredIndex_ReleaseSharedLocks(self.index.inner.as_ptr()) };
    }
}

/// Owned disk-index adhoc-BF context, borrowing the index it was created from
/// for `'idx`. Freed on [`Drop`].
pub struct AdhocBfCtx<'idx> {
    /// Owned VecSim adhoc-BF context handle.
    ///
    /// # Invariant
    ///
    /// Valid (returned by `VecSimIndex_AdhocBfCtx_New`) and not yet freed,
    /// from construction until [`Drop`].
    inner: NonNull<VecSimAdhocBfCtx>,
    /// Ties the context to the `'idx` lifetime of the index it was created
    /// from: disk contexts perform label distance lookups against that index
    /// on every [`get_distance_from`](Self::get_distance_from) call, so the
    /// index must outlive the context.
    _marker: PhantomData<&'idx VecSimIndex>,
}

impl<'idx> AdhocBfCtx<'idx> {
    /// # Safety
    ///
    /// The caller must establish the [`inner`](Self::inner) field invariant,
    /// and the index `ptr` was created from must stay valid for the chosen
    /// `'idx` lifetime.
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
}

impl Drop for AdhocBfCtx<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.inner` upholds its invariant.
        unsafe { VecSimIndex_AdhocBfCtx_Free(self.inner.as_ptr()) };
    }
}
