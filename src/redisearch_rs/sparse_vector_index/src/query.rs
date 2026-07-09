/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Query processing for sparse vector search.
//!
//! A sparse query is the union of its dimensions' posting lists: every
//! document sharing at least one dimension with the query has a non-zero dot
//! product and can rank in the top K. [`SparseVectorIterator`] walks that
//! union and reduces each document to a single
//! [`SparseScore`](index_result::RSResultKind::SparseScore) result carrying
//! `Σ q_i · d_i`. The top K is chosen downstream, by the result-processor
//! sorter, from the request's `LIMIT`.

use std::{alloc::Layout, ptr::NonNull};

use index_result::{RSIndexResult, RSResultKind, SuspendedIndexResult};
use index_spec::IndexSpecReadGuard;
use inverted_index::{IndexReaderCore, RawIndexReaderCore, numeric::NumericFloatCompression};
use ref_mode::Ref;
use rlookup::{KeyHandleSlot, RLookupKey, RLookupKeyHandle};
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, NoOpChecker, RQEIterator, RQEIteratorError, RQESuspendedIterator, ResumeOutcome,
    SkipToOutcome, UnionFullFlat, UnionFullHeap,
    boxed::{
        ResumeSlotOutcome, finish_resume_in_place, resume_child_slot_in_place,
        suspend_child_slot_in_place,
    },
    inverted_index::{InvIndIterator, RawInvIndIterator},
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use crate::SparseVectorIndex;

type PostingListIterator<'index> =
    InvIndIterator<'index, IndexReaderCore<'index, NumericFloatCompression>, NoOpChecker>;

type SuspendedPostingListIterator<'query> =
    <PostingListIterator<'query> as RQEIterator<'query>>::Suspended;

/// A [`DimensionIterator`] in either mode: `P` is the posting-list iterator's
/// active or suspended form.
///
/// # Invariants
///
/// 1. [`DimensionIterator`] and [`SuspendedDimensionIterator`] are
///    layout-identical, so suspend and resume convert [`it`](Self::it) in
///    place and reinterpret the box. Enforced by a `const _` assertion block.
#[repr(C)]
pub struct RawDimensionIterator<P> {
    /// Boxed so that the result [`DimensionUnion`] points into lives outside
    /// its child vector, which it reborrows mutably while holding those
    /// pointers.
    it: Box<P>,
    /// The index the posting list belongs to. Only dereferenced under the spec
    /// read lock, in [`reseat_on_live_list`](Self::reseat_on_live_list).
    index: NonNull<SparseVectorIndex>,
    /// The dimension whose posting list [`it`](Self::it) reads.
    dim_id: u32,
}

/// An iterator over one dimension's posting list, yielding `(doc_id, d_i)`
/// with the query weight `q_i` as its result weight.
///
/// GC frees the posting list of a dimension it empties, so
/// [`resume`](RQESuspendedIterator::resume) looks the dimension up again and
/// aborts when its list is gone or replaced. The union then drops this child,
/// which contributed nothing to any score.
pub type DimensionIterator<'index> = RawDimensionIterator<PostingListIterator<'index>>;

/// The suspended form of a [`DimensionIterator`].
pub type SuspendedDimensionIterator<'query> =
    RawDimensionIterator<SuspendedPostingListIterator<'query>>;

// Invariant 1 on `RawDimensionIterator`.
const _: () = {
    use std::mem::{align_of, offset_of, size_of};
    type A = DimensionIterator<'static>;
    type S = SuspendedDimensionIterator<'static>;
    assert!(offset_of!(A, it) == offset_of!(S, it));
    assert!(offset_of!(A, index) == offset_of!(S, index));
    assert!(offset_of!(A, dim_id) == offset_of!(S, dim_id));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query, Rf: Ref, RRf: Ref, RA>
    RawDimensionIterator<
        RawInvIndIterator<
            'query,
            Rf,
            RawIndexReaderCore<RRf, NumericFloatCompression>,
            NoOpChecker,
            RA,
        >,
    >
{
    /// Look the dimension up again and reseat the reader on its posting list.
    /// Returns `false`, leaving the reader untouched, when GC removed or
    /// replaced the list.
    ///
    /// # Safety
    ///
    /// 1. [`index`](Self::index) points to a live [`SparseVectorIndex`].
    /// 2. The spec read lock is held from this call until the iterator is next
    ///    parked.
    unsafe fn reseat_on_live_list(&mut self) -> bool {
        // SAFETY: 1. the pointee is allocated, and 2. no writer holds a
        // mutable reference to it while this one lives.
        let index = unsafe { self.index.as_ref() };
        let Some(list) = index.get_dimension(self.dim_id) else {
            return false;
        };
        let ii = list.inner();
        if !self.it.reads_index(ii) {
            return false;
        }
        // SAFETY: 1. `reads_index` held just above. 2. `ii` was resolved through
        // its owner just now, after any write made while the iterator was
        // parked. 3. The read lock (this function's 2.) keeps it allocated and
        // unwritten until the iterator is parked again.
        unsafe { self.it.reseat_reader_index(NonNull::from(ii)) };
        true
    }
}

impl<'index> DimensionIterator<'index> {
    /// Build the iterator over dimension `dim_id` of `index`, with `weight` as
    /// its result weight. Returns `None` when no document has the dimension.
    ///
    /// # Safety
    ///
    /// As [`SparseVectorIterator::new`].
    pub unsafe fn new(index: NonNull<SparseVectorIndex>, dim_id: u32, weight: f32) -> Option<Self> {
        // SAFETY: 1, 2 of `SparseVectorIterator::new`: the pointee lives for
        // `'index` and is not written while this borrow is used.
        let index_ref: &'index SparseVectorIndex = unsafe { index.as_ref() };
        let posting_list = index_ref.get_dimension(dim_id)?;
        let result = RSIndexResult::build_numeric(0.0)
            .weight(f64::from(weight))
            .build();
        Some(Self {
            it: Box::new(InvIndIterator::new(
                posting_list.reader(),
                result,
                NoOpChecker,
            )),
            // Kept as given, not re-derived from `index_ref`: a GC write
            // between parks invalidates every reference derived from a
            // shared borrow, and revalidation must read through a pointer
            // that write did not invalidate.
            index,
            dim_id,
        })
    }
}

impl<'index> RQEIterator<'index> for DimensionIterator<'index> {
    type Suspended = SuspendedDimensionIterator<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw`, so the projection is in bounds.
        let slot = unsafe { &raw mut (*raw).it };
        // SAFETY: `slot` is an initialized field of the exclusively owned
        // allocation, rewritten below; a panic in between leaks the allocation,
        // as only `raw` owns it, rather than dropping the moved-from slot.
        let it = unsafe { slot.read() };
        let suspended = it.suspend();
        // SAFETY: `slot` is aligned and in bounds, and `Box` is pointer-sized
        // whatever its pointee, so the suspended box fits it.
        unsafe {
            slot.cast::<Box<SuspendedPostingListIterator<'index>>>()
                .write(suspended)
        };
        // SAFETY: `it` now holds its suspended form, and `index` and `dim_id`
        // carry no mode: by invariant 1 on `RawDimensionIterator` the
        // allocation is a valid `SuspendedDimensionIterator`.
        unsafe { Box::from_raw(raw.cast::<SuspendedDimensionIterator<'index>>()) }
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    fn rewind(&mut self) {
        self.it.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    fn type_(&self) -> IteratorType {
        self.it.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.it.intersection_sort_weight(prioritize_union_children)
    }
}

impl<'query> RQESuspendedIterator<'query> for SuspendedDimensionIterator<'query> {
    type Resumed<'a>
        = DimensionIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // SAFETY: 1. `index` came from `SparseVectorIterator::new`, whose
        // caller keeps it alive. 2. `guard` witnesses the read lock, which the
        // caller holds until the iterator is parked again.
        if !unsafe { self.reseat_on_live_list() } {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw`, so the projection is in bounds.
        let slot = unsafe { &raw mut (*raw).it };
        // SAFETY: as in `suspend`; `resume` consumes the moved-out value.
        let it = unsafe { slot.read() };
        let outcome = it.resume(guard).map(|outcome| {
            let (it, slot_outcome) = match outcome {
                ResumeOutcome::Ok(it) => (it, ResumeSlotOutcome::Unchanged),
                ResumeOutcome::Moved(it) => (it, ResumeSlotOutcome::Moved),
                ResumeOutcome::Aborted => return ResumeSlotOutcome::Aborted,
            };
            // SAFETY: as in `suspend`.
            unsafe { slot.cast::<Box<PostingListIterator<'a>>>().write(it) };
            slot_outcome
        });
        // SAFETY: 1. `raw` came from `Box::into_raw` and nothing borrows the
        // allocation. 2. `outcome` is the slot's, and `index` and `dim_id` need
        // no drop. 3. Once `it` holds its active form for `'a`, by invariant 1 on
        // `RawDimensionIterator` the allocation is a valid `DimensionIterator<'a>`.
        unsafe { finish_resume_in_place(raw, outcome) }
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }
}

/// The union over the query's dimension iterators. Always a full union (no
/// quick exit), so each result aggregates every dimension present at that
/// document, which the sum needs.
///
/// `#[repr(C, u8)]`, as is [`SuspendedDimensionUnion`], so the two share a tag
/// encoding and suspend and resume convert the payload in place.
///
/// Public only because [`SparseVectorIterator`] names it; not exported.
#[repr(C, u8)]
pub enum DimensionUnion<'index> {
    /// The union [`SparseVectorIterator::new`] picks for few dimensions.
    Flat(UnionFullFlat<'index, DimensionIterator<'index>>),
    /// The union [`SparseVectorIterator::new`] picks for many dimensions.
    Heap(UnionFullHeap<'index, DimensionIterator<'index>>),
}

/// The suspended form of a [`DimensionUnion`], arm for arm. Public for the
/// same reason, through [`SuspendedSparseVectorIterator`].
#[repr(C, u8)]
pub enum SuspendedDimensionUnion<'query> {
    /// The suspended [`Flat`](DimensionUnion::Flat) union.
    Flat(<UnionFullFlat<'query, DimensionIterator<'query>> as RQEIterator<'query>>::Suspended),
    /// The suspended [`Heap`](DimensionUnion::Heap) union.
    Heap(<UnionFullHeap<'query, DimensionIterator<'query>> as RQEIterator<'query>>::Suspended),
}

macro_rules! dispatch {
    ($self:expr, $it:ident => $body:expr) => {
        match $self {
            DimensionUnion::Flat($it) => $body,
            DimensionUnion::Heap($it) => $body,
        }
    };
}

/// The sparse score of a union result: `Σ q_i · d_i` over its children.
///
/// The union always yields an aggregate, even with a single child.
fn score(result: &RSIndexResult<'_>) -> f64 {
    result.as_aggregate().map_or(0.0, |agg| {
        agg.iter()
            .map(|child| child.weight * child.as_numeric().unwrap_or(0.0))
            .sum()
    })
}

/// A [`SparseVectorIterator`] in either mode: `U` is the dimension union's
/// active or suspended form, and `R` the score result's.
///
/// # Invariants
///
/// 1. [`SparseVectorIterator`] and [`SuspendedSparseVectorIterator`] are
///    layout-identical, so suspend and resume convert the union and the result
///    in place and reinterpret the box, keeping the allocation C holds
///    pointers into, such as the
///    [`key_mut_ref`](SparseVectorIterator::key_mut_ref) slot. Enforced by a
///    `const _` assertion block.
#[repr(C)]
pub struct RawSparseVectorIterator<'query, U, R> {
    union: U,
    result: R,
    /// The number of query dimensions that have a posting list.
    num_dimensions: usize,
    /// The number of dimensions in the query, found or not.
    query_dimensions: usize,
    /// Where the score is yielded for `$YIELD_SCORE_AS`, or null when it is not.
    /// Written by the pipeline once the lookup key is resolved, through the
    /// handle [`key_handle`](Self::key_handle) tracks.
    own_key: *mut RLookupKey<'query>,
    key_handle: KeyHandleSlot<'query>,
}

/// Iterator for one `@field:[SPARSE $q]` query node.
///
/// Yields every document that shares a dimension with the query, in doc-ID
/// order, as a [`SparseScore`](index_result::RSResultKind::SparseScore) result whose
/// value is the document's dot product with the query. Reducing at this node
/// keeps the sparse score apart from any text or tag clauses the node is
/// combined with.
pub type SparseVectorIterator<'index> =
    RawSparseVectorIterator<'index, DimensionUnion<'index>, RSIndexResult<'index>>;

/// The suspended form of a [`SparseVectorIterator`].
pub type SuspendedSparseVectorIterator<'query> =
    RawSparseVectorIterator<'query, SuspendedDimensionUnion<'query>, SuspendedIndexResult<'query>>;

// Invariant 1 on `RawSparseVectorIterator`. The union arms carry their own
// proofs.
const _: () = {
    use std::mem::{align_of, offset_of, size_of};
    type A = SparseVectorIterator<'static>;
    type S = SuspendedSparseVectorIterator<'static>;
    assert!(offset_of!(A, union) == offset_of!(S, union));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, num_dimensions) == offset_of!(S, num_dimensions));
    assert!(offset_of!(A, query_dimensions) == offset_of!(S, query_dimensions));
    assert!(offset_of!(A, own_key) == offset_of!(S, own_key));
    assert!(offset_of!(A, key_handle) == offset_of!(S, key_handle));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
    assert!(size_of::<DimensionUnion<'static>>() == size_of::<SuspendedDimensionUnion<'static>>());
    assert!(
        align_of::<DimensionUnion<'static>>() == align_of::<SuspendedDimensionUnion<'static>>()
    );
};

impl<'index> SparseVectorIterator<'index> {
    /// Build the iterator for `query`, a vector already checked and
    /// normalized by [`prepare`](crate::prepare) against `index`.
    ///
    /// Query dimensions with no posting list contribute nothing and are
    /// skipped. Returns `None` when none remain, so the node matches nothing.
    ///
    /// The union is a heap union when the query keeps more than
    /// `min_union_iter_heap` dimensions, and a flat one otherwise.
    ///
    /// # Safety
    ///
    /// 1. `index` points to a live [`SparseVectorIndex`] that outlives the
    ///    iterator (`'index`).
    /// 2. The index is written to only while the iterator is parked, that is
    ///    held across a read-lock release, and
    ///    [`resume`](RQESuspendedIterator::resume) is called under the read lock
    ///    before the iterator is used again.
    /// 3. `index` was not derived from a shared reference that such a write
    ///    would invalidate: pass the owner's own pointer, such as the one the
    ///    field stores.
    pub unsafe fn new(
        index: NonNull<SparseVectorIndex>,
        query: &[(u32, f32)],
        min_union_iter_heap: usize,
    ) -> Option<Self> {
        let children: Vec<DimensionIterator<'index>> = query
            .iter()
            .filter_map(|&(dim_id, weight)| {
                // SAFETY: the caller upholds this function's contract, which is
                // `DimensionIterator::new`'s.
                unsafe { DimensionIterator::new(index, dim_id, weight) }
            })
            .collect();

        if children.is_empty() {
            return None;
        }
        let num_dimensions = children.len();
        let union = if num_dimensions > min_union_iter_heap {
            DimensionUnion::Heap(UnionFullHeap::new(children))
        } else {
            DimensionUnion::Flat(UnionFullFlat::new(children))
        };

        Some(Self {
            union,
            result: RSIndexResult::build_sparse_score(0.0).build(),
            num_dimensions,
            query_dimensions: query.len(),
            own_key: std::ptr::null_mut(),
            key_handle: KeyHandleSlot::new(),
        })
    }

    /// The slot the pipeline writes the `$YIELD_SCORE_AS` lookup key into.
    pub const fn key_mut_ref(&mut self) -> &mut *mut RLookupKey<'index> {
        &mut self.own_key
    }

    /// Attach the handle that tracks [`key_mut_ref`](Self::key_mut_ref),
    /// invalidating any different handle attached before.
    ///
    /// # Safety
    ///
    /// The same contract as [`Metric::set_handle`](rqe_iterators::Metric::set_handle):
    ///
    /// 1. `key_handle` is [`None`], or points to an initialized
    ///    [`RLookupKeyHandle`] that remains alive until this iterator is
    ///    dropped or the handle is replaced.
    /// 2. The `is_valid` field of the previously attached handle, if it differs
    ///    from `key_handle`, must be writable exclusively during this call.
    /// 3. The `is_valid` field of `key_handle` must be writable exclusively
    ///    whenever this iterator invalidates it: when the iterator is dropped or
    ///    the handle is replaced.
    pub unsafe fn set_handle(&mut self, key_handle: Option<NonNull<RLookupKeyHandle<'index>>>) {
        // SAFETY: (1), (2) and (3) are `KeyHandleSlot::set`'s (1), (2) and (3).
        unsafe { self.key_handle.set(key_handle) };
    }

    /// The number of query dimensions that have a posting list.
    pub const fn num_dimensions(&self) -> usize {
        self.num_dimensions
    }

    /// The number of dimensions in the query, including those no document has.
    pub const fn query_dimensions(&self) -> usize {
        self.query_dimensions
    }

    /// Reduce the union's current result into [`result`](Self::result).
    fn load(
        result: &mut RSIndexResult<'index>,
        from: &RSIndexResult<'_>,
        key: *mut RLookupKey<'_>,
    ) {
        let value = score(from);
        result.doc_id = from.doc_id;
        *result
            .as_numeric_mut()
            .expect("the sparse result is built as a sparse score") = value;
        let metrics = result.metrics_mut();
        metrics.reset();
        if !key.is_null() {
            // The metrics vector types the key the way C sees it: the header
            // `RLookupKey` is exported under, which is this key's first field.
            //
            // SAFETY: `key` is non-null per the check above. It is the lookup
            // key the pipeline resolved for this node, which lives in the
            // query's `RLookup` and so outlives every result this iterator
            // yields.
            metrics.push_with_key(unsafe { &*key.cast::<ffi::RLookupKey>() }, value);
        }
    }
}

impl<'index> RQEIterator<'index> for SparseVectorIterator<'index> {
    type Suspended = SuspendedSparseVectorIterator<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw`; the borrow ends with the match.
        match unsafe { &mut (*raw).union } {
            // SAFETY: the payload is valid and exclusively owned, and is read as
            // its suspended form from here on.
            DimensionUnion::Flat(u) => unsafe { suspend_child_slot_in_place(NonNull::from(u)) },
            // SAFETY: as above.
            DimensionUnion::Heap(u) => unsafe { suspend_child_slot_in_place(NonNull::from(u)) },
        }
        // SAFETY: `raw` came from `Box::into_raw`, so the projection is in bounds.
        let result = NonNull::new(unsafe { &raw mut (*raw).result })
            .expect("a field of a non-null allocation is non-null");
        // SAFETY: 1. `result` is aligned and points to an initialized field.
        // 2. It is unaliased: `raw` exclusively owns the allocation, and no
        // reference into it is live.
        unsafe { RSIndexResult::into_suspended_in_place(result) };
        // SAFETY: the union payload and the result now hold their suspended forms,
        // the union tag encodes the same arm in both enums, and the other fields
        // carry no index borrows: by invariant 1 on `RawSparseVectorIterator`
        // the allocation is a valid `SuspendedSparseVectorIterator`.
        unsafe { Box::from_raw(raw.cast::<SuspendedSparseVectorIterator<'index>>()) }
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.at_eof() {
            None
        } else {
            Some(&mut self.result)
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let key = self.own_key;
        let found = dispatch!(&mut self.union, u => u.read()?.map(|r| &*r));
        let Some(from) = found else {
            return Ok(None);
        };
        Self::load(&mut self.result, from, key);
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let key = self.own_key;
        let outcome = dispatch!(&mut self.union, u => match u.skip_to(doc_id)? {
            None => None,
            Some(SkipToOutcome::Found(r)) => Some((true, &*r)),
            Some(SkipToOutcome::NotFound(r)) => Some((false, &*r)),
        });
        let Some((found, from)) = outcome else {
            return Ok(None);
        };
        Self::load(&mut self.result, from, key);
        Ok(Some(if found {
            SkipToOutcome::Found(&mut self.result)
        } else {
            SkipToOutcome::NotFound(&mut self.result)
        }))
    }

    fn rewind(&mut self) {
        dispatch!(&mut self.union, u => u.rewind());
        self.result.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        dispatch!(&self.union, u => u.num_estimated())
    }

    fn last_doc_id(&self) -> DocId {
        dispatch!(&self.union, u => u.last_doc_id())
    }

    fn at_eof(&self) -> bool {
        dispatch!(&self.union, u => u.at_eof())
    }

    fn type_(&self) -> IteratorType {
        IteratorType::SparseVector
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'query> RQESuspendedIterator<'query> for SuspendedSparseVectorIterator<'query> {
    type Resumed<'a>
        = SparseVectorIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // `current()` hands the result out mutably, so a consumer could have
        // swapped in a result that borrows the index, which this iterator cannot
        // resume. Refuse before reinterpreting anything.
        if self.result.kind() != RSResultKind::SparseScore {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw`; the borrow ends with the match.
        // On `Aborted` or `Err` the helper leaves the payload uninitialized,
        // which the teardown below accounts for.
        let outcome = match unsafe { &mut (*raw).union } {
            // SAFETY: the payload is valid and exclusively owned.
            SuspendedDimensionUnion::Flat(u) => unsafe {
                resume_child_slot_in_place(NonNull::from(u), guard)
            },
            // SAFETY: as above.
            SuspendedDimensionUnion::Heap(u) => unsafe {
                resume_child_slot_in_place(NonNull::from(u), guard)
            },
        };
        let moved = match outcome {
            Ok(ResumeSlotOutcome::Unchanged) => false,
            Ok(ResumeSlotOutcome::Moved) => true,
            Ok(ResumeSlotOutcome::Aborted) | Err(_) => {
                // SAFETY: `raw` is exclusively owned and its union slot was
                // consumed; every other field is still initialized.
                unsafe { free_without_union(raw) };
                return outcome.map(|_| ResumeOutcome::Aborted);
            }
        };

        // SAFETY: `raw` came from `Box::into_raw`, so the projection is in bounds.
        let result = NonNull::new(unsafe { &raw mut (*raw).result })
            .expect("a field of a non-null allocation is non-null");
        // SAFETY: 1. and 2. `result` is initialized and unaliased. 3. A sparse
        // score holds no index-backed pointers, as checked on entry; its metrics
        // borrow query-pipeline data, valid for `'a` by `'query: 'a`.
        unsafe { SuspendedIndexResult::into_active_in_place::<'a>(result) };
        // SAFETY: the union payload and the result now hold their active forms
        // for `'a`, the union tag encodes the same arm in both enums, and the
        // other fields carry no index borrows: by invariant 1 on
        // `RawSparseVectorIterator` the allocation is a valid
        // `SparseVectorIterator<'a>`.
        let mut active = unsafe { Box::from_raw(raw.cast::<SparseVectorIterator<'a>>()) };

        if !moved {
            return Ok(ResumeOutcome::Ok(active));
        }
        // Reduce the union's new position into the result.
        let it = &mut *active;
        let key = it.own_key;
        if let Some(from) = dispatch!(&mut it.union, u => u.current().map(|r| &*r)) {
            SparseVectorIterator::load(&mut it.result, from, key);
        }
        Ok(ResumeOutcome::Moved(active))
    }

    fn last_doc_id(&self) -> DocId {
        match &self.union {
            SuspendedDimensionUnion::Flat(u) => RQESuspendedIterator::last_doc_id(u),
            SuspendedDimensionUnion::Heap(u) => RQESuspendedIterator::last_doc_id(u),
        }
    }

    fn num_estimated(&self) -> usize {
        match &self.union {
            SuspendedDimensionUnion::Flat(u) => RQESuspendedIterator::num_estimated(u),
            SuspendedDimensionUnion::Heap(u) => RQESuspendedIterator::num_estimated(u),
        }
    }
}

/// Drop every field of `*raw` but the union, then free the allocation.
///
/// # Safety
///
/// 1. `raw` came from [`Box::into_raw`], and is exclusively owned.
/// 2. The union payload was consumed, while every other field is initialized.
unsafe fn free_without_union(raw: *mut SuspendedSparseVectorIterator<'_>) {
    // SAFETY: 1. `raw` points to a live allocation of this type, so the field
    // place is in bounds.
    let result = unsafe { &raw mut (*raw).result };
    // SAFETY: as above.
    let key_handle = unsafe { &raw mut (*raw).key_handle };
    // SAFETY: 1. and 2. The fields are initialized, and dropped once each. The
    // union's tag needs no drop.
    unsafe { std::ptr::drop_in_place(result) };
    // SAFETY: as above.
    unsafe { std::ptr::drop_in_place(key_handle) };
    // SAFETY: 1. The global allocator allocated it for the `Box` it came from.
    unsafe {
        std::alloc::dealloc(
            raw.cast(),
            Layout::new::<SuspendedSparseVectorIterator<'_>>(),
        )
    };
}

/// Print the profile of a [`SparseVectorIterator`], active or suspended: it
/// reads only the dimension counts, which the two forms share.
fn print_sparse_profile(
    query_dimensions: usize,
    num_dimensions: usize,
    map: &mut redis_reply::MapBuilder<'_>,
    ctx: &mut ProfilePrintCtx<'_>,
) {
    map.kv_simple_string(c"Type", c"SPARSE_VECTOR");
    let as_cstring =
        |n: usize| std::ffi::CString::new(n.to_string()).expect("a number has no interior NUL");
    // The gap between the two is the query's dimensions no document has.
    map.kv_simple_string(c"Query dimensions", &as_cstring(query_dimensions));
    map.kv_simple_string(c"Dimensions", &as_cstring(num_dimensions));
    ctx.print_optional_counters(map);
    ctx.print_estimated(map);
}

impl ProfilePrint for SparseVectorIterator<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        print_sparse_profile(self.query_dimensions, self.num_dimensions, map, ctx);
    }
}

impl ProfilePrint for SuspendedSparseVectorIterator<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        print_sparse_profile(self.query_dimensions, self.num_dimensions, map, ctx);
    }
}

// A dimension is never profiled on its own: its `SparseVectorIterator` prints
// the whole union. This impl only satisfies the `ProfilePrint` bound every
// iterator form carries.
impl<P: ProfilePrint> ProfilePrint for RawDimensionIterator<P> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.it.print_profile(map, ctx);
    }
}
