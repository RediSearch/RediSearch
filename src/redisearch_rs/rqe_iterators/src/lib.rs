/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Stub `AREQ_CheckTimedOut` for lib unit tests so the linker doesn't pull
// `query.c.o` (and its C/coord/SSL transitive closure) from
// `libredisearch_all.a`. The flag is only ever set by Redis on the main
// thread, which doesn't exist here. Integration tests use the real symbol.
#[cfg(test)]
#[unsafe(no_mangle)]
unsafe extern "C" fn AREQ_CheckTimedOut(_areq: *mut ffi::AREQ) -> bool {
    false
}

use std::ptr::NonNull;
use std::sync::OnceLock;

use ref_mode::{Active, Ref};
use rqe_core::{DocId, FieldIndex};
use thiserror::Error;

use ::inverted_index::{FieldMask, NumericFilter};
use index_result::{RSIndexResult, RawIndexResult};
pub use query_error::QueryError;
use query_term::RSQueryTerm;

pub mod boxed;
pub mod c2rust;
pub mod config;
pub mod deferred;
pub mod empty;
pub mod expiration_checker;
pub mod geo_shape;
pub mod id_list;
pub mod id_list_lazy;
pub mod interop;
pub mod intersection;
pub mod inverted_index;
pub mod maybe_empty;
pub mod metric;
pub mod metric_lazy;
pub mod not;
pub mod not_optimized;
pub mod not_reducer;
pub mod optional;
pub mod optional_optimized;
pub mod optional_reducer;
pub mod profile;
pub mod profile_print;
pub mod resume_outcome;
pub mod union;
mod union_flat;
mod union_heap;
pub mod union_opaque;
pub mod union_reducer;
mod union_trimmed;
pub mod utils;
pub mod wildcard;

pub use boxed::{
    RQEDynIterator, RQEDynSuspendedIterator, RQESuspendedIterator, TypeErasedRQEIterator,
    TypeErasedRQESuspendedIterator,
};
pub use config::IteratorsConfig;
pub use empty::Empty;
pub use expiration_checker::{ExpirationChecker, FieldExpirationChecker, NoOpChecker};
pub use geo_shape::{GeoShape, MemTracker, NoTracker};
pub use id_list::IdList;
pub use id_list_lazy::IdListLazy;
pub use intersection::{Intersection, NewIntersectionIterator, new_intersection_iterator};
pub use inverted_index::{
    GeoRangeError, InvalidGeoInput, Missing, Numeric, NumericIteratorVariant, Tag, Term,
    build_geo_numeric_filters, build_geo_range_iterator, build_numeric_filter_iterator,
    extract_geo_unit_factor, new_geo_range_iterator, open_numeric_or_geo_index,
};
pub use metric::Metric;
pub use metric_lazy::MetricLazy;
pub use resume_outcome::ResumeOutcome;
pub use rqe_iterator_type::IteratorType;
pub use union::{
    Union, UnionFlat, UnionFullFlat, UnionFullHeap, UnionHeap, UnionQuickFlat, UnionQuickHeap,
    UnionTrimmed,
};
pub use union_opaque::{UnionOpaque, UnionVariant};
pub use wildcard::{NewWildcardIterator, Wildcard, WildcardIterator};

#[derive(Debug)]
/// The outcome of [`RQEIterator::skip_to`], generic over the [`Ref`] mode.
pub enum SkipToOutcomeRaw<'iterator, 'query, Rf: Ref> {
    /// The iterator has a valid entry for the requested `doc_id`.
    Found(&'iterator mut RawIndexResult<'query, Rf>),

    /// The iterator doesn't have an entry for the requested `doc_id`, but there are entries with an id greater than the requested one.
    NotFound(&'iterator mut RawIndexResult<'query, Rf>),
}

/// Manual `PartialEq` impl with a transitive bound on
/// `RawIndexResult<'query, Rf>: PartialEq` — only [`Active`] satisfies this
/// (see [`ref_mode`]).
impl<'iterator, 'query, Rf: Ref> PartialEq for SkipToOutcomeRaw<'iterator, 'query, Rf>
where
    RawIndexResult<'query, Rf>: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Found(a), Self::Found(b)) => a == b,
            (Self::NotFound(a), Self::NotFound(b)) => a == b,
            _ => false,
        }
    }
}

/// The outcome of [`RQEIterator::skip_to`] when the iterator holds [`Active`]
/// references into the index. This is the only instantiation that's
/// constructible from trait-impl code today; the more general
/// [`SkipToOutcomeRaw`] exists so the iterator structs can store function
/// pointers whose signatures are uniform across `Active`/`Suspended`
/// instantiations.
pub type SkipToOutcome<'iterator, 'index> = SkipToOutcomeRaw<'iterator, 'index, Active<'index>>;

#[derive(Debug, Error)]
/// An iterator failure indications
pub enum RQEIteratorError {
    /// The iterator has reached the time limit for execution.
    #[error("reached time limit")]
    TimedOut,
    /// Iterator failed to read from the inverted index.
    #[error("failed to read from inverted index")]
    IoError(#[from] std::io::Error),
}

/// Trait providing the iterators API.
///
/// Every iterator is also [`ProfilePrint`](profile_print::ProfilePrint), so a
/// type-erased iterator can report its real `FT.PROFILE` output through the
/// [`RQEDynIterator`] vtable rather than a generic placeholder.
pub trait RQEIterator<'index>: profile_print::ProfilePrint + 'index {
    /// The suspended counterpart of this iterator. Carries no live
    /// references into the *index* (those are weakened to raw pointers on
    /// suspend), but may still borrow query-pipeline data for `'index` — see
    /// the `'query` parameter on [`RQESuspendedIterator`]. It can therefore be
    /// held across a lock release/reacquire cycle: the index pointers are
    /// re-validated on resume, while the query-pipeline borrows stay live.
    type Suspended: RQESuspendedIterator<'index> + 'index;

    /// Transition to the suspended state.
    ///
    /// Implementations should perform a pure pointer cast of the box:
    /// the active and suspended types are `#[repr(C)]` layout-compatible
    /// over [`SharedPtr`](ref_mode::SharedPtr) (a `#[repr(transparent)]`
    /// `NonNull`) fields, so the same heap allocation can be relabelled as
    /// the suspended type without reallocation. Preserving the heap address
    /// is what keeps composite aggregate-result pointers valid across the
    /// cycle.
    fn suspend(self: Box<Self>) -> Box<Self::Suspended>;

    /// Return the current [`RSIndexResult`] stored within this [`RQEIterator`].
    ///
    /// Calls to [`read`](Self::read) and [`skip_to`](Self::skip_to) also return
    /// this reference. Sometimes however, especially in the case of wrapper
    /// iterators, you might not have an immediate use for the actual result,
    /// and would instead want to keep it aside for later in time. The child
    /// iterator already has that result anyway, and it is this method which
    /// provides the ability to expose it (for later use).
    ///
    /// # Usage
    ///
    /// Calling this method before the first [`read`](Self::read) or [`skip_to`](Self::skip_to),
    /// or directly after [`rewind`](Self::rewind) will return a default result
    /// without meaningful data.
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>>;

    /// Read the next entry from the iterator.
    ///
    /// On a successful read, the iterator must set its [`last_doc_id`](Self::last_doc_id) property to the new current result id.
    /// This function returns Ok with the current result for valid results, or None if the iterator is depleted.
    /// The function will return Err(RQEIteratorError) for any error.
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>;

    /// Skip to the next record in the iterator with an ID greater or equal to the given `docId`.
    ///
    /// It is assumed that when [`skip_to`](Self::skip_to) is called, `self.last_doc_id() < doc_id`.
    ///
    /// On a successful read, the iterator must set its [`last_doc_id`](Self::last_doc_id) property to the new current result id.
    ///
    /// Return `Ok(`[`SkipToOutcome::Found`]`)` if the iterator has found a record with the `docId` and `Ok(`[`SkipToOutcome::NotFound`]`)`
    /// if the iterator found a result greater than `docId`. `None` will be returned if the iterator has reached the end of the index.
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError>;

    /// Rewind the iterator to the beginning and reset its properties.
    fn rewind(&mut self);

    /// Returns an upper-bound estimation for the number of results the iterator is going to yield.
    fn num_estimated(&self) -> usize;

    /**************** properties ****************/

    /// Returns the last doc id that was read or skipped to.
    fn last_doc_id(&self) -> DocId;

    /// Returns `false` if the iterator can yield more results.
    /// The iterator implementation must ensure that [`at_eof`](Self::at_eof) returns `true`
    /// when [`read`](Self::read) would return `Ok(None)`.
    fn at_eof(&self) -> bool;

    /// Returns the [`IteratorType`] of this iterator.
    fn type_(&self) -> IteratorType;

    /// Returns `Some(&self)` if this iterator is a [`c2rust::CRQEIterator`], `None` otherwise.
    ///
    /// Used by [`Intersection`] to compute sort weights without requiring `'static`.
    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator> {
        None
    }

    /// Returns the sort weight for this iterator when used as a child of an [`Intersection`].
    ///
    /// [`Intersection`] uses this to order its children before execution: a lower value makes
    /// this iterator act as the pivot (minimising `SkipTo` calls). The final sort key is
    /// `num_estimated * intersection_sort_weight(...)`.
    ///
    /// Implementers:
    /// - [`Intersection`]: `1.0 / num_children` — fewer children means tighter selectivity.
    /// - [`Union`]: `num_children` when `prioritize_union_children`, else `1.0`.
    /// - Everything else: `1.0` — neutral, no influence.
    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64;
}

/// [`RQEIterator`] impl for boxed iterators, including type-erased `dyn` variants.
///
/// All methods delegate through the vtable to the concrete type's implementation.
impl<'index, I: RQEIterator<'index> + 'index> RQEIterator<'index> for Box<I> {
    type Suspended = I::Suspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // The receiver `Box<Box<I>>` is unwrapped via `*self` to recover the
        // inner `Box<I>` by value, then dispatched through the underlying
        // iterator's `suspend` method.
        <I as RQEIterator<'index>>::suspend(*self)
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (**self).current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (**self).read()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        (**self).skip_to(doc_id)
    }

    fn rewind(&mut self) {
        (**self).rewind()
    }

    fn num_estimated(&self) -> usize {
        (**self).num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        (**self).last_doc_id()
    }

    fn at_eof(&self) -> bool {
        (**self).at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        (**self).type_()
    }

    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator> {
        (**self).as_c_iterator()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        (**self).intersection_sort_weight(prioritize_union_children)
    }
}

/// Global holder for APIs to get iterators for SearchEnterprise. This allows `rqe_iterators`
/// to get access to iterators it does not know about.
pub static SEARCH_ENTERPRISE_ITERATORS: OnceLock<Box<dyn SearchEnterpriseIterators>> =
    OnceLock::new();

/// A trait to allow SearchEnterprise to provide iterators for on-disk search. The actual
/// implementation will provide iterators `rqe_iterators` does not know about.
///
/// Each iterator constructor requires a `snapshot` handle. It must be a
/// [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) returned from the disk API's
/// `createSnapshot` for the same `index`, and it must remain valid for the lifetime of the
/// returned iterator. This is what guarantees every iterator created for one query observes
/// the same database state — there is no live-database fallback.
pub trait SearchEnterpriseIterators: Send + Sync {
    /// Iterate over all the documents in the index. Each document in the iterator will have the
    /// given weight.
    ///
    /// On failure, the implementation populates `status` (when present) with the cause before
    /// returning `Err`.
    fn new_wildcard_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        weight: f64,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
        status: Option<&mut QueryError>,
    ) -> Result<crate::boxed::TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>>;

    /// Iterate over all the terms in the index, loading offset data for each document.
    ///
    /// Each document in the iterator will have the term inside the given `query_term` and will
    /// have the given weight. The iterator will also filter the results according to the given
    /// field mask. Use this variant for phrase queries, slop constraints, or any query that needs
    /// term positions.
    fn new_term_on_disk_with_offsets<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        query_term: Box<RSQueryTerm>,
        field_mask: FieldMask,
        weight: f64,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<crate::boxed::TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>>;

    /// Iterate over all the terms in the index, skipping offset data for efficiency.
    ///
    /// Each document in the iterator will have the term inside the given `query_term` and will
    /// have the given weight. The iterator will also filter the results according to the given
    /// field mask. Use this variant for BM25_STD queries or any query that doesn't need term
    /// positions.
    fn new_term_on_disk_without_offsets<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        query_term: Box<RSQueryTerm>,
        field_mask: FieldMask,
        weight: f64,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<crate::boxed::TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>>;

    /// Iterate over all the tags (tokens) in the index at the given field index. Each document in
    /// then iterator will have the given weight.
    fn new_tag_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        token: &ffi::RSToken,
        field_index: FieldIndex,
        weight: f64,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<crate::boxed::TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>>;

    /// Iterate over the entries of the numeric index at the given field index whose value
    /// matches `filter`.
    fn new_numeric_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        filter: &NumericFilter,
        field_index: FieldIndex,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<crate::boxed::TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>>;
}
