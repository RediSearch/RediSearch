/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ptr::NonNull, sync::OnceLock};

use ffi::{IndexSpec, t_docId};
use thiserror::Error;

use ::inverted_index::{RSIndexResult, t_fieldMask};
use query_term::RSQueryTerm;

pub mod c2rust;
pub mod empty;
pub mod expiration_checker;
pub mod id_list;
pub mod interop;
pub mod intersection;
pub mod inverted_index;
pub mod maybe_empty;
pub mod metric;
pub mod not;
pub mod not_optimized;
pub mod not_reducer;
pub mod optional;
pub mod optional_optimized;
pub mod optional_reducer;
pub mod profile;
pub mod union;
mod union_flat;
mod union_heap;
pub mod union_opaque;
pub mod union_reducer;
mod union_trimmed;
pub mod utils;
pub mod wildcard;

pub use empty::Empty;
pub use expiration_checker::{ExpirationChecker, FieldExpirationChecker, NoOpChecker};
pub use id_list::IdList;
pub use intersection::{Intersection, NewIntersectionIterator, new_intersection_iterator};
pub use inverted_index::{
    GeoRangeError, InvalidGeoInput, Missing, Numeric, NumericIteratorVariant, Tag, Term,
    build_geo_numeric_filters, extract_geo_unit_factor, new_geo_range_iterator,
    open_numeric_or_geo_index,
};
pub use not::NotIterator;
pub use optional::OptionalIterator;
pub use rqe_iterator_type::IteratorType;
pub use union::{
    Union, UnionFlat, UnionFullFlat, UnionFullHeap, UnionHeap, UnionQuickFlat, UnionQuickHeap,
    UnionTrimmed,
};
pub use union_opaque::{UnionOpaque, UnionVariant};
pub use wildcard::{NewWildcardIterator, Wildcard, WildcardIterator};

#[derive(Debug, PartialEq)]
/// The outcome of [`RQEIterator::skip_to`].
pub enum SkipToOutcome<'iterator, 'index> {
    /// The iterator has a valid entry for the requested `doc_id`.
    Found(&'iterator mut RSIndexResult<'index>),

    /// The iterator doesn't have an entry for the requested `doc_id`, but there are entries with an id greater than the requested one.
    NotFound(&'iterator mut RSIndexResult<'index>),
}

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

#[derive(Debug, PartialEq)]
/// The status of the iterator after a call to [`revalidate`](RQEIterator::revalidate)
pub enum RQEValidateStatus<'iterator, 'index> {
    /// The iterator is still valid and at the same position.
    Ok,
    /// The iterator is still valid but its internal state has changed.
    Moved {
        /// The new current document the iterator is at, or `None` if the iterator is at EOF.
        current: Option<&'iterator mut RSIndexResult<'index>>,
    },
    /// The iterator is no longer valid, and should not be used or rewound. Should be dropped.
    Aborted,
}

/// Trait providing the iterators API.
pub trait RQEIterator<'index> {
    /// Return the current [`RSIndexResult`] stored within this [`RQEIterator`].
    ///
    /// Calls to [`read`](Self::read), [`skip_to`](Self::skip_to) and
    /// [`revalidate`](Self::revalidate) (moved case) also return this reference.
    /// Sometimes however, especially in the case of wrapper iterators, you might
    /// not have an immediate use for the actual result, and would instead want to keep it aside
    /// for later in time. The child iterator already has that result anyway,
    /// and it is this method which provides the ability to expose it (for later use).
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
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError>;

    /// Called when the iterator is being revalidated after a concurrent index change.
    ///
    /// The iterator should check if it is still valid.
    ///
    /// # Safety
    /// `spec` must point to a valid [`IndexSpec`] for the duration of the call.
    unsafe fn revalidate(
        &mut self,
        spec: NonNull<IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError>;

    /// Rewind the iterator to the beginning and reset its properties.
    fn rewind(&mut self);

    /// Returns an upper-bound estimation for the number of results the iterator is going to yield.
    fn num_estimated(&self) -> usize;

    /**************** properties ****************/

    /// Returns the last doc id that was read or skipped to.
    fn last_doc_id(&self) -> t_docId;

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

/// Blanket [`RQEIterator`] impl for `Box<I>` where `I` is a concrete iterator type.
///
/// All core methods delegate to the inner iterator.
impl<'index, I: RQEIterator<'index> + 'index> RQEIterator<'index> for Box<I> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (**self).current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (**self).read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        (**self).skip_to(doc_id)
    }

    unsafe fn revalidate(
        &mut self,
        spec: NonNull<IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Delegating to inner iterator with the same `spec` passed by our caller.
        unsafe { (**self).revalidate(spec) }
    }

    fn rewind(&mut self) {
        (**self).rewind()
    }

    fn num_estimated(&self) -> usize {
        (**self).num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
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

/// [`RQEIterator`] impl for type-erased iterators.
///
/// All methods — including profiling — delegate through the vtable to the
/// concrete type's implementation.
impl<'index> RQEIterator<'index> for Box<dyn RQEIterator<'index> + 'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (**self).current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (**self).read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        (**self).skip_to(doc_id)
    }

    unsafe fn revalidate(
        &mut self,
        spec: NonNull<IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Delegating to inner iterator with the same `spec` passed by our caller.
        unsafe { (**self).revalidate(spec) }
    }

    fn rewind(&mut self) {
        (**self).rewind()
    }

    fn num_estimated(&self) -> usize {
        (**self).num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
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
pub trait SearchEnterpriseIterators: Send + Sync {
    /// Iterate over all the documents in the index. Each document in the iterator will have the
    /// given weight.
    fn new_wildcard_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>>;

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
        field_mask: t_fieldMask,
        weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>>;

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
        field_mask: t_fieldMask,
        weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>>;

    /// Iterate over all the tags (tokens) in the index at the given field index. Each document in
    /// then iterator will have the given weight.
    fn new_tag_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        token: &ffi::RSToken,
        field_index: ffi::t_fieldIndex,
        weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>>;
}
