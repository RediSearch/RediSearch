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
// `libredisearch_c_bundle.a`. The flag is only ever set by Redis on the main
// thread, which doesn't exist here. Integration tests use the real symbol.
#[cfg(test)]
#[unsafe(no_mangle)]
unsafe extern "C" fn AREQ_CheckTimedOut(_areq: *mut ffi::AREQ) -> bool {
    false
}

use std::ptr::NonNull;
use std::sync::OnceLock;

use index_spec::IndexSpecReadGuard;
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
    RQEDynIterator, RQEDynSuspendedIterator, RQEIteratorBoxed, RQESuspendedIterator,
    TypeErasedRQEIterator, TypeErasedRQESuspendedIterator,
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
    TermIndexReader, build_geo_numeric_filters, build_geo_range_iterator,
    build_numeric_filter_iterator, build_term_iterator, extract_geo_unit_factor,
    free_geo_numeric_filters, new_geo_range_iterator, open_numeric_or_geo_index,
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
    /// # Contract
    ///
    /// This is a *has-current* oracle: `Some(&mut result)` while the iterator is
    /// positioned on a result, `None` once a [`read`](Self::read) or
    /// [`skip_to`](Self::skip_to) found nothing and left it positioned *past* its
    /// last result. Never a stale result once exhausted — that is what lets
    /// resume-path callers detect a move that landed at EOF.
    ///
    /// [`at_eof`](Self::at_eof) reports the same state inverted, so both are
    /// driven by one piece of information: whether the iterator has run past its
    /// last result. An iterator positioned on its final result has not, and still
    /// owes that result to its caller. The unread state is the one exception —
    /// see `# Usage` below.
    ///
    /// # Usage
    ///
    /// Before the first [`read`](Self::read) or [`skip_to`](Self::skip_to), and
    /// directly after a [`rewind`](Self::rewind), the iterator is positioned
    /// nowhere. Implementations that own a reusable result hand that back,
    /// carrying no meaningful data; ones that materialise a result only on a read
    /// — [`Empty`], or `TopKIterator` — answer `None`. Neither form says anything
    /// about whether there are results to come, and
    /// [`at_eof`](Self::at_eof) is `false` for both.
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
    /// Conversely, a call that carries no result — `None`, or an
    /// [`RQEIteratorError`] — must not leave
    /// [`last_doc_id`](Self::last_doc_id) equal to `doc_id`: an iterator may not
    /// claim the probed document as its position without a result to back it.
    /// Parents pair the two, reading a position equal to the id they asked for as
    /// "the child holds this document, so [`current`](Self::current) has it" —
    /// which a bare position turns into a lie.
    ///
    /// Return `Ok(`[`SkipToOutcome::Found`]`)` if the iterator has found a record with the `docId` and `Ok(`[`SkipToOutcome::NotFound`]`)`
    /// if the iterator found a result greater than `docId`. `None` will be returned if the iterator has reached the end of the index.
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError>;

    /// Called when the iterator is being revalidated after a concurrent index change.
    ///
    /// The iterator should check if it is still valid by comparing its stored state
    /// against the current index state.
    ///
    /// # Exhaustion is terminal
    ///
    /// An implementation that was at [`at_eof`](Self::at_eof) on entry must still be
    /// at it on return — only [`rewind`](Self::rewind) restarts an iterator, and
    /// [`Moved`](RQEValidateStatus::Moved)`{ current: Some(_) }` out of the exhausted
    /// state is forbidden outright. Callers act on exhaustion irreversibly: a
    /// composite drops the children that report it, so one that comes back alive
    /// re-enters a parent that has already moved on without it, replaying documents
    /// from behind the position that parent now holds.
    ///
    /// The trap is restoring a position by re-seeking: [`rewind`](Self::rewind) clears
    /// the past-the-end state, and a re-seek to the last yielded document *finds* it,
    /// so an exhausted iterator silently becomes a live one sitting on a result it has
    /// already handed out. Restore the exhausted position instead of seeking back to
    /// it. The same requirement applies to [`RQESuspendedIterator::resume`].
    ///
    /// # Errors
    ///
    /// Revalidation re-reads and seeks the index to restore the position, so it can fail with an
    /// [`RQEIteratorError`] — [`TimedOut`](RQEIteratorError::TimedOut) or
    /// [`IoError`](RQEIteratorError::IoError) — which is distinct from
    /// [`Aborted`](RQEValidateStatus::Aborted). On `Err` the fix-up is left half-applied: children
    /// may have been repositioned or dropped while the state derived from them was never re-synced.
    /// The iterator is therefore in an indeterminate state, and the caller must drop it rather than
    /// read from it or revalidate it again. This mirrors [`RQESuspendedIterator::resume`], which
    /// consumes the iterator and drops it on the same failure.
    ///
    /// At the FFI boundary the two errors are reported apart: `TimedOut` becomes
    /// `VALIDATE_TIMEOUT`, which tells the C caller the result set is incomplete, while `IoError`
    /// becomes `VALIDATE_ABORTED`, a dead subtree in a query that still has time left.
    ///
    /// # Locking
    ///
    /// The caller must hold the spec read lock, represented by [`IndexSpecReadGuard`].
    /// The lock ensures the spec remains valid and unchanged during this call.
    ///
    /// # Errors
    ///
    /// An error is terminal. It interrupts a fix-up that is already half applied —
    /// children repositioned or dropped, the state derived from them never re-synced —
    /// so [`current`](Self::current), [`at_eof`](Self::at_eof) and
    /// [`last_doc_id`](Self::last_doc_id) stop describing anything, and there is no
    /// earlier state to roll back to either: the position they would be restored to
    /// belongs to an index the iterator no longer sits in.
    ///
    /// Calling any of them afterwards is therefore meaningless rather than merely
    /// stale, and so is [`rewind`](Self::rewind). Drop the iterator instead. Composites
    /// propagate the error rather than handling it, and the C boundary reports
    /// `VALIDATE_ABORTED`, on which the result processor frees the whole tree.
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError>;

    /// Rewind the iterator to the beginning and reset its properties.
    fn rewind(&mut self);

    /// Returns an upper-bound estimation for the number of results the iterator is going to yield.
    fn num_estimated(&self) -> usize;

    /**************** properties ****************/

    /// Returns the last doc id that was read or skipped to.
    fn last_doc_id(&self) -> DocId;

    /// Returns `true` once the iterator has run *past* its last result.
    ///
    /// The negation of [`current`](Self::current), whose contract defines the
    /// state both report. Callers that select the live members of a set rely on
    /// that boundary: a composite rebuilding its active children after a resume
    /// keeps every child that still owes a result, and drops only those with
    /// nothing left.
    ///
    /// Before the first [`read`](Self::read) or [`skip_to`](Self::skip_to), and
    /// directly after a [`rewind`](Self::rewind), an iterator has run past
    /// nothing, so this is `false` — including when its data turns out to be empty
    /// and the first read will find nothing. Only an iterator that cannot yield
    /// anything *by construction*, whatever the data, is at EOF from the start:
    /// [`Empty`] is one, an [`IdList`] over an empty list is not.
    ///
    /// That unread state is also the one place where the two answers stop being
    /// strict negations, for an implementation that materialises its result only
    /// on a read: [`current`](Self::current) already answers `None` there while
    /// this is still `false`, as its `# Usage` describes.
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
impl<'index, I: RQEIterator<'index> + ?Sized + 'index> RQEIterator<'index> for Box<I> {
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

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        (**self).revalidate(spec)
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

/// Combined trait for iterators that implement both [`RQEIterator`] and
/// [`ProfilePrint`](profile_print::ProfilePrint).
pub trait RQEIteratorPrintable<'index>: RQEIterator<'index> + profile_print::ProfilePrint {}

impl<'index, T> RQEIteratorPrintable<'index> for T where
    T: RQEIterator<'index> + profile_print::ProfilePrint
{
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
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;

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
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;

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
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;

    /// Iterate over all the tags (tokens) in the index at the given field index. Each document in
    /// then iterator will have the given weight.
    fn new_tag_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        token: &ffi::RSToken,
        field_index: FieldIndex,
        weight: f64,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;

    /// Iterate over the entries of the numeric index at the given field index whose value
    /// matches `filter`.
    fn new_numeric_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        filter: &NumericFilter,
        field_index: FieldIndex,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;

    /// Iterate over the entries of the geo (numeric-encoded geohash) index at
    /// the given field index whose `(lon, lat)` falls within the radius
    /// described by `gf`.
    ///
    /// `gf` is mutated in place: [`build_geo_numeric_filters`] decomposes the
    /// circle into up to [`geo::GEO_RANGE_COUNT`] numeric range filters and
    /// stores them in `gf.numericFilters` (owned by `*gf`, freed by `GeoFilter_Free`).
    /// The disk path then runs each range through the numeric machinery and
    /// post-filters survivors by true great-circle distance.
    fn new_geo_on_disk<'index>(
        &self,
        index: &'index mut ffi::RedisSearchDiskIndexSpec,
        gf: &'index mut ffi::GeoFilter,
        field_index: ffi::t_fieldIndex,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>>;
}
