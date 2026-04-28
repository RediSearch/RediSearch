/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Wildcard`].

use std::ptr::NonNull;

use ffi::{RS_FIELDMASK_ALL, t_docId};
use inverted_index::codec::{doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly};
use inverted_index::{DocIdsDecoder, RSIndexResult, opaque};

use crate::IteratorType;
use crate::{
    Empty, RQEIterator, RQEIteratorError, RQEValidateStatus, SEARCH_ENTERPRISE_ITERATORS,
    SkipToOutcome,
};

/// An iterator that yields all ids within a given range, from 1 to max id (inclusive) in an index.
#[derive(Default)]
pub struct Wildcard<'index> {
    // Supposed to be the max id in the index
    top_id: t_docId,

    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl Wildcard<'_> {
    pub fn new(top_id: t_docId, weight: f64) -> Self {
        Wildcard {
            top_id,
            result: RSIndexResult::build_virt()
                .frequency(1)
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
        }
    }
}

impl<'index> RQEIterator<'index> for Wildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        self.result.doc_id += 1;
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }
        debug_assert!(self.last_doc_id() < doc_id);

        if doc_id > self.top_id {
            // skip beyond range - set to EOF
            self.result.doc_id = self.top_id;
            return Ok(None);
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.top_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.top_id
    }

    unsafe fn revalidate(
        &mut self,
        _spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Wildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

/// A marker trait for iterators that match all documents.
pub trait WildcardIterator<'index>: RQEIterator<'index> {}

/// [`Wildcard`] is obviously a wildcard iterator.
impl<'index> WildcardIterator<'index> for Wildcard<'index> {}

/// [`inverted_index::Wildcard`](crate::inverted_index::Wildcard) is used in the optimized version.
impl<'index, E> WildcardIterator<'index> for crate::inverted_index::Wildcard<'index, E>
where
    E: inverted_index::DecodedBy
        + inverted_index::opaque::OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as inverted_index::DecodedBy>::Decoder: DocIdsDecoder,
{
}

/// A [`Profile`](crate::profile::Profile) wrapper preserves the wildcard property of its child.
impl<'index, I: WildcardIterator<'index>> WildcardIterator<'index>
    for crate::profile::Profile<'index, I>
{
}

/// A [`CRQEIterator`](crate::c2rust::CRQEIterator) may wrap a wildcard iterator
/// at runtime, but this cannot be verified statically.
/// The caller is responsible for only using this impl when the underlying C
/// iterator is actually a wildcard—mirroring the C code's use of an untyped
/// `QueryIterator*` for the `wcii` field.
impl<'index> WildcardIterator<'index> for crate::c2rust::CRQEIterator {}

impl<'index> RQEIterator<'index> for Box<dyn WildcardIterator<'index> + 'index> {
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
        spec: std::ptr::NonNull<ffi::IndexSpec>,
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

    fn as_c_iterator(&self) -> Option<&crate::c2rust::CRQEIterator> {
        (**self).as_c_iterator()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        (**self).intersection_sort_weight(prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for Box<dyn WildcardIterator<'index> + 'index> {}

/// The result of [`new_wildcard_iterator`], representing the different kinds of
/// wildcard iterators that can be created depending on the index configuration.
pub enum NewWildcardIterator<'index> {
    /// Non-optimized wildcard: yields all document ids from 1 to `maxDocId`.
    NotOptimized(Wildcard<'index>),
    /// Optimized wildcard: reads from the `existingDocs` inverted index.
    Optimized(OptimizedWildcard<'index>),
    /// Empty wildcard: the index has no documents.
    Empty(Empty),
    /// Disk-backed wildcard: delegates to the enterprise disk index iterator.
    Disk(DiskWildcardIterator<'index>),
}

/// An optimized wildcard iterator over the `existingDocs` inverted index.
///
/// The encoding may be either [`DocIdsOnly`] or [`RawDocIdsOnly`], depending on
/// the index configuration.
pub enum OptimizedWildcard<'index> {
    /// Optimized wildcard with [`DocIdsOnly`] encoding.
    DocIdsOnly(crate::inverted_index::Wildcard<'index, DocIdsOnly>),
    /// Optimized wildcard with [`RawDocIdsOnly`] encoding.
    RawDocIdsOnly(crate::inverted_index::Wildcard<'index, RawDocIdsOnly>),
}

/// Delegates each [`RQEIterator`] method to the active variant.
macro_rules! delegate_rqe_iterator {
    ($self:ident, $method:ident $(, $arg:ident)*) => {
        match $self {
            Self::DocIdsOnly(it) => it.$method($($arg),*),
            Self::RawDocIdsOnly(it) => it.$method($($arg),*),
        }
    };
}

impl<'index> RQEIterator<'index> for OptimizedWildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_rqe_iterator!(self, current)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, read)
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_rqe_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        delegate_rqe_iterator!(self, num_estimated)
    }

    fn last_doc_id(&self) -> t_docId {
        delegate_rqe_iterator!(self, last_doc_id)
    }

    fn at_eof(&self) -> bool {
        delegate_rqe_iterator!(self, at_eof)
    }

    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Delegating to variant with the same `spec` passed by our caller.
        unsafe { delegate_rqe_iterator!(self, revalidate, spec) }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_rqe_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_rqe_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for OptimizedWildcard<'index> {}

/// Delegates each [`RQEIterator`] method to the active variant.
macro_rules! delegate_wildcard_iterator {
    ($self:ident, $method:ident $(, $arg:ident)*) => {
        match $self {
            Self::NotOptimized(it) => it.$method($($arg),*),
            Self::Optimized(it) => it.$method($($arg),*),
            Self::Empty(it) => it.$method($($arg),*),
            Self::Disk(it) => it.$method($($arg),*),
        }
    };
}

impl<'index> RQEIterator<'index> for NewWildcardIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_wildcard_iterator!(self, current)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, read)
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_wildcard_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        delegate_wildcard_iterator!(self, num_estimated)
    }

    fn last_doc_id(&self) -> t_docId {
        delegate_wildcard_iterator!(self, last_doc_id)
    }

    fn at_eof(&self) -> bool {
        delegate_wildcard_iterator!(self, at_eof)
    }

    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Delegating to variant with the same `spec` passed by our caller.
        unsafe { delegate_wildcard_iterator!(self, revalidate, spec) }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_wildcard_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_wildcard_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for NewWildcardIterator<'index> {}

/// Create a [`WildcardIterator`] for an index whose spec has
/// [`SchemaRule`](ffi::SchemaRule)`.index_all` set.
///
/// When [`spec.existingDocs`](ffi::IndexSpec::existingDocs) is non-null, the returned iterator
/// reads from the existing-documents inverted index (either
/// [`DocIdsOnly`] or [`RawDocIdsOnly`]
/// encoding). When it is null (no documents indexed yet), an [`Empty`] iterator
/// is returned instead.
///
/// # Safety
///
/// 1. `sctx` must point to a valid [`RedisSearchCtx`](ffi::RedisSearchCtx) that
///    remains valid for `'index`.
/// 2. `sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for `'index`.
/// 3. `sctx.spec.rule` must be a non-null pointer to a valid [`SchemaRule`](ffi::SchemaRule) with
///    [`index_all`](ffi::SchemaRule::index_all) set to `true`.
/// 4. `sctx.spec.existingDocs`, when non-null, must point to a valid
///    [`opaque::InvertedIndex`] with either
///    [`DocIdsOnly`] or [`RawDocIdsOnly`]
///    encoding.
pub unsafe fn new_wildcard_iterator_optimized<'index>(
    sctx: NonNull<ffi::RedisSearchCtx>,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `sctx` points to a valid `RedisSearchCtx` (1).
    let sctx_ref = unsafe { sctx.as_ref() };
    let spec = NonNull::new(sctx_ref.spec).expect("sctx.spec is null");
    // SAFETY: Caller guarantees `sctx.spec` is a valid, non-null pointer (2).
    let spec_ref = unsafe { spec.as_ref() };
    let rule = NonNull::new(spec_ref.rule).expect("sctx.spec.rule is null");
    // SAFETY: Caller guarantees `sctx.spec.rule` is a valid, non-null pointer (3).
    let rule_ref = unsafe { rule.as_ref() };
    debug_assert!(rule_ref.index_all);

    match NonNull::new(spec_ref.existingDocs) {
        Some(existing_docs) => {
            let ii = existing_docs.cast::<opaque::InvertedIndex>();
            // SAFETY: Caller guarantees `existingDocs` points to a valid
            // `opaque::InvertedIndex` with `DocIdsOnly` or `RawDocIdsOnly`
            // encoding (4).
            let ii_ref = unsafe { ii.as_ref() };
            let optimized = match ii_ref {
                opaque::InvertedIndex::DocIdsOnly(ii) => OptimizedWildcard::DocIdsOnly(
                    crate::inverted_index::Wildcard::new(ii.reader(), weight),
                ),
                opaque::InvertedIndex::RawDocIdsOnly(ii) => OptimizedWildcard::RawDocIdsOnly(
                    crate::inverted_index::Wildcard::new(ii.reader(), weight),
                ),
                _ => panic!("spec.existingDocs has the wrong inverted index type: {ii_ref:?}"),
            };
            NewWildcardIterator::Optimized(optimized)
        }
        None => NewWildcardIterator::Empty(Empty),
    }
}

/// Create a [`WildcardIterator`] backed by an on-disk index implementation.
///
/// This delegates to [`SEARCH_ENTERPRISE_ITERATORS`]'s
/// [`new_wildcard_on_disk`](crate::SearchEnterpriseIterators::new_wildcard_on_disk)
/// and wraps the resulting iterator in a [`DiskWildcardIterator`].
///
/// If the enterprise iterator cannot be created, this function logs a warning
/// and falls back to an empty iterator.
///
/// # Safety
///
/// 1. `disk_spec` must reference a valid [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec)
///    that remains valid for `'index`.
/// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized before calling this function.
pub unsafe fn new_wildcard_iterator_on_disk<'index>(
    disk_spec: &'index mut ffi::RedisSearchDiskIndexSpec,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `SEARCH_ENTERPRISE_ITERATORS` is
    // initialized when `spec.diskSpec` is non-null (8).
    let enterprise_iters_api = SEARCH_ENTERPRISE_ITERATORS
        .get()
        .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");
    match enterprise_iters_api.new_wildcard_on_disk(disk_spec, weight) {
        Ok(it) => NewWildcardIterator::Disk(DiskWildcardIterator(it)),
        Err(err) => {
            tracing::warn!(
                "Failed to create a disk wildcard iterator ({err}); falling back to empty iterator."
            );
            NewWildcardIterator::Empty(Empty)
        }
    }
}

/// Create a [`WildcardIterator`] from a query evaluation context.
///
/// There are three possible code paths:
///
/// 1. **Disk index** — when [`spec.diskSpec`](ffi::IndexSpec::diskSpec) is non-null, delegates to
///    [`SEARCH_ENTERPRISE_ITERATORS`]'s [`new_wildcard_on_disk`](crate::SearchEnterpriseIterators::new_wildcard_on_disk)
///    and wraps the result in a [`DiskWildcardIterator`].
/// 2. **[`index_all`](ffi::SchemaRule::index_all) optimized** — when
///    [`SchemaRule`](ffi::SchemaRule)`.index_all` is set, delegates to
///    [`new_wildcard_iterator_optimized`] which reads from the
///    [`existingDocs`](ffi::IndexSpec::existingDocs) inverted index.
/// 3. **Fallback** — creates a simple [`Wildcard`] iterator that yields all
///    document ids up to [`docTable.maxDocId`](ffi::DocTable::maxDocId).
///
/// # Safety
///
/// 1. `query` must point to a valid [`QueryEvalCtx`](ffi::QueryEvalCtx) that
///    remains valid for `'index`.
/// 2. `query.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx) that remains valid for `'index`.
/// 3. `query.sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for `'index`.
/// 4. `query.sctx.spec.rule`, when non-null, must point to a valid [`SchemaRule`](ffi::SchemaRule).
/// 5. When [`SchemaRule`](ffi::SchemaRule)`.index_all` is true, the preconditions of
///    [`new_wildcard_iterator_optimized`] must also hold.
/// 6. `query.docTable` must be a non-null pointer to a valid [`DocTable`](ffi::DocTable) that
///    remains valid for `'index`.
/// 7. `query.sctx.spec.diskSpec`, when non-null, must point to a valid
///    [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec) that remains valid for `'index`.
/// 8. When `query.sctx.spec.diskSpec` is non-null, [`SEARCH_ENTERPRISE_ITERATORS`] must be
///    initialized.
pub unsafe fn new_wildcard_iterator<'index>(
    query: NonNull<ffi::QueryEvalCtx>,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `query` points to a valid `QueryEvalCtx` (1).
    let query = unsafe { query.as_ref() };
    let sctx = NonNull::new(query.sctx).expect("query.sctx is null");
    // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: Caller guarantees `query.sctx.spec` is a valid, non-null pointer (3).
    let spec = unsafe { &*sctx_ref.spec };

    if !spec.diskSpec.is_null() {
        // SAFETY: Caller guarantees `spec.diskSpec` is a valid, non-null
        // pointer to a `RedisSearchDiskIndexSpec` that remains valid for
        // `'index` (7).
        let disk_spec = unsafe { &mut *spec.diskSpec };
        // SAFETY: Caller guarantees all preconditions of
        // `new_wildcard_iterator_on_disk` hold (7, 8).
        return unsafe { new_wildcard_iterator_on_disk(disk_spec, weight) };
    }

    let index_all = NonNull::new(spec.rule)
        .map(|rule| {
            // SAFETY: Caller guarantees `spec.rule`, when non-null, points to
            // a valid `SchemaRule` (4).
            let rule_ref = unsafe { rule.as_ref() };
            rule_ref.index_all
        })
        .unwrap_or_default();

    if index_all {
        // SAFETY: Caller guarantees the preconditions of
        // `new_wildcard_iterator_optimized` hold when `rule.index_all` is
        // true (5).
        unsafe { new_wildcard_iterator_optimized(sctx, weight) }
    } else {
        // SAFETY: Caller guarantees `query.docTable` is a valid, non-null
        // pointer (6).
        let doc_table = unsafe { &*query.docTable };
        NewWildcardIterator::NotOptimized(Wildcard::new(doc_table.maxDocId, weight))
    }
}

/// A wildcard iterator backed by an enterprise disk index iterator.
///
/// This is a thin wrapper around a [`Box<dyn RQEIterator>`] provided by
/// [`SEARCH_ENTERPRISE_ITERATORS`] that implements [`WildcardIterator`],
/// allowing disk-based wildcard queries to be used interchangeably with
/// in-memory ones.
#[repr(transparent)]
pub struct DiskWildcardIterator<'index>(Box<dyn RQEIterator<'index> + 'index>);

impl<'index> RQEIterator<'index> for DiskWildcardIterator<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.0.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.0.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }

    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Delegating to inner iterator with the same `spec` passed by our caller.
        unsafe { self.0.revalidate(spec) }
    }

    fn rewind(&mut self) {
        self.0.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.0.type_()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.0.intersection_sort_weight(prioritize_union_children)
    }
}

/// [`DiskWildcardIterator`] matches all documents on the disk index.
impl<'index> WildcardIterator<'index> for DiskWildcardIterator<'index> {}
