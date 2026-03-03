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

use ffi::{
    IteratorType, IteratorType_EMPTY_ITERATOR, IteratorType_INV_IDX_WILDCARD_ITERATOR,
    IteratorType_WILDCARD_ITERATOR, RS_FIELDMASK_ALL, t_docId,
};
use inverted_index::{DocIdsDecoder, RSIndexResult, opaque};

use crate::{
    Empty, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, c2rust::CRQEIterator,
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
    pub const fn new(top_id: t_docId, weight: f64) -> Self {
        Wildcard {
            top_id,
            result: RSIndexResult::virt()
                .frequency(1)
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL),
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

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    fn is_wildcard(&self) -> bool {
        true
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

/// [`Empty`] is used as wildcard in the optimized version if the spec has no document.
struct EmptyWildcard(Empty);

impl<'index> RQEIterator<'index> for EmptyWildcard {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.0.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.0.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.0.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.0.revalidate()
    }

    #[inline(always)]
    fn is_wildcard(&self) -> bool {
        true
    }
}

/// [`EmptyIterator`] matches all documents (vacuously, since the index is empty).
impl<'index> WildcardIterator<'index> for EmptyWildcard {}

// TODO: new_wildcard_iterator_optimized() and new_wildcard_iterator() have to return
// the actual IteratorType value so the ffi code can properly wrap it into a RQEIteratorWrapper.
// We can stop returning those once all the code using those have been ported to Rust.

/// Create a [`WildcardIterator`] for an index whose spec has
/// [`SchemaRule`](ffi::SchemaRule)`.index_all` set.
///
/// When [`spec.existingDocs`](ffi::IndexSpec::existingDocs) is non-null, the returned iterator
/// reads from the existing-documents inverted index (either
/// [`DocIdsOnly`](inverted_index::codec::doc_ids_only::DocIdsOnly) or
/// [`RawDocIdsOnly`](inverted_index::codec::raw_doc_ids_only::RawDocIdsOnly)
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
///    [`DocIdsOnly`](inverted_index::codec::doc_ids_only::DocIdsOnly) or
///    [`RawDocIdsOnly`](inverted_index::codec::raw_doc_ids_only::RawDocIdsOnly)
///    encoding.
pub unsafe fn new_wildcard_iterator_optimized<'index>(
    sctx: NonNull<ffi::RedisSearchCtx>,
    weight: f64,
) -> (Box<dyn WildcardIterator<'index> + 'index>, IteratorType) {
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
            let it: Box<dyn WildcardIterator> = match ii_ref {
                opaque::InvertedIndex::DocIdsOnly(ii) => {
                    // SAFETY: All preconditions of `Wildcard::new` are
                    // satisfied: `sctx` is valid (1), `sctx.spec` is valid (2),
                    // both remain valid for `'index`, and the encoding matches.
                    Box::new(unsafe {
                        crate::inverted_index::Wildcard::new(ii.reader(), sctx, weight)
                    })
                }
                opaque::InvertedIndex::RawDocIdsOnly(ii) => {
                    // SAFETY: Same as the `DocIdsOnly` arm above.
                    Box::new(unsafe {
                        crate::inverted_index::Wildcard::new(ii.reader(), sctx, weight)
                    })
                }
                _ => panic!("spec.existingDocs has the wrong inverted index type: {ii_ref:?}"),
            };
            (it, IteratorType_INV_IDX_WILDCARD_ITERATOR)
        }
        None => (Box::new(EmptyWildcard(Empty)), IteratorType_EMPTY_ITERATOR),
    }
}

/// Create a [`WildcardIterator`] from a query evaluation context.
///
/// There are three possible code paths:
///
/// 1. **Disk index** — when [`spec.diskSpec`](ffi::IndexSpec::diskSpec) is non-null, delegates to the C
///    function `SearchDisk_NewWildcardIterator` and wraps the result in a
///    [`DiskWildcardIterator`].
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
///    [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec). `SearchDisk_NewWildcardIterator` must return
///    a valid, owning `QueryIterator` pointer with all required callbacks set.
pub unsafe fn new_wildcard_iterator<'index>(
    query: NonNull<ffi::QueryEvalCtx>,
    weight: f64,
) -> (Box<dyn WildcardIterator<'index> + 'index>, IteratorType) {
    // SAFETY: Caller guarantees `query` points to a valid `QueryEvalCtx` (1).
    let query = unsafe { query.as_ref() };
    let sctx = NonNull::new(query.sctx).expect("query.sctx is null");
    // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: Caller guarantees `query.sctx.spec` is a valid, non-null pointer (3).
    let spec = unsafe { &*sctx_ref.spec };

    if !spec.diskSpec.is_null() {
        // SAFETY: Caller guarantees `spec` is valid (3), so `spec.diskSpec`
        // is a valid, non-null pointer to a `RedisSearchDiskIndexSpec`.
        // `SearchDisk_NewWildcardIterator` returns an owning pointer to a
        // fully initialized `QueryIterator` (7).
        let it = unsafe { ffi::SearchDisk_NewWildcardIterator(spec.diskSpec, weight) };
        let it = NonNull::new(it).expect("SearchDisk_NewWildcardIterator returned null");
        // SAFETY: `SearchDisk_NewWildcardIterator` returns a valid, owning
        // `QueryIterator` pointer with all required callbacks set (7).
        let c_it = unsafe { CRQEIterator::new(it) };
        // Read the type from the C iterator before wrapping it.
        let iter_type = c_it.type_;
        return (Box::new(DiskWildcardIterator(c_it)), iter_type);
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
        (
            Box::new(Wildcard::new(doc_table.maxDocId, weight)),
            IteratorType_WILDCARD_ITERATOR,
        )
    }
}

/// A wildcard iterator backed by a C-side disk index iterator.
///
/// This is a thin wrapper around [`CRQEIterator`] that implements
/// [`WildcardIterator`], allowing disk-based wildcard queries to be used
/// interchangeably with in-memory ones.
#[repr(transparent)]
struct DiskWildcardIterator(CRQEIterator);

impl<'index> RQEIterator<'index> for DiskWildcardIterator {
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

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.0.revalidate()
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

    fn is_wildcard(&self) -> bool {
        // strictly speaking this is a wildcard iterator but the current reducers code from other
        // iterators do not account for it.
        false
    }
}

/// [`DiskWildcardIterator`] matches all documents on the disk index.
impl<'index> WildcardIterator<'index> for DiskWildcardIterator {}
