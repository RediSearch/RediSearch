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
//! [`Metric`](index_result::RSResultKind::Metric) result carrying
//! `Σ q_i · d_i`. The top K is chosen downstream, by the result-processor
//! sorter, from the request's `LIMIT`.

use std::ptr::NonNull;

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::{IndexReaderCore, numeric::NumericFloatCompression};
use rlookup::{KeyHandleSlot, RLookupKey, RLookupKeyHandle};
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, NoOpChecker, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    UnionFullFlat, UnionFullHeap,
    inverted_index::InvIndIterator,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use crate::SparseVectorIndex;

/// An iterator over one dimension's posting list, yielding `(doc_id, d_i)`
/// with the query weight `q_i` as its result weight.
pub type DimensionIterator<'index> =
    InvIndIterator<'index, IndexReaderCore<'index, NumericFloatCompression>, NoOpChecker>;

/// The union over the query's dimension iterators. Always a full union (no
/// quick exit), so each result aggregates every dimension present at that
/// document, which the sum needs.
enum DimensionUnion<'index> {
    Flat(UnionFullFlat<'index, DimensionIterator<'index>>),
    Heap(UnionFullHeap<'index, DimensionIterator<'index>>),
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

/// Iterator for one `@field:[SPARSE $q]` query node.
///
/// Yields every document that shares a dimension with the query, in doc-ID
/// order, as a [`Metric`](index_result::RSResultKind::Metric) result whose
/// value is the document's dot product with the query. Reducing at this node
/// keeps the sparse score apart from any text or tag clauses the node is
/// combined with.
pub struct SparseVectorIterator<'index> {
    union: DimensionUnion<'index>,
    result: RSIndexResult<'index>,
    /// The number of query dimensions that have a posting list.
    num_dimensions: usize,
    /// Where the score is yielded for `$YIELD_SCORE_AS`, or null when it is not.
    /// Written by the pipeline once the lookup key is resolved, through the
    /// handle [`key_handle`](Self::key_handle) tracks.
    own_key: *mut RLookupKey<'index>,
    key_handle: KeyHandleSlot<'index>,
}

impl<'index> SparseVectorIterator<'index> {
    /// Build the iterator for `query`, a vector already checked and
    /// normalized by [`prepare`](crate::prepare) against `index`.
    ///
    /// Query dimensions with no posting list contribute nothing and are
    /// skipped. Returns `None` when none remain, so the node matches nothing.
    ///
    /// The union is a heap union when the query keeps more than
    /// `min_union_iter_heap` dimensions, and a flat one otherwise.
    pub fn new(
        index: &'index SparseVectorIndex,
        query: &[(u32, f32)],
        min_union_iter_heap: usize,
    ) -> Option<Self> {
        let children: Vec<DimensionIterator<'index>> = query
            .iter()
            .filter_map(|&(dim_id, weight)| {
                let posting_list = index.get_dimension(dim_id)?;
                let result = RSIndexResult::build_numeric(0.0)
                    .weight(f64::from(weight))
                    .build();
                Some(InvIndIterator::new(
                    posting_list.reader(),
                    result,
                    NoOpChecker,
                ))
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
            result: RSIndexResult::build_metric(0.0).build(),
            num_dimensions,
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
            .expect("the sparse result is built as a metric") = value;
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

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let key = self.own_key;
        let moved = dispatch!(&mut self.union, u => match u.revalidate(spec)? {
            RQEValidateStatus::Ok => return Ok(RQEValidateStatus::Ok),
            RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
            RQEValidateStatus::Moved { current } => current.map(|r| &*r),
        });
        let current = match moved {
            Some(from) => {
                Self::load(&mut self.result, from, key);
                Some(&mut self.result)
            }
            None => None,
        };
        Ok(RQEValidateStatus::Moved { current })
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

impl ProfilePrint for SparseVectorIterator<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"SPARSE_VECTOR");
        let dims = std::ffi::CString::new(self.num_dimensions.to_string())
            .expect("a number has no interior NUL");
        map.kv_simple_string(c"Dimensions", &dims);
        ctx.print_optional_counters(map);
        ctx.print_estimated(map);
    }
}
