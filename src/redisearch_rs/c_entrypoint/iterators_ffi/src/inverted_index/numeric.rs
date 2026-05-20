/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::{FieldSpec, RSGlobalConfig};
use field::FieldFilterContext;
use inverted_index::NumericFilter;
use query_node_type::QueryNodeType;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    NumericIteratorVariant, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    open_numeric_or_geo_index, profile_print,
};

/// Wrapper around [`NumericIteratorVariant`].
pub(super) struct NumericIterator<'index> {
    /// The iterator variant (unfiltered, filtered numeric, or geo).
    iterator: NumericIteratorVariant<'index>,
}

impl<'index> NumericIterator<'index> {
    /// Wrap a variant for use by [`crate::inverted_index::geo`].
    pub(super) const fn new(iterator: NumericIteratorVariant<'index>) -> Self {
        Self { iterator }
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for NumericIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
        self.iterator.current()
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        self.iterator.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: u64,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        self.iterator.skip_to(doc_id)
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        self.iterator.revalidate(spec)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.iterator.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.iterator.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> u64 {
        self.iterator.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.iterator.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxNumeric
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

///
/// # Safety
///
/// 1. `spec` must be a valid non-null pointer to an [`ffi::IndexSpec`].
/// 2. `fs` must be a valid non-null pointer to a [`FieldSpec`] for a numeric or geo field.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn openNumericOrGeoIndex(
    spec: *mut ffi::IndexSpec,
    fs: *mut FieldSpec,
    create_if_missing: bool,
) -> *mut ffi::NumericRangeTree {
    debug_assert!(!spec.is_null());
    debug_assert!(!fs.is_null());
    // SAFETY: 1. guarantees spec is valid and non-null.
    let spec = unsafe { &mut *spec };
    // SAFETY: 2. guarantees fs is valid and non-null.
    let fs = unsafe { &mut *fs };

    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 1. and 2. are forwarded from this function's safety contract.
    match unsafe { open_numeric_or_geo_index(spec, fs, create_if_missing, compress) } {
        Some(tree) => std::ptr::from_mut(tree).cast::<ffi::NumericRangeTree>(),
        None => ptr::null_mut(),
    }
}

/// Opens the numeric/geo index and creates an iterator over all matching sub-ranges.
///
/// # Returns
///
/// - `NULL` if the index doesn't exist for this field (i.e., no documents have been indexed
///   for it yet).
/// - `NULL` if no sub-ranges in the tree match the filter.
/// - A single iterator if exactly one sub-range matches.
/// - A union iterator over all matching sub-ranges otherwise.
///
/// # Safety
///
/// 1. `ctx` must be a valid non-NULL pointer to a [`ffi::RedisSearchCtx`], remaining valid
///    for the lifetime of the returned iterator.
/// 2. `ctx.spec` must be a valid non-NULL pointer to an [`ffi::IndexSpec`].
/// 3. `flt` must be a valid non-NULL pointer to a [`NumericFilter`] whose `field_spec` field
///    is a valid non-NULL pointer to a [`FieldSpec`], remaining valid for the lifetime of the
///    returned iterator.
/// 4. `config` must be a valid non-NULL pointer to an [`ffi::IteratorsConfig`].
/// 5. `filter_ctx` must be a valid non-NULL pointer to a [`FieldFilterContext`] with a field
///    index (not a field mask).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNumericFilterIterator(
    ctx: *const ffi::RedisSearchCtx,
    flt: *const NumericFilter,
    _for_type: ffi::FieldType,
    config: *const ffi::IteratorsConfig,
    filter_ctx: *const FieldFilterContext,
) -> *mut ffi::QueryIterator {
    debug_assert!(!ctx.is_null());
    debug_assert!(!flt.is_null());

    // SAFETY: 3. guarantees flt is valid and non-null.
    let flt_ref = unsafe { &*flt };
    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).minUnionIterHeap } as usize;

    // SAFETY: 1. guarantees ctx is valid and non-null.
    let sctx = unsafe { NonNull::new_unchecked(ctx as *mut ffi::RedisSearchCtx) };
    // SAFETY: 5. guarantees filter_ctx is valid and non-null.
    let field_ctx = unsafe { &*filter_ctx };

    let node_type = if flt_ref.is_numeric_filter() {
        QueryNodeType::Numeric
    } else {
        QueryNodeType::Geo
    };

    // SAFETY: 1. guarantees sctx.spec is valid and non-null.
    let spec_ptr = unsafe { sctx.as_ref() }.spec;
    // SAFETY: 1. guarantees sctx.spec is valid and non-null.
    let spec = unsafe { &mut *spec_ptr };
    // SAFETY: 3. guarantees flt.field_spec is valid and non-null.
    let fs = unsafe { &mut *(flt_ref.field_spec as *mut ffi::FieldSpec) };
    // SAFETY: 1.–3. are forwarded from this function's safety contract.
    let Some(tree) = (unsafe { open_numeric_or_geo_index(spec, fs, false, compress) }) else {
        return ptr::null_mut();
    };

    // SAFETY: 1. and 5.
    let variants = unsafe { NumericIteratorVariant::from_tree(tree, sctx, flt_ref, field_ctx) };
    if variants.is_empty() {
        return ptr::null_mut();
    }

    let children: Vec<CRQEIterator> = variants
        .into_iter()
        .map(|iterator| {
            let ptr = RQEIteratorWrapper::boxed_new(NumericIterator::new(iterator));
            // SAFETY: `boxed_new` uses `Box::into_raw`, which is guaranteed non-null.
            let ptr = unsafe { NonNull::new_unchecked(ptr) };
            // SAFETY: `ptr` is a valid, uniquely-owned `QueryIterator`.
            unsafe { CRQEIterator::new(ptr) }
        })
        .collect();

    crate::union::build_union_from_children(
        children,
        true,
        min_union_iter_heap,
        node_type,
        ptr::null(),
        1.0,
    )
}

impl profile_print::ProfilePrint for NumericIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut profile_print::ProfilePrintCtx<'_>,
    ) {
        self.iterator.print_profile(map, ctx);
    }
}
