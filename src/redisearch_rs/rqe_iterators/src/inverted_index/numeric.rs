/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{f64, ptr::NonNull};

use crate::{
    FieldExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    c2rust::CRQEIterator,
    expiration_checker::{ExpirationChecker, NoOpChecker},
    profile_print::{ProfilePrint, ProfilePrintCtx, format_g},
};
use ffi::{
    FieldType_INDEXFLD_T_GEO, FieldType_INDEXFLD_T_NUMERIC, IndexFlags, QueryIterator,
    RedisSearchCtx,
};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    FilterGeoReader, FilterNumericReader, IndexReader, NumericFilter, NumericReader,
    ResumableReader, SuspendableReader,
};
use numeric_range_tree::{NumericIndexReader, NumericRangeTree, RawNumericIndexReader};
use query_types::QueryNodeType;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use super::core::{InvIndIterator, RawInvIndIterator, ResumeStatus};

/// An iterator over numeric inverted index entries, parameterised over a
/// [`Ref`] mode. See [`Numeric`] for the [`Active`] instantiation that
/// implements [`RQEIterator`].
///
/// The [`inverted_index::IndexReader`] API can be used to fully scan an inverted index.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode (see [`RawInvIndIterator`] for details).
/// * `R` - The type of the numeric reader.
/// * `E` - The expiration checker type used to check for expired documents.
#[repr(C)]
pub struct RawNumeric<'query, Rf: Ref, R, E = NoOpChecker, RA = R> {
    it: RawInvIndIterator<'query, Rf, R, E, RA>,
    /// The numeric range tree and its revision ID, used to detect changes during revalidation.
    range_tree_info: Option<RangeTreeInfo>,
    /// Minimum numeric range, only used in debug print.
    range_min: f64,
    /// Maximum numeric range, only used in debug print.
    range_max: f64,
}

/// Alias for an [`Active`] [`RawNumeric`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Numeric<'index, R, E = NoOpChecker> = RawNumeric<'index, Active<'index>, R, E, R>;

/// Information about the numeric range tree backing a [`Numeric`] iterator.
struct RangeTreeInfo {
    /// Pointer to the numeric range tree.
    tree: NonNull<NumericRangeTree>,
    /// The revision ID at the time the iterator was created.
    /// Used to detect if the tree has been modified.
    revision_id: u32,
}

impl<'query, Rf: Ref, R, E, RA> RawNumeric<'query, Rf, R, E, RA> {
    /// Cached minimum numeric range (only used in debug print / FT.PROFILE).
    pub const fn range_min(&self) -> f64 {
        self.range_min
    }

    /// Cached maximum numeric range (only used in debug print / FT.PROFILE).
    pub const fn range_max(&self) -> f64 {
        self.range_max
    }

    /// Cached [`IndexFlags`] of the underlying inverted index — see
    /// [`RawInvIndIterator::flags`].
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }

    /// Check if the iterator should abort revalidation.
    ///
    /// The numeric range tree's revision id changes when the tree is
    /// modified by GC (a node split or removal). The iterator's cached
    /// `revision_id` snapshot is compared against the current value; if
    /// they differ, the cursor is invalidated and the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    ///
    /// Reads only the range tree's `NonNull` pointer and `revision_id` (owned by
    /// the spec, stable under the read lock); it materializes no `&'a`
    /// reader/buffer borrow, so it is sound to call on the suspended form during
    /// [`RQESuspendedIterator::resume`].
    pub(crate) const fn should_abort(&self) -> bool {
        // If there's no range tree, we can't check for changes
        let Some(ref info) = self.range_tree_info else {
            return false;
        };

        let current_revision_id = {
            // SAFETY: Condition 2 of `Self::new` guarantees the tree
            // remains valid for the iterator's lifetime.
            let tree = unsafe { info.tree.as_ref() };
            tree.revision_id()
        };
        // If the revision id changed the numeric tree was either completely deleted or a node was split or removed.
        // The cursor is invalidated so we cannot revalidate the iterator.
        current_revision_id != info.revision_id
    }
}

impl<'index, R, E> Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    /// Create an iterator returning results from a numeric inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`NumericReader`] such as [`inverted_index::FilterNumericReader`]
    /// or [`inverted_index::FilterGeoReader`].
    ///
    /// `expiration_checker` is used to check for expired documents when reading from the inverted index.
    ///
    /// `range_tree` is the underlying range tree backing the iterator.
    /// It is used during revalidation to check if the iterator is still valid.
    ///
    /// `range_min` and `range_max` are the minimum and maximum numeric ranges,
    /// respectively. They are only used in debug print.
    ///
    /// # Safety
    ///
    /// 1. If `range_tree` is Some, it must be a valid pointer to a [`NumericRangeTree`].
    /// 2. If `range_tree` is Some, it must stay valid during the iterator's lifetime.
    pub unsafe fn new(
        reader: R,
        expiration_checker: E,
        range_tree: Option<&NumericRangeTree>,
        range_min: Option<f64>,
        range_max: Option<f64>,
    ) -> Self {
        let result = RSIndexResult::build_numeric(0.0).build();

        let range_tree_info = range_tree.map(|tree| {
            let revision_id = tree.revision_id();
            RangeTreeInfo {
                tree: NonNull::from_ref(tree),
                revision_id,
            }
        });

        let range_min = range_min.unwrap_or(f64::NEG_INFINITY);
        let range_max = range_max.unwrap_or(f64::INFINITY);
        assert!(range_min <= range_max);

        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            range_tree_info,
            range_min,
            range_max,
        }
    }

    /// Get a reference to the underlying reader.
    ///
    /// This is used by FFI code to access the reader.
    pub const fn reader(&self) -> &R {
        &self.it.reader
    }
}

impl<'index, R, E> RQEIteratorBoxed<'index> for Numeric<'index, R, E>
where
    R: NumericReader<'index> + SuspendableReader + 'index,
    R::Suspended: ResumableReader,
    for<'a> <R::Suspended as ResumableReader>::Resumed<'a>: NumericReader<'a>,
    E: ExpirationChecker + 'static,
{
    // Reader weakens `R -> R::Suspended`; the frozen `RA = R` slot keeps the
    // inner iterator's dispatch pointers unchanged across the cast.
    type Suspended = RawNumeric<'index, Suspended, R::Suspended, E, R>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawNumeric` is `#[repr(C)]` over the inner
        // `RawInvIndIterator`, layout-identical across modes by invariant 1 on
        // [`RawInvIndIterator`]: `reader` weakens `R -> R::Suspended` while the
        // frozen `RA = R` slot keeps the dispatch pointers unchanged. The
        // remaining fields (`range_tree_info`, `range_min`, `range_max`) carry no
        // `Rf`, so they survive the cast unchanged. `Box::from_raw` reuses the
        // same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawNumeric<'index, Suspended, R::Suspended, E, R>) }
    }
}

impl<'query, RS, E, RA> RQESuspendedIterator<'query> for RawNumeric<'query, Suspended, RS, E, RA>
where
    RS: ResumableReader,
    for<'a> RS::Resumed<'a>: NumericReader<'a>,
    E: ExpirationChecker + 'static,
{
    type Resumed<'a>
        = Numeric<'a, RS::Resumed<'a>, E>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: identity check on the suspended form. On abort we drop the
        // suspended iterator without promoting it to Active — nothing is
        // materialized.
        if self.should_abort() {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: run the shared in-place resume transition on the inner core
        // iterator (refresh pointers, reset stale offsets, promote the result,
        // and re-seek if GC moved us). `guard` witnesses the read lock the
        // refresh requires.
        let status = self.it.resume_in_place(guard)?;

        // Step 3: reinterpret the owning box's type. The heap address is
        // preserved across the cast.
        let raw = Box::into_raw(self);
        // SAFETY: `RawNumeric` is `#[repr(C)]` over the inner `RawInvIndIterator`
        // (layout-identical across modes by invariant 1 on `RawInvIndIterator`)
        // plus the `Rf`-free `range_tree_info`/`range_min`/`range_max` fields.
        // `resume_in_place` left the inner iterator as a valid active iterator, so
        // the whole `RawNumeric` is now a valid `Numeric<'a, RS::Resumed<'a>, E>`.
        let active = unsafe { Box::from_raw(raw as *mut Numeric<'a, RS::Resumed<'a>, E>) };

        Ok(match status {
            ResumeStatus::Unchanged => ResumeOutcome::Ok(active),
            ResumeStatus::Moved => ResumeOutcome::Moved(active),
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }
}
impl<'index, R, E> RQEIterator<'index> for Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxNumeric
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, R, E> ProfilePrint for Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"NUMERIC");
        let term_str = format!(
            "{} - {}",
            format_g(self.range_min),
            format_g(self.range_max),
        );
        let term_cstr = std::ffi::CString::new(term_str).unwrap();
        map.kv_simple_string(c"Term", &term_cstr);
        ctx.print_optional_counters(map);
        map.kv_long_long(c"Estimated number of matches", self.num_estimated() as i64);
    }
}

/// Opens the numeric or geo index for a field, optionally creating it if missing.
///
/// # Arguments
///
/// - `spec`: The index spec that owns the field. Updated with memory usage when a new tree is
///   created.
/// - `fs`: The field spec for the numeric or geo field whose tree is being opened. Must be of
///   numeric or geo type.
/// - `create_if_missing`: If `true` and the field has no tree yet, a new [`NumericRangeTree`] is
///   allocated and attached to `fs`.
/// - `numeric_compress`: Passed to [`NumericRangeTree::new`] when creating a fresh tree.
///   Controls whether values in the inverted index are stored in compressed form.
///
/// # Returns
///
/// - `Some` if the tree exists (or was just created).
/// - `None` if the tree is absent and `create_if_missing` is `false`.
///
/// # Safety
///
/// 1. `spec` and `fs` must be valid, properly initialised references.
/// 2. `fs.tree`, if non-null, must point to a live [`NumericRangeTree`] whose ownership was
///    transferred to `fs` (i.e. allocated with `Box::into_raw`).
pub unsafe fn open_numeric_or_geo_index<'a>(
    spec: &mut ffi::IndexSpec,
    fs: &'a mut ffi::FieldSpec,
    create_if_missing: bool,
    numeric_compress: bool,
) -> Option<&'a mut NumericRangeTree> {
    debug_assert!(fs.types() & (FieldType_INDEXFLD_T_NUMERIC | FieldType_INDEXFLD_T_GEO) != 0);

    if fs.tree.is_null() && create_if_missing {
        let tree = NumericRangeTree::new(numeric_compress);
        // Update the spec's inverted index size with the new tree's initial root range size.

        let initial_size = tree.root().range().map_or(0, |r| r.memory_usage());
        let tree = Box::into_raw(Box::new(tree));
        fs.tree = tree.cast();
        spec.stats.invertedSize += initial_size;
    }

    if fs.tree.is_null() {
        None
    } else {
        // SAFETY: 2. fs.tree is non-null and points to a live NumericRangeTree.
        Some(unsafe { &mut *fs.tree.cast::<NumericRangeTree>() })
    }
}

/// Selects the correct numeric reader variant based on the filter.
///
/// - No filter → [`NumericIteratorVariant::Unfiltered`]
/// - Numeric filter (no geo sub-filter) → [`NumericIteratorVariant::Filtered`]
/// - Geo filter → [`NumericIteratorVariant::Geo`]
pub enum NumericIteratorVariant<'index> {
    /// No filter: iterates all entries in the range.
    Unfiltered(Numeric<'index, NumericIndexReader<'index>, FieldExpirationChecker>),
    /// Numeric filter: skips entries outside the filter's value range.
    Filtered(
        Numeric<'index, FilterNumericReader<NumericIndexReader<'index>>, FieldExpirationChecker>,
    ),
    /// Geo filter: skips entries that do not pass the geo predicate.
    Geo(Numeric<'index, FilterGeoReader<NumericIndexReader<'index>>, FieldExpirationChecker>),
}

impl<'index> NumericIteratorVariant<'index> {
    /// Creates a [`NumericIteratorVariant`] for each range in `tree` matching `filter`.
    ///
    /// # Returns
    ///
    /// One variant per matching range. Empty when no ranges match.
    ///
    /// # Safety
    ///
    /// 1. `sctx` and `sctx.spec` must remain valid for the lifetime of all returned iterators.
    /// 2. `field_ctx.field` must be a field index (tag == `FieldMaskOrIndex::Index`), not a field mask.
    pub unsafe fn from_tree(
        tree: &'index NumericRangeTree,
        sctx: NonNull<ffi::RedisSearchCtx>,
        filter: &'index NumericFilter,
        field_ctx: &field::FieldFilterContext,
    ) -> Vec<Self> {
        let field_index = match field_ctx.field {
            field::FieldMaskOrIndex::Index(index) => index,
            field::FieldMaskOrIndex::Mask(_) => {
                panic!("Numeric queries require a field index, not a field mask");
            }
        };

        let ranges = tree.find(filter);

        let range_tree: Option<&NumericRangeTree> = if filter.field_spec.is_null() {
            None
        } else {
            Some(tree)
        };

        ranges
            .iter()
            .map(|range| {
                let min_val = range.min_val();
                let max_val = range.max_val();

                // Determine if we can skip the filter: if the filter is numeric (not geo)
                // and both the range min and max are within the filter bounds, the reader
                // doesn't need to check the filter for each record.
                let reader_filter = if filter.is_numeric_filter()
                    && filter.value_in_range(min_val)
                    && filter.value_in_range(max_val)
                {
                    None
                } else {
                    Some(filter)
                };

                let reader = range.entries().reader();

                // SAFETY: 1. guarantees `sctx` and `sctx.spec` are valid for the iterators' lifetime.
                let expiration_checker = unsafe {
                    crate::FieldExpirationChecker::new(
                        sctx,
                        field::FieldFilterContext {
                            field: field::FieldMaskOrIndex::Index(field_index),
                            predicate: field_ctx.predicate,
                        },
                        reader.flags(),
                    )
                };

                Self::new(
                    reader,
                    reader_filter,
                    expiration_checker,
                    range_tree,
                    min_val,
                    max_val,
                )
            })
            .collect()
    }

    /// Create the correct iterator variant for the given reader and optional filter.
    ///
    /// The variant is selected as follows:
    /// - `filter` is `None` → [`NumericIteratorVariant::Unfiltered`]
    /// - `filter` is `Some(f)` where `f.is_numeric_filter()` → [`NumericIteratorVariant::Filtered`]
    /// - `filter` is `Some(f)` where `!f.is_numeric_filter()` → [`NumericIteratorVariant::Geo`]
    pub fn new(
        reader: NumericIndexReader<'index>,
        filter: Option<&'index NumericFilter>,
        expiration_checker: FieldExpirationChecker,
        range_tree: Option<&'index NumericRangeTree>,
        range_min: f64,
        range_max: f64,
    ) -> Self {
        match filter {
            None => {
                // SAFETY: `range_tree` lifetime is enforced by `'index`.
                let iter = unsafe {
                    Numeric::new(
                        reader,
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                Self::Unfiltered(iter)
            }
            Some(f) if f.is_numeric_filter() => {
                // SAFETY: `range_tree` lifetime is enforced by `'index`.
                let iter = unsafe {
                    Numeric::new(
                        FilterNumericReader::new(*f, reader),
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                Self::Filtered(iter)
            }
            Some(f) => {
                // SAFETY: `range_tree` lifetime is enforced by `'index`.
                let iter = unsafe {
                    Numeric::new(
                        FilterGeoReader::new(*f, reader),
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                Self::Geo(iter)
            }
        }
    }

    /// Returns the cached flags of the underlying index reader.
    pub const fn flags(&self) -> IndexFlags {
        match self {
            Self::Unfiltered(iter) => iter.flags(),
            Self::Filtered(iter) => iter.flags(),
            Self::Geo(iter) => iter.flags(),
        }
    }

    /// Returns the minimum value of the numeric range (used for profiling).
    pub const fn range_min(&self) -> f64 {
        match self {
            Self::Unfiltered(iter) => iter.range_min(),
            Self::Filtered(iter) => iter.range_min(),
            Self::Geo(iter) => iter.range_min(),
        }
    }

    /// Returns the maximum value of the numeric range (used for profiling).
    pub const fn range_max(&self) -> f64 {
        match self {
            Self::Unfiltered(iter) => iter.range_max(),
            Self::Filtered(iter) => iter.range_max(),
            Self::Geo(iter) => iter.range_max(),
        }
    }
}

impl<'index> RQEIterator<'index> for NumericIteratorVariant<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match self {
            Self::Unfiltered(iter) => iter.current(),
            Self::Filtered(iter) => iter.current(),
            Self::Geo(iter) => iter.current(),
        }
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        match self {
            Self::Unfiltered(iter) => iter.read(),
            Self::Filtered(iter) => iter.read(),
            Self::Geo(iter) => iter.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        match self {
            Self::Unfiltered(iter) => iter.skip_to(doc_id),
            Self::Filtered(iter) => iter.skip_to(doc_id),
            Self::Geo(iter) => iter.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match self {
            Self::Unfiltered(iter) => iter.rewind(),
            Self::Filtered(iter) => iter.rewind(),
            Self::Geo(iter) => iter.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match self {
            Self::Unfiltered(iter) => iter.num_estimated(),
            Self::Filtered(iter) => iter.num_estimated(),
            Self::Geo(iter) => iter.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        match self {
            Self::Unfiltered(iter) => iter.last_doc_id(),
            Self::Filtered(iter) => iter.last_doc_id(),
            Self::Geo(iter) => iter.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match self {
            Self::Unfiltered(iter) => iter.at_eof(),
            Self::Filtered(iter) => iter.at_eof(),
            Self::Geo(iter) => iter.at_eof(),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self {
            Self::Unfiltered(iter) => iter.revalidate(spec),
            Self::Filtered(iter) => iter.revalidate(spec),
            Self::Geo(iter) => iter.revalidate(spec),
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxNumeric
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl ProfilePrint for NumericIteratorVariant<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        match self {
            Self::Unfiltered(it) => it.print_profile(map, ctx),
            Self::Filtered(it) => it.print_profile(map, ctx),
            Self::Geo(it) => {
                use crate::RQEIterator as _;

                let se_hash = geo::hash::GeoHashBits {
                    bits: it.range_min() as u64,
                    step: geo::hash::GEO_STEP_MAX,
                };
                let nw_hash = geo::hash::GeoHashBits {
                    bits: it.range_max() as u64,
                    step: geo::hash::GEO_STEP_MAX,
                };
                let (se_lon, se_lat) = geo::hash::decode_to_lon_lat(se_hash);
                let (nw_lon, nw_lat) = geo::hash::decode_to_lon_lat(nw_hash);
                map.kv_simple_string(c"Type", c"GEO");
                let se = [se_lon.into_inner(), se_lat.into_inner()];
                let nw = [nw_lon.into_inner(), nw_lat.into_inner()];
                let term_str = format!(
                    "{},{} - {},{}",
                    format_g(se[0]),
                    format_g(se[1]),
                    format_g(nw[0]),
                    format_g(nw[1]),
                );
                let term_cstr = std::ffi::CString::new(term_str).unwrap();
                map.kv_simple_string(c"Term", &term_cstr);
                ctx.print_optional_counters(map);
                map.kv_long_long(c"Estimated number of matches", it.num_estimated() as i64);
            }
        }
    }
}

/// Build a numeric (or geo) filter iterator over all matching sub-ranges of the
/// field's [`NumericRangeTree`].
///
/// Opens the field's range tree, collects one iterator per matching sub-range
/// (a [`NumericIteratorVariant`] each), and combines them with
/// [`build_union`](crate::union_opaque::build_union). The node type recorded on
/// the union is [`Numeric`](QueryNodeType::Numeric) or [`Geo`](QueryNodeType::Geo)
/// depending on the filter.
///
/// Returns [`None`] — an empty (matchless) result, not an error — when the index
/// does not exist for the field (nothing indexed yet) or when no sub-range
/// matches the filter.
///
/// # Safety
///
/// 1. `sctx.spec` must be a valid non-null [`IndexSpec`](ffi::IndexSpec); `sctx`
///    and its spec must remain valid for the lifetime of the returned iterator.
/// 2. `flt.field_spec` must be a valid non-null pointer to a [`FieldSpec`](ffi::FieldSpec)
///    for a numeric or geo field, remaining valid for the lifetime of the
///    returned iterator.
/// 3. `field_ctx.field` must be a field index (not a field mask).
pub unsafe fn build_numeric_filter_iterator(
    sctx: &RedisSearchCtx,
    flt: &NumericFilter,
    min_union_iter_heap: usize,
    field_ctx: &field::FieldFilterContext,
    compress: bool,
) -> Option<NonNull<QueryIterator>> {
    let node_type = if flt.is_numeric_filter() {
        QueryNodeType::Numeric
    } else {
        QueryNodeType::Geo
    };

    // SAFETY: precondition (1) — `sctx.spec` is valid and non-null.
    let spec = unsafe { &mut *sctx.spec };
    // SAFETY: precondition (2) — `flt.field_spec` is valid and non-null.
    let fs = unsafe { &mut *(flt.field_spec as *mut ffi::FieldSpec) };
    // SAFETY: `spec`/`fs` are valid (1, 2); the field is numeric/geo so the tree
    // is the right type. We never create the tree here (`create_if_missing` is
    // false), so the `fs.tree` ownership precondition is trivially upheld.
    let tree = unsafe { open_numeric_or_geo_index(spec, fs, false, compress) }?;

    // SAFETY: `sctx`/`sctx.spec` remain valid (1); `field_ctx.field` is a field
    // index (3).
    let variants =
        unsafe { NumericIteratorVariant::from_tree(tree, NonNull::from(sctx), flt, field_ctx) };
    if variants.is_empty() {
        return None;
    }

    let children: Vec<CRQEIterator> = variants
        .into_iter()
        .map(CRQEIterator::from_rust_leaf)
        .collect();

    let iter =
        crate::union_opaque::build_union(children, true, min_union_iter_heap, node_type, 1.0);
    Some(iter)
}

/// [`Suspended`]-mode counterpart of [`NumericIteratorVariant`] used
/// as its `RQEIteratorBoxed::Suspended` type. Each variant holds the
/// `Suspended` form of the corresponding `Numeric` instantiation, retaining
/// the `'query` lifetime so query-attached borrows stay valid across the
/// suspend/resume cycle.
pub enum NumericIteratorVariantSuspended<'query> {
    /// Suspended counterpart of [`NumericIteratorVariant::Unfiltered`].
    ///
    /// The trailing `RA` type argument is the frozen **active** reader (see
    /// [`RawInvIndIterator`]'s `RA`), matching what `Numeric::suspend` produces.
    Unfiltered(
        RawNumeric<
            'query,
            Suspended,
            RawNumericIndexReader<Suspended>,
            FieldExpirationChecker,
            NumericIndexReader<'query>,
        >,
    ),
    /// Suspended counterpart of [`NumericIteratorVariant::Filtered`].
    Filtered(
        RawNumeric<
            'query,
            Suspended,
            FilterNumericReader<RawNumericIndexReader<Suspended>>,
            FieldExpirationChecker,
            FilterNumericReader<NumericIndexReader<'query>>,
        >,
    ),
    /// Suspended counterpart of [`NumericIteratorVariant::Geo`].
    Geo(
        RawNumeric<
            'query,
            Suspended,
            FilterGeoReader<RawNumericIndexReader<Suspended>>,
            FieldExpirationChecker,
            FilterGeoReader<NumericIndexReader<'query>>,
        >,
    ),
}

impl<'query> NumericIteratorVariantSuspended<'query> {
    /// Mirror of [`NumericIteratorVariant::range_min`] on the suspended side.
    pub const fn range_min(&self) -> f64 {
        match self {
            Self::Unfiltered(iter) => iter.range_min(),
            Self::Filtered(iter) => iter.range_min(),
            Self::Geo(iter) => iter.range_min(),
        }
    }

    /// Mirror of [`NumericIteratorVariant::range_max`] on the suspended side.
    pub const fn range_max(&self) -> f64 {
        match self {
            Self::Unfiltered(iter) => iter.range_max(),
            Self::Filtered(iter) => iter.range_max(),
            Self::Geo(iter) => iter.range_max(),
        }
    }

    /// Mirror of [`NumericIteratorVariant::flags`] on the suspended side.
    pub const fn flags(&self) -> ffi::IndexFlags {
        match self {
            Self::Unfiltered(iter) => iter.flags(),
            Self::Filtered(iter) => iter.flags(),
            Self::Geo(iter) => iter.flags(),
        }
    }
}

impl<'index> RQEIteratorBoxed<'index> for NumericIteratorVariant<'index> {
    type Suspended = NumericIteratorVariantSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Field-by-field dispatch: the enum tag's discriminant layout is
        // unspecified across these two distinct enum types, so we can't
        // whole-box-cast. Match arms call the inner `RawNumeric`'s
        // `RQEIteratorBoxed::suspend` (disambiguated via UFCS so it isn't
        // resolved against an inherent or auto-impl `suspend`) and re-wrap
        // into the suspended enum.
        match *self {
            NumericIteratorVariant::Unfiltered(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NumericIteratorVariantSuspended::Unfiltered(*suspended))
            }
            NumericIteratorVariant::Filtered(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NumericIteratorVariantSuspended::Filtered(*suspended))
            }
            NumericIteratorVariant::Geo(it) => {
                let suspended = RQEIteratorBoxed::suspend(Box::new(it));
                Box::new(NumericIteratorVariantSuspended::Geo(*suspended))
            }
        }
    }
}

impl<'query> RQESuspendedIterator<'query> for NumericIteratorVariantSuspended<'query> {
    type Resumed<'a>
        = NumericIteratorVariant<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Forward the inner variant's outcome: an aborted inner aborts the
        // whole wrapper; otherwise reconstruct the concrete
        // `NumericIteratorVariant` and preserve the `Ok`/`Moved` status.
        let (variant, moved) = match *self {
            NumericIteratorVariantSuspended::Unfiltered(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NumericIteratorVariant::Unfiltered(*active), false),
                ResumeOutcome::Moved(active) => (NumericIteratorVariant::Unfiltered(*active), true),
            },
            NumericIteratorVariantSuspended::Filtered(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NumericIteratorVariant::Filtered(*active), false),
                ResumeOutcome::Moved(active) => (NumericIteratorVariant::Filtered(*active), true),
            },
            NumericIteratorVariantSuspended::Geo(it) => match Box::new(it).resume(guard)? {
                ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
                ResumeOutcome::Ok(active) => (NumericIteratorVariant::Geo(*active), false),
                ResumeOutcome::Moved(active) => (NumericIteratorVariant::Geo(*active), true),
            },
        };
        let active = Box::new(variant);
        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            NumericIteratorVariantSuspended::Unfiltered(it) => {
                RQESuspendedIterator::last_doc_id(it)
            }
            NumericIteratorVariantSuspended::Filtered(it) => RQESuspendedIterator::last_doc_id(it),
            NumericIteratorVariantSuspended::Geo(it) => RQESuspendedIterator::last_doc_id(it),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            NumericIteratorVariantSuspended::Unfiltered(it) => {
                RQESuspendedIterator::num_estimated(it)
            }
            NumericIteratorVariantSuspended::Filtered(it) => {
                RQESuspendedIterator::num_estimated(it)
            }
            NumericIteratorVariantSuspended::Geo(it) => RQESuspendedIterator::num_estimated(it),
        }
    }
}
