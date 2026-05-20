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
    BoxedRQEIterator, FieldExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed,
    RQEIteratorError, RQESuspendedIterator, RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    expiration_checker::{ExpirationChecker, NoOpChecker},
    profile_print::{ProfilePrint, ProfilePrintCtx, format_g},
};
use ffi::{
    FieldType_INDEXFLD_T_GEO, FieldType_INDEXFLD_T_NUMERIC, IndexFlags,
    ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    FilterGeoReader, FilterNumericReader, IndexReader, NumericFilter, NumericReader,
    RefreshOutcome, ResumableReader, SuspendableReader,
};
use numeric_range_tree::{NumericIndexReader, NumericRangeTree};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use super::core::{InvIndIterator, RawInvIndIterator};

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
pub struct RawNumeric<Rf: Ref, R, E = NoOpChecker> {
    it: RawInvIndIterator<Rf, R, E>,
    /// The numeric range tree and its revision ID, used to detect changes during revalidation.
    range_tree_info: Option<RangeTreeInfo>,
    /// Minimum numeric range, only used in debug print.
    range_min: f64,
    /// Maximum numeric range, only used in debug print.
    range_max: f64,
}

/// Alias for an [`Active`] [`RawNumeric`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Numeric<'index, R, E = NoOpChecker> = RawNumeric<Active<'index>, R, E>;

/// Information about the numeric range tree backing a [`Numeric`] iterator.
struct RangeTreeInfo {
    /// Pointer to the numeric range tree.
    tree: NonNull<NumericRangeTree>,
    /// The revision ID at the time the iterator was created.
    /// Used to detect if the tree has been modified.
    revision_id: u32,
}

impl<Rf: Ref, R, E> RawNumeric<Rf, R, E> {
    /// Cached minimum numeric range (only used in debug print / FT.PROFILE).
    /// Mode-independent — the value is set at construction and never changes.
    pub const fn range_min(&self) -> f64 {
        self.range_min
    }

    /// Cached maximum numeric range (only used in debug print / FT.PROFILE).
    /// Mode-independent — see [`range_min`](Self::range_min).
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
    /// # Why mode-independent
    ///
    /// The body reads only mode-independent fields: the `NonNull` tree
    /// pointer is stable across modes, and `revision_id()` is a plain
    /// field read on a struct that's owned by the spec (its address is
    /// stable under the read lock). No `&'a` borrow of any
    /// reader/buffer field is materialized, so `should_abort` is safe
    /// to call on `RawNumeric<Suspended, RS, E>` from
    /// `RQESuspendedIterator::resume`.
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
    type Suspended = RawNumeric<Suspended, R::Suspended, E>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawNumeric` is `#[repr(C)]`. The `Rf`-dependent field is
        // the inner `RawInvIndIterator<Rf, R, E>`, whose layout is identical
        // across modes (see [`InvIndIterator::suspend`]). The remaining
        // fields (`range_tree_info`, `range_min`, `range_max`) carry no
        // `Rf` in their type, so they survive the cast unchanged.
        // Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawNumeric<Suspended, R::Suspended, E>) }
    }
}

impl<RS, E> RQESuspendedIterator for RawNumeric<Suspended, RS, E>
where
    RS: ResumableReader,
    for<'a> RS::Resumed<'a>: NumericReader<'a>,
    E: ExpirationChecker + 'static,
{
    type Resumed<'a> = Numeric<'a, RS::Resumed<'a>, E>;

    fn resume<'a>(
        mut self: Box<Self>,
        _guard: &'a IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<'a>, RQEIteratorError> {
        // Step 1: should_abort on the suspended form. Reads only
        // mode-independent fields (the cached `range_tree_info` pointer
        // and the tree's `revision_id`). On abort we drop the suspended
        // iterator without promoting it to Active — nothing is
        // materialized.
        if self.should_abort() {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: refresh reader pointers in place while still
        // suspended.
        let outcome = self.it.refresh_pointers();

        // Step 3: whole-box cast Suspended → Active.
        let raw = Box::into_raw(self);
        // SAFETY: `RawNumeric` is `#[repr(C)]` over `RawInvIndIterator<Rf, R, E>`,
        // plus mode-independent `range_tree_info` / `range_min` / `range_max`
        // fields. The `RawInvIndIterator` layout is identical across
        // modes (see `RawInvIndIterator::suspend`). The caller's read
        // lock (witnessed by `_guard`) plus the `refresh_pointers` step
        // above together ensure every pointer inside the iterator is
        // valid for `'a`.
        let mut active = unsafe { Box::from_raw(raw as *mut Numeric<'a, RS::Resumed<'a>, E>) };

        // Step 4: if GC ran, re-seek to the previous last_doc_id.
        let status = match outcome {
            RefreshOutcome::Ok => ValidateStatus_VALIDATE_OK,
            RefreshOutcome::NeedsReseek { last_doc_id } => {
                active.it.reseek_after_refresh(last_doc_id)
            }
        };
        let active = BoxedRQEIterator::new(active);
        Ok(if status == ValidateStatus_VALIDATE_MOVED {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id_field()
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
