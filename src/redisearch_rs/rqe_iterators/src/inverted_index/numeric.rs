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
    FilterGeoReader, FilterNumericReader, IndexReader, NumericFilter, NumericReader, ReseatIndex,
    ResumableReader, SuspendableReader,
};
use numeric_range_tree::{
    NodeIndex, NumericIndex, NumericIndexReader, NumericRangeTree, RangeWindow,
    RawNumericIndexReader,
};
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

/// The range a [`Numeric`] iterator reads, located as a node of the tree that
/// owns it, so revalidation can resolve it again.
#[derive(Clone, Copy)]
pub struct RangeSource {
    /// The tree owning the range. A raw pointer rather than a reference, so
    /// that it stays usable across the writes [`RangeSource::new`] allows.
    tree: NonNull<NumericRangeTree>,
    /// The node holding the range. It resolves to the same range only while the
    /// tree's [revision](NumericRangeTree::revision_id) is unchanged.
    node: NodeIndex,
}

impl RangeSource {
    /// Locate a range as `node` of `tree`.
    ///
    /// # Safety
    ///
    /// 1. `tree` must be [valid] for reads of a [`NumericRangeTree`] for as long
    ///    as any iterator built with this source lives.
    /// 2. `tree` must be the pointer the tree's owner holds, or a raw copy of it
    ///    — not one derived from a reference to the tree, which a writer's `&mut`
    ///    would invalidate.
    /// 3. The tree must only be written to, or mutably borrowed, while every
    ///    iterator built with this source is parked across a lock release.
    ///
    /// Which range the source must locate depends on the reader it is paired
    /// with, so [`Numeric::new`] carries that requirement.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn new(tree: NonNull<NumericRangeTree>, node: NodeIndex) -> Self {
        Self { tree, node }
    }
}

/// Information about the numeric range tree backing a [`Numeric`] iterator.
struct RangeTreeInfo {
    /// Where the reader's range lives.
    source: RangeSource,
    /// The revision ID at the time the iterator was created.
    /// Used to detect if the tree has been modified.
    revision_id: u32,
}

/// What revalidation finds when it looks the reader's range up again.
enum RangeLookup {
    /// The tree changed shape, so the range may be gone: the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    Gone,
    /// The range's entries, freshly resolved through the tree, to
    /// [reseat](inverted_index::ReseatIndex) the reader on.
    Live(NonNull<NumericIndex>),
    /// The iterator was built without a tree, so there is nothing to look up.
    Untracked,
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

    /// Look the reader's range up again through the tree.
    ///
    /// The numeric range tree's revision id changes when the tree changes shape
    /// (a node split or removal). While it is unchanged, the node the reader was
    /// opened on still holds the same range, so the range's entries are resolved
    /// afresh for the caller to [reseat](ReseatIndex) the reader on.
    ///
    /// Reads only through the tree pointer its owner holds (stable under the read
    /// lock); it materializes no `&'a` reader/buffer borrow, so it is sound to
    /// call on the suspended form during [`RQESuspendedIterator::resume`].
    fn resolve_live_index(&self) -> RangeLookup {
        let Some(ref info) = self.range_tree_info else {
            return RangeLookup::Untracked;
        };

        // SAFETY: 1. and 2. of `RangeSource::new` keep the tree valid for the
        // iterator's lifetime and its pointer valid across writes; by 3. nothing
        // writes to it or borrows it mutably while the iterator revalidates.
        let tree = unsafe { info.source.tree.as_ref() };
        if tree.revision_id() != info.revision_id {
            return RangeLookup::Gone;
        }

        let range = tree
            .node(info.source.node)
            .range()
            .expect("an unchanged revision keeps the range on its node");
        RangeLookup::Live(NonNull::from(range.entries()))
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
    /// `range_source` locates the reader's range in the tree backing the
    /// iterator. Revalidation uses it to check the iterator is still valid and to
    /// reseat the reader on the range's entries.
    ///
    /// `range_min` and `range_max` are the minimum and maximum numeric ranges,
    /// respectively. They are only used in debug print.
    ///
    /// # Safety
    ///
    /// 1. If `range_source` is Some, `node` must hold the range whose entries
    ///    `reader` reads. Revalidation reseats `reader` on that range's entries,
    ///    so pairing it with another range's reader would point the reader at an
    ///    index whose blocks its position does not describe. [`RangeSource::new`]
    ///    carries the requirements on the source itself.
    /// 2. If `range_source` is None, nothing may write to the index `reader`
    ///    reads while the iterator lives, as revalidation then has no index to
    ///    [reseat](ReseatIndex) `reader` on.
    pub unsafe fn new(
        reader: R,
        expiration_checker: E,
        range_source: Option<RangeSource>,
        range_min: Option<f64>,
        range_max: Option<f64>,
    ) -> Self {
        let result = RSIndexResult::build_numeric(0.0).build();

        let range_tree_info = range_source.map(|source| {
            // SAFETY: 1. and 3. of `RangeSource::new`.
            let revision_id = unsafe { source.tree.as_ref() }.revision_id();
            RangeTreeInfo {
                source,
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
    R: NumericReader<'index> + ReseatIndex<NumericIndex> + SuspendableReader + 'index,
    R::Suspended: ResumableReader + ReseatIndex<NumericIndex>,
    for<'a> <R::Suspended as ResumableReader>::Resumed<'a>:
        NumericReader<'a> + ReseatIndex<NumericIndex>,
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
    RS: ResumableReader + ReseatIndex<NumericIndex>,
    for<'a> RS::Resumed<'a>: NumericReader<'a> + ReseatIndex<NumericIndex>,
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
        match self.resolve_live_index() {
            RangeLookup::Gone => return Ok(ResumeOutcome::Aborted),
            // `resume_in_place` first reads the index's GC marker through the
            // reader's pointer, which may not have survived a write while
            // suspended.
            // SAFETY: 1. An unchanged revision keeps the reader's range on the node
            // it was opened on. 2. `ii` was resolved just now, under the read lock
            // `guard` witnesses. 3. The revalidation protocol only lets the index
            // be written while the reader is parked across a lock release.
            RangeLookup::Live(ii) => unsafe { self.it.reader.reseat(ii) },
            RangeLookup::Untracked => {}
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
    R: NumericReader<'index> + ReseatIndex<NumericIndex>,
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
        match self.resolve_live_index() {
            RangeLookup::Gone => return Ok(RQEValidateStatus::Aborted),
            // `revalidate` reads the index's GC marker through the reader's
            // pointer, which may not have survived a write to the index.
            // SAFETY: 1. An unchanged revision keeps the reader's range on the node
            // it was opened on. 2. `ii` was resolved just now, under the read lock
            // `spec` witnesses. 3. The revalidation protocol only lets the index be
            // written while the reader is parked across a lock release.
            RangeLookup::Live(ii) => unsafe { self.it.reader.reseat(ii) },
            RangeLookup::Untracked => {}
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
        ctx.print_estimated(map);
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

/// Payload of [`RawNumericIteratorVariant::Unfiltered`]: a numeric leaf reading
/// the range's entries directly. `Rf` flows into the reader (which weakens on
/// suspend) but never into the frozen `RA` slot, which stays the **active**
/// reader — see [`RawInvIndIterator`]'s `RA` parameter.
type UnfilteredArm<'query, Rf> = RawNumeric<
    'query,
    Rf,
    RawNumericIndexReader<Rf>,
    FieldExpirationChecker,
    NumericIndexReader<'query>,
>;

/// Payload of [`RawNumericIteratorVariant::Filtered`] — [`UnfilteredArm`]'s
/// reader wrapped in a [`FilterNumericReader`].
type FilteredArm<'query, Rf> = RawNumeric<
    'query,
    Rf,
    FilterNumericReader<RawNumericIndexReader<Rf>>,
    FieldExpirationChecker,
    FilterNumericReader<NumericIndexReader<'query>>,
>;

/// Payload of [`RawNumericIteratorVariant::Geo`] — [`UnfilteredArm`]'s reader
/// wrapped in a [`FilterGeoReader`].
type GeoArm<'query, Rf> = RawNumeric<
    'query,
    Rf,
    FilterGeoReader<RawNumericIndexReader<Rf>>,
    FieldExpirationChecker,
    FilterGeoReader<NumericIndexReader<'query>>,
>;

/// Selects the correct numeric reader variant based on the filter,
/// parameterised over a [`Ref`] mode. See [`NumericIteratorVariant`] for the
/// [`Active`] instantiation that implements [`RQEIterator`], and
/// [`NumericIteratorVariantSuspended`] for its passive carrier across a lock
/// release/reacquire cycle.
///
/// - No filter → [`NumericIteratorVariant::Unfiltered`]
/// - Numeric filter (no geo sub-filter) → [`NumericIteratorVariant::Filtered`]
/// - Geo filter → [`NumericIteratorVariant::Geo`]
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `NumericIteratorVariant<'query>`
///    and `NumericIteratorVariantSuspended<'query>` are layout-identical, so
///    [`suspend`](RQEIteratorBoxed::suspend) /
///    [`resume`](RQESuspendedIterator::resume) can transition each payload in
///    place and then reinterpret the owning `Box` between the two. Being a single
///    `#[repr(C, u8)]` generic, the two share a tag encoding and variant order by
///    construction; the per-arm payload correspondence and layout identity are
///    enforced by the `const _` proof below.
#[repr(C, u8)]
pub enum RawNumericIteratorVariant<'query, Rf: Ref> {
    /// No filter: iterates all entries in the range.
    Unfiltered(UnfilteredArm<'query, Rf>),
    /// Numeric filter: skips entries outside the filter's value range.
    Filtered(FilteredArm<'query, Rf>),
    /// Geo filter: skips entries that do not pass the geo predicate.
    Geo(GeoArm<'query, Rf>),
}

/// Alias for an [`Active`] [`RawNumericIteratorVariant`] — the only
/// instantiation with an [`RQEIterator`] impl.
pub type NumericIteratorVariant<'index> = RawNumericIteratorVariant<'index, Active<'index>>;

/// [`Suspended`]-mode counterpart of [`NumericIteratorVariant`], used as its
/// [`RQEIteratorBoxed::Suspended`] type. Retains the `'query` lifetime so
/// query-attached borrows stay valid across the suspend/resume cycle.
pub type NumericIteratorVariantSuspended<'query> = RawNumericIteratorVariant<'query, Suspended>;

impl<'query, Rf: Ref> RawNumericIteratorVariant<'query, Rf> {
    /// Returns the cached flags of the underlying index reader.
    ///
    /// Available in every mode: the value is a construction-time snapshot
    /// ([`RawInvIndIterator::flags`]), so a suspended variant answers the same
    /// as the active one it came from.
    pub const fn flags(&self) -> IndexFlags {
        match self {
            Self::Unfiltered(iter) => iter.flags(),
            Self::Filtered(iter) => iter.flags(),
            Self::Geo(iter) => iter.flags(),
        }
    }

    /// Returns the minimum value of the numeric range (used for profiling).
    ///
    /// Mode-independent, for the same reason as [`Self::flags`].
    pub const fn range_min(&self) -> f64 {
        match self {
            Self::Unfiltered(iter) => iter.range_min(),
            Self::Filtered(iter) => iter.range_min(),
            Self::Geo(iter) => iter.range_min(),
        }
    }

    /// Returns the maximum value of the numeric range (used for profiling).
    ///
    /// Mode-independent, for the same reason as [`Self::flags`].
    pub const fn range_max(&self) -> f64 {
        match self {
            Self::Unfiltered(iter) => iter.range_max(),
            Self::Filtered(iter) => iter.range_max(),
            Self::Geo(iter) => iter.range_max(),
        }
    }
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
    /// 3. `tree` must satisfy the requirements of [`RangeSource::new`] for
    ///    `'index`.
    pub unsafe fn from_tree(
        tree: NonNull<NumericRangeTree>,
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

        // SAFETY: 3.
        let tree_ref: &'index NumericRangeTree = unsafe { tree.as_ref() };

        // The optimizer paginates the sort field by changing the filter's window
        // between rewinds; every other query leaves it unbounded.
        let nodes = tree_ref.find_windowed_nodes(filter, RangeWindow::from_filter(filter));

        nodes
            .into_iter()
            .map(|node| {
                let range = tree_ref
                    .node(node)
                    .range()
                    .expect("found nodes hold a range");
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

                // SAFETY: by 3.
                let source = unsafe { RangeSource::new(tree, node) };
                // SAFETY: 1. `reader` reads the range `node` holds.
                unsafe {
                    Self::new(
                        reader,
                        reader_filter,
                        expiration_checker,
                        Some(source),
                        min_val,
                        max_val,
                    )
                }
            })
            .collect()
    }

    /// Create the correct iterator variant for the given reader and optional filter.
    ///
    /// The variant is selected as follows:
    /// - `filter` is `None` → [`NumericIteratorVariant::Unfiltered`]
    /// - `filter` is `Some(f)` where `f.is_numeric_filter()` → [`NumericIteratorVariant::Filtered`]
    /// - `filter` is `Some(f)` where `!f.is_numeric_filter()` → [`NumericIteratorVariant::Geo`]
    ///
    /// # Safety
    ///
    /// The requirements of [`Numeric::new`] on `range_source` and `reader`.
    pub unsafe fn new(
        reader: NumericIndexReader<'index>,
        filter: Option<&'index NumericFilter>,
        expiration_checker: FieldExpirationChecker,
        range_source: Option<RangeSource>,
        range_min: f64,
        range_max: f64,
    ) -> Self {
        match filter {
            None => {
                // SAFETY: forwarded from this function's contract.
                let iter = unsafe {
                    Numeric::new(
                        reader,
                        expiration_checker,
                        range_source,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                Self::Unfiltered(iter)
            }
            Some(f) if f.is_numeric_filter() => {
                // SAFETY: forwarded from this function's contract; the filtering
                // readers read the same range as `reader`.
                let iter = unsafe {
                    Numeric::new(
                        FilterNumericReader::new(*f, reader),
                        expiration_checker,
                        range_source,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                Self::Filtered(iter)
            }
            Some(f) => {
                // SAFETY: forwarded from this function's contract; the filtering
                // readers read the same range as `reader`.
                let iter = unsafe {
                    Numeric::new(
                        FilterGeoReader::new(*f, reader),
                        expiration_checker,
                        range_source,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                Self::Geo(iter)
            }
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
                ctx.print_estimated(map);
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
/// 2. `flt.field_index` must be within `sctx.spec`'s current field count, and the
///    field at that index must be of numeric or geo type.
/// 3. `field_ctx.field` must be a field index (not a field mask).
pub unsafe fn build_numeric_filter_iterator(
    sctx: &RedisSearchCtx,
    flt: &NumericFilter,
    min_union_iter_heap: usize,
    field_ctx: &field::FieldFilterContext,
) -> Option<NonNull<QueryIterator>> {
    let node_type = if flt.is_numeric_filter() {
        QueryNodeType::Numeric
    } else {
        QueryNodeType::Geo
    };

    // SAFETY: precondition (1) — `sctx.spec` is valid and non-null.
    let spec = unsafe { &mut *sctx.spec };
    debug_assert!(
        flt.field_index < spec.numFields,
        "field_index must be within the spec's current field count"
    );
    // SAFETY: `field_index` is within `spec.numFields` (checked above), so this
    // stays within the bounds of the `numFields`-sized array `spec.fields` points to.
    let fs_ptr = unsafe { spec.fields.add(flt.field_index as usize) };
    // SAFETY: precondition (2) — `field_index` was captured from the field's own
    // stable index when the filter was built; `IndexSpec.fields` only grows between
    // then and now (existing indices stay valid), and `spec` is the current field
    // array, read under the lock evaluation holds.
    let fs = unsafe { &*fs_ptr };
    // The field is numeric/geo, so its tree is a `NumericRangeTree`. Copy the
    // owner's pointer rather than open the tree through a `&mut`, as
    // `RangeSource::new` requires.
    let tree = NonNull::new(fs.tree.cast::<NumericRangeTree>())?;

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

// Compile-time proof of invariant 1 on `RawNumericIteratorVariant`: a
// `Box<NumericIteratorVariant>` can be reinterpreted as a
// `Box<NumericIteratorVariantSuspended>` and back, as `suspend`/`resume` below
// do. Because both are instantiations of the *same* `#[repr(C, u8)]` enum, the
// variant order and tag encoding agree by construction and need no assertion.
// What remains to be proven:
//
// (a) **Arm correspondence.** `suspend` fills each payload slot with
//     `<active arm as RQEIteratorBoxed>::Suspended` — see
//     [`crate::boxed::suspend_child_slot_in_place`] — and only then relabels the
//     owning box. That is sound only if the suspended enum's matching arm *is*
//     that projection. Asserted below as a type equality rather than a layout
//     comparison: it is the property the reinterpretation actually needs, and a
//     mismatch (e.g. a reader whose `SuspendableReader::Suspended` does not
//     simply weaken `Rf`) becomes a build error instead of a silently
//     mistyped payload.
//
// (b) **Per-arm payload layout identity.** Implied by (a) together with
//     invariant 1 on `RawInvIndIterator` (proven in `core.rs` for a
//     representative reader) and the reader-side layout invariants that back the
//     `SuspendableReader`/`ResumableReader` impls of `RawNumericIndexReader`
//     (const proof in `numeric_range_tree::index`), `FilterNumericReader` and
//     `FilterGeoReader` (const proofs in `inverted_index`). Asserted per arm
//     anyway so a regression names the arm that broke, and because under
//     `#[repr(C, u8)]` each payload sits at an offset derived from *that
//     payload's* alignment — per-arm alignment equality, not the enum's, is what
//     pins the payload offsets. (`offset_of!` into enum variants is not yet
//     stable, so the offsets cannot be asserted directly.)
//
// (c) **Enum size/alignment equality.** Needed on its own by `resume`'s
//     abort/error paths, which free an allocation created for the active enum
//     using `Layout::new::<NumericIteratorVariantSuspended>()`.
const _: () = {
    use std::mem::{align_of, size_of};

    /// Witnesses that `Self` and `T` are the same type: the blanket impl is the
    /// only one, so a `A: IsSame<B>` bound holds exactly when `A` *is* `B`.
    trait IsSame<T> {}
    impl<T> IsSame<T> for T {}

    /// (a) — fails to compile unless suspending `A` yields exactly `S`.
    const fn assert_suspends_to<A, S>()
    where
        A: RQEIteratorBoxed<'static>,
        A::Suspended: IsSame<S>,
    {
    }

    assert_suspends_to::<UnfilteredArm<'static, Active<'static>>, UnfilteredArm<'static, Suspended>>(
    );
    assert_suspends_to::<FilteredArm<'static, Active<'static>>, FilteredArm<'static, Suspended>>();
    assert_suspends_to::<GeoArm<'static, Active<'static>>, GeoArm<'static, Suspended>>();

    // (b)
    assert!(
        size_of::<UnfilteredArm<'static, Active<'static>>>()
            == size_of::<UnfilteredArm<'static, Suspended>>()
    );
    assert!(
        align_of::<UnfilteredArm<'static, Active<'static>>>()
            == align_of::<UnfilteredArm<'static, Suspended>>()
    );
    assert!(
        size_of::<FilteredArm<'static, Active<'static>>>()
            == size_of::<FilteredArm<'static, Suspended>>()
    );
    assert!(
        align_of::<FilteredArm<'static, Active<'static>>>()
            == align_of::<FilteredArm<'static, Suspended>>()
    );
    assert!(
        size_of::<GeoArm<'static, Active<'static>>>() == size_of::<GeoArm<'static, Suspended>>()
    );
    assert!(
        align_of::<GeoArm<'static, Active<'static>>>() == align_of::<GeoArm<'static, Suspended>>()
    );

    // (c)
    assert!(
        size_of::<NumericIteratorVariant<'static>>()
            == size_of::<NumericIteratorVariantSuspended<'static>>()
    );
    assert!(
        align_of::<NumericIteratorVariant<'static>>()
            == align_of::<NumericIteratorVariantSuspended<'static>>()
    );
};

impl<'index> RQEIteratorBoxed<'index> for NumericIteratorVariant<'index> {
    type Suspended = NumericIteratorVariantSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Transition the payload in place, arm by arm. Both enums are the same
        // `#[repr(C, u8)]` generic at different `Rf`, so they share a tag
        // encoding and variant order; the helper writes exactly the arm's
        // `Suspended` projection, which invariant 1's proof (a) pins to the
        // matching suspended arm. Together those make the final whole-box cast
        // sound. The tag byte itself is untouched.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match.
        match unsafe { &mut *raw } {
            NumericIteratorVariant::Unfiltered(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload; the
                // helper reinitialises the slot as its `Suspended` form in place.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            NumericIteratorVariant::Filtered(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            NumericIteratorVariant::Geo(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
        }
        // SAFETY: the payload now holds its `Suspended` form at the same offset,
        // and the tag encodes the same variant in both enums. `Box::from_raw`
        // reuses the same allocation, so the box address is preserved.
        unsafe { Box::from_raw(raw.cast::<NumericIteratorVariantSuspended<'index>>()) }
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
        let raw = Box::into_raw(self);
        // Resume the payload in place, arm by arm — the tag byte is untouched,
        // so the whole-box cast below lands on the same variant of the active
        // enum (same `#[repr(C, u8)]` generic at a different `Rf`; arm
        // correspondence and per-arm layout identity are invariant 1's const
        // proof above).
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match. On `Unchanged`/`Moved` the helper rewrites the payload as its
        // resumed form; on `Aborted`/`Err` it consumes the payload, leaving it
        // uninitialised (handled below).
        let outcome = match unsafe { &mut *raw } {
            NumericIteratorVariantSuspended::Unfiltered(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            NumericIteratorVariantSuspended::Filtered(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            NumericIteratorVariantSuspended::Geo(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
        };

        match outcome {
            Ok(crate::boxed::ResumeSlotOutcome::Unchanged) => {
                // SAFETY: the payload holds its resumed form at the same offset
                // and the tag is unchanged; `Box::from_raw` reuses the same
                // allocation, so the box address — and the FFI's cached
                // `header.current` into the payload's result — stay valid.
                let active = unsafe { Box::from_raw(raw.cast::<NumericIteratorVariant<'a>>()) };
                Ok(ResumeOutcome::Ok(active))
            }
            Ok(crate::boxed::ResumeSlotOutcome::Moved) => {
                // SAFETY: as above.
                let active = unsafe { Box::from_raw(raw.cast::<NumericIteratorVariant<'a>>()) };
                Ok(ResumeOutcome::Moved(active))
            }
            Ok(crate::boxed::ResumeSlotOutcome::Aborted) => {
                // The payload was consumed; the allocation holds only the tag
                // and an uninitialised payload, so it must be freed without
                // dropping anything.
                // SAFETY: `raw` was allocated by `Box` with exactly this layout;
                // nothing in it is live any more.
                unsafe {
                    std::alloc::dealloc(
                        raw.cast::<u8>(),
                        std::alloc::Layout::new::<NumericIteratorVariantSuspended<'query>>(),
                    )
                };
                Ok(ResumeOutcome::Aborted)
            }
            Err(e) => {
                // As `Aborted`: the payload was consumed; free the allocation
                // without dropping it.
                // SAFETY: as above.
                unsafe {
                    std::alloc::dealloc(
                        raw.cast::<u8>(),
                        std::alloc::Layout::new::<NumericIteratorVariantSuspended<'query>>(),
                    )
                };
                Err(e)
            }
        }
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
