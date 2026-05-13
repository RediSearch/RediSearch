/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::{GeoDistance_GEO_DISTANCE_M, GeoFilter, IndexFlags_Index_StoreNumeric, t_docId};
use inverted_index::{
    FilterNumericReader, IndexReader, InvertedIndex, NumericFilter, NumericReader, RSIndexResult,
};
use rqe_iterators::{
    IteratorType, NoOpChecker, RQEIterator, RQEValidateStatus, SkipToOutcome,
    inverted_index::Numeric,
};

use crate::inverted_index::utils::BaseTest;
use rqe_iterators_test_utils::MockContext;

/// Builder for creating a Numeric iterator with optional parameters.
#[allow(dead_code)]
struct NumericBuilder<'index, R, E = NoOpChecker> {
    reader: R,
    range_tree: Option<NonNull<numeric_range_tree::NumericRangeTree>>,
    range_min: Option<f64>,
    range_max: Option<f64>,
    expiration_checker: E,
    _marker: std::marker::PhantomData<&'index ()>,
}

#[allow(dead_code)]
impl<'index, R> NumericBuilder<'index, R, NoOpChecker>
where
    R: NumericReader<'index>,
{
    /// Create a new builder with the required parameters.
    ///
    /// All other parameters are optional and will use sensible defaults:
    /// - `range_tree`: None
    /// - `range_min`: None
    /// - `range_max`: None
    /// - `expiration_checker`: NoOpChecker
    fn new(reader: R) -> Self {
        Self {
            reader,
            range_tree: None,
            range_min: None,
            range_max: None,
            expiration_checker: NoOpChecker,
            _marker: std::marker::PhantomData,
        }
    }
}

#[allow(dead_code)]
impl<'index, R, E> NumericBuilder<'index, R, E>
where
    R: NumericReader<'index>,
    E: rqe_iterators::ExpirationChecker,
{
    /// Set the numeric range tree.
    fn range_tree(mut self, range_tree: NonNull<numeric_range_tree::NumericRangeTree>) -> Self {
        self.range_tree = Some(range_tree);
        self
    }

    /// Set the minimum numeric range (for debug printing).
    fn range_min(mut self, range_min: f64) -> Self {
        self.range_min = Some(range_min);
        self
    }

    /// Set the maximum numeric range (for debug printing).
    fn range_max(mut self, range_max: f64) -> Self {
        self.range_max = Some(range_max);
        self
    }

    /// Set the expiration checker.
    fn expiration_checker<E2: rqe_iterators::ExpirationChecker>(
        self,
        checker: E2,
    ) -> NumericBuilder<'index, R, E2> {
        NumericBuilder {
            reader: self.reader,
            range_tree: self.range_tree,
            range_min: self.range_min,
            range_max: self.range_max,
            expiration_checker: checker,
            _marker: std::marker::PhantomData,
        }
    }

    /// Build the Numeric iterator.
    fn build(self) -> Numeric<'index, R, E> {
        let tree = self.range_tree.map(|t| unsafe { t.as_ref() });
        // SAFETY: `range_tree`, when provided, is a valid pointer to a
        // `NumericRangeTree` that outlives the returned iterator.
        unsafe {
            Numeric::new(
                self.reader,
                self.expiration_checker,
                tree,
                self.range_min,
                self.range_max,
            )
        }
    }
}

struct NumericBaseTest {
    test: BaseTest<inverted_index::numeric::Numeric>,
}

impl NumericBaseTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        // The numeric record has a value of `doc_id * 2.0`.
        RSIndexResult::build_numeric(doc_id as f64 * 2.0)
            .doc_id(doc_id)
            .build()
    }

    fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_StoreNumeric,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }

    fn create_iterator(
        &self,
    ) -> Numeric<
        '_,
        inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>,
        NoOpChecker,
    > {
        let reader = self.test.ii.reader();

        NumericBuilder::new(reader)
            .range_tree(self.test.mock_ctx.numeric_range_tree())
            .build()
    }
}

#[test]
fn numeric_type() {
    let test = NumericBaseTest::new(10);
    let it = test.create_iterator();
    assert_eq!(it.type_(), IteratorType::InvIdxNumeric);
}

#[test]
/// test reading from Numeric iterator
fn numeric_read() {
    let test = NumericBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.read(&mut it, test.test.docs_ids_iter());

    // same but using a passthrough filter
    let test = NumericBaseTest::new(100);
    let filter = NumericFilter::default();
    let reader = test.test.ii.reader();
    let reader = FilterNumericReader::new(&filter, reader);
    let mut it = NumericBuilder::new(reader)
        .range_tree(test.test.mock_ctx.numeric_range_tree())
        .build();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Numeric iterator
fn numeric_skip_to() {
    let test = NumericBaseTest::new(10);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Numeric iterator with a filter
fn numeric_filter() {
    let test = NumericBaseTest::new(100);
    let filter = NumericFilter {
        min: 50.0,
        max: 75.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, test.test.ii.reader());
    let mut it = NumericBuilder::new(reader)
        .range_tree(test.test.mock_ctx.numeric_range_tree())
        .build();
    let docs_ids = test
        .test
        .docs_ids_iter()
        // records have a numeric value of twice their doc id
        .filter(|id| *id * 2 >= 50 && *id * 2 <= 75);
    test.test.read(&mut it, docs_ids);
}

#[test]
fn skip_multi_id() {
    // Add multiple entries with the same docId
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(3.0).doc_id(1).build());

    let context = MockContext::new(0, 0);
    let mut it = NumericBuilder::new(ii.reader())
        .range_tree(context.numeric_range_tree())
        .build();

    // Read the first entry. Expect to get the entry with value 1.0
    let record = it
        .read()
        .expect("failed to read")
        .expect("expected result not eof");
    assert_eq!(record.doc_id, 1);
    assert_eq!(record.as_numeric(), Some(1.0));
    assert_eq!(it.last_doc_id(), 1);
    assert!(!it.at_eof());

    // Read the next entry. Expect EOF since we have only one unique docId
    assert_eq!(it.read().unwrap(), None);
    assert!(it.at_eof());
}

#[test]
fn skip_multi_id_and_value() {
    // Add multiple entries with the same docId and numeric value
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());

    let context = MockContext::new(0, 0);
    let mut it = NumericBuilder::new(ii.reader())
        .range_tree(context.numeric_range_tree())
        .build();

    // Read the first entry. Expect to get the entry with value 1.0
    let record = it
        .read()
        .expect("failed to read")
        .expect("expected result not eof");
    assert_eq!(record.doc_id, 1);
    assert_eq!(record.as_numeric(), Some(1.0));
    assert_eq!(it.last_doc_id(), 1);
    assert!(!it.at_eof());

    // Read the next entry. Expect EOF since we have only one unique docId
    assert_eq!(it.read().unwrap(), None);
    assert!(it.at_eof());
}

#[test]
fn get_correct_value() {
    // Add entries with the same ID but different values
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(3.0).doc_id(1).build());

    // Create an iterator that reads only entries with value >= 2.0
    let filter = NumericFilter {
        min: 2.0,
        max: 3.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, ii.reader());

    let context = MockContext::new(0, 0);
    let mut it = NumericBuilder::new(reader)
        .range_tree(context.numeric_range_tree())
        .build();

    // Read the first entry. Expect to get the entry with value 2.0
    let record = it
        .read()
        .expect("failed to read")
        .expect("expected result not eof");
    assert_eq!(record.doc_id, 1);
    assert_eq!(record.as_numeric(), Some(2.0));
    assert_eq!(it.last_doc_id(), 1);
    assert!(!it.at_eof());

    // Read the next entry. Expect EOF since we have only one unique docId with value 2.0
    assert_eq!(it.read().unwrap(), None);
    assert!(it.at_eof());
}

#[test]
fn eof_after_filtering() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);

    // Fill the index with entries, all with value 1.0
    for id in 1..=1234 {
        let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(id).build());
    }

    // Create an iterator that reads only entries with value 2.0
    let filter = NumericFilter {
        min: 2.0,
        max: 2.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, ii.reader());
    let context = MockContext::new(0, 0);
    let mut it = NumericBuilder::new(reader)
        .range_tree(context.numeric_range_tree())
        .build();

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    assert_eq!(it.skip_to(1).expect("skip_to failed"), None);
}

#[test]
fn numeric_range() {
    let ii = InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);

    let it = NumericBuilder::new(ii.reader())
        .range_min(1.0)
        .range_max(10.0)
        .build();
    assert_eq!(it.range_min(), 1.0);
    assert_eq!(it.range_max(), 10.0);

    // Default range values when not explicitly set.
    let it = NumericBuilder::new(ii.reader()).build();
    assert_eq!(it.range_min(), f64::NEG_INFINITY);
    assert_eq!(it.range_max(), f64::INFINITY);
}

/// Test that read correctly skips remaining duplicates after skip_to lands
/// on a doc with multiple entries in a multi-value index.
#[test]
fn skip_to_then_read_with_duplicates() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    // Add multiple entries with the same docId (triggers HasMultiValue flag).
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(10.0).doc_id(5).build());

    let context = MockContext::new(0, 0);
    let mut it = NumericBuilder::new(ii.reader())
        .range_tree(context.numeric_range_tree())
        .build();

    // Skip to doc 1 — should find it.
    let res = it.skip_to(1).expect("skip_to failed");
    let Some(SkipToOutcome::Found(record)) = res else {
        panic!("expected Found for doc 1, got {res:?}");
    };
    assert_eq!(record.doc_id, 1);

    // Read should skip the remaining duplicate entries for doc 1 and return doc 5.
    let record = it.read().expect("read failed").expect("expected a result");
    assert_eq!(record.doc_id, 5);

    // No more docs.
    assert_eq!(it.read().expect("read failed"), None);
    assert!(it.at_eof());
}

/// Test the `reader()` accessor on the Numeric iterator.
#[test]
fn numeric_reader_accessor() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(3).build());

    let context = MockContext::new(0, 0);
    let it = NumericBuilder::new(ii.reader())
        .range_tree(context.numeric_range_tree())
        .build();

    // Verify the reader is accessible and reports correct unique doc count.
    assert_eq!(it.reader().unique_docs(), 2);
}

/// Test `should_abort` returns false when no range tree is provided.
#[test]
fn numeric_no_range_tree_revalidate() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
    let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(3).build());

    // Build without a range tree — should_abort will return false.
    let mut it = NumericBuilder::new(ii.reader()).build();

    // Read one doc to advance the iterator.
    let record = it.read().expect("read failed").expect("expected a result");
    assert_eq!(record.doc_id, 1);

    // Revalidate should succeed (not abort) even though there is no range tree.
    // SAFETY: test-only call with valid context
    assert_eq!(
        unsafe { it.revalidate(mock_ctx.spec()) }.expect("revalidate failed"),
        RQEValidateStatus::Ok
    );
}

/// A [`GeoFilter`] with a non-null address so `is_numeric_filter()` returns `false`.
/// `fieldSpec` and `numericFilters` are null — tests using this stub must not
/// reach code paths that dereference those pointers.
pub fn geo_filter_stub() -> GeoFilter {
    GeoFilter {
        fieldSpec: ptr::null(),
        lat: 0.0,
        lon: 0.0,
        radius: 1.0,
        unitType: GeoDistance_GEO_DISTANCE_M,
        numericFilters: ptr::null_mut(),
    }
}

mod from_tree {
    use ffi::t_docId;
    use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
    use inverted_index::NumericFilter;
    use numeric_range_tree::NumericRangeTree;
    use rqe_iterators::{NumericIteratorVariant, RQEIterator, RQEValidateStatus};
    use rqe_iterators_test_utils::MockContext;

    fn make_field_ctx() -> FieldFilterContext {
        FieldFilterContext {
            field: FieldMaskOrIndex::Index(0),
            predicate: FieldExpirationPredicate::Default,
        }
    }

    fn passthrough_filter() -> NumericFilter {
        NumericFilter {
            min: f64::NEG_INFINITY,
            max: f64::INFINITY,
            min_inclusive: true,
            max_inclusive: true,
            ..Default::default()
        }
    }

    fn build_tree(entries: &[(t_docId, f64)]) -> NumericRangeTree {
        let mut tree = NumericRangeTree::new(false);
        for (doc_id, value) in entries {
            tree.add(*doc_id, *value, false, 0);
        }
        tree
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri does not support #[should_panic]")]
    #[should_panic(expected = "Numeric queries require a field index, not a field mask")]
    fn mask_field_panics() {
        let tree = build_tree(&[(1, 1.0)]);
        let ctx = MockContext::new(0, 0);
        let filter = passthrough_filter();
        let field_ctx = FieldFilterContext {
            field: FieldMaskOrIndex::Mask(1),
            predicate: FieldExpirationPredicate::Default,
        };
        // SAFETY: panics before any safety-relevant pointer is touched.
        unsafe { NumericIteratorVariant::from_tree(&tree, ctx.sctx(), &filter, &field_ctx) };
    }

    #[test]
    fn empty_when_no_ranges_match() {
        let tree = build_tree(&[(1, 1.0), (2, 3.0), (3, 5.0)]);
        let ctx = MockContext::new(0, 0);
        let filter = NumericFilter {
            min: -100.0,
            max: -1.0,
            min_inclusive: true,
            max_inclusive: true,
            ..Default::default()
        };
        let field_ctx = make_field_ctx();

        // SAFETY: `ctx` keeps sctx/spec alive past `iters`; field is Index.
        let iters =
            unsafe { NumericIteratorVariant::from_tree(&tree, ctx.sctx(), &filter, &field_ctx) };

        assert!(
            iters.is_empty(),
            "expected no matching ranges, got {}",
            iters.len()
        );
    }

    #[test]
    fn unfiltered_when_range_contained_in_filter_bounds() {
        let tree = build_tree(&[(1, 1.0), (2, 3.0), (3, 5.0)]);
        let ctx = MockContext::new(0, 0);
        let filter = passthrough_filter();
        let field_ctx = make_field_ctx();

        // SAFETY: `ctx` keeps sctx/spec alive past `iters`; field is Index.
        let iters =
            unsafe { NumericIteratorVariant::from_tree(&tree, ctx.sctx(), &filter, &field_ctx) };

        assert!(!iters.is_empty(), "expected at least one iterator");
        for iter in &iters {
            assert!(
                matches!(iter, NumericIteratorVariant::Unfiltered(_)),
                "expected Unfiltered variant, range is fully inside filter bounds"
            );
        }
    }

    #[test]
    fn filtered_when_range_partially_overlaps_filter_bounds() {
        // Range [1, 15] extends outside the filter [5, 10], so per-record checks are needed.
        let tree = build_tree(&[(1, 1.0), (2, 8.0), (3, 15.0)]);
        let ctx = MockContext::new(0, 0);
        let filter = NumericFilter {
            min: 5.0,
            max: 10.0,
            min_inclusive: true,
            max_inclusive: true,
            ..Default::default()
        };
        let field_ctx = make_field_ctx();

        // SAFETY: `ctx` keeps sctx/spec alive past `iters`; field is Index.
        let iters =
            unsafe { NumericIteratorVariant::from_tree(&tree, ctx.sctx(), &filter, &field_ctx) };

        assert!(!iters.is_empty(), "expected at least one iterator");
        for iter in &iters {
            assert!(
                matches!(iter, NumericIteratorVariant::Filtered(_)),
                "expected Filtered variant for partially-overlapping range"
            );
        }
    }

    #[test]
    fn geo_variant_for_geo_filter() {
        let tree = build_tree(&[(1, 1.0)]);
        let ctx = MockContext::new(0, 0);
        let geo_filter = super::geo_filter_stub();
        let filter = NumericFilter {
            geo_filter: &geo_filter as *const _ as *const _,
            min: f64::NEG_INFINITY,
            max: f64::INFINITY,
            min_inclusive: true,
            max_inclusive: true,
            ..Default::default()
        };
        let field_ctx = make_field_ctx();

        // SAFETY: `ctx` keeps sctx/spec alive past `iters`; field is Index.
        // `geo_filter` is stack-allocated and outlives `filter`.
        let iters =
            unsafe { NumericIteratorVariant::from_tree(&tree, ctx.sctx(), &filter, &field_ctx) };

        assert!(!iters.is_empty(), "expected at least one iterator");
        for iter in &iters {
            assert!(
                matches!(iter, NumericIteratorVariant::Geo(_)),
                "expected Geo variant for geo filter"
            );
        }
    }

    #[test]
    fn can_read_all_documents() {
        let tree = build_tree(&[(1, 1.0), (3, 3.0), (5, 5.0)]);
        let ctx = MockContext::new(0, 0);
        let filter = passthrough_filter();
        let field_ctx = make_field_ctx();

        // SAFETY: `ctx` keeps sctx/spec alive past `iters`; field is Index.
        let mut iters =
            unsafe { NumericIteratorVariant::from_tree(&tree, ctx.sctx(), &filter, &field_ctx) };

        assert_eq!(
            iters.len(),
            1,
            "expected exactly one range for a single-leaf tree"
        );
        let it = &mut iters[0];

        let mut doc_ids = Vec::new();
        while let Some(record) = it.read().expect("read failed") {
            doc_ids.push(record.doc_id);
        }
        assert_eq!(doc_ids, vec![1, 3, 5]);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the stored NonNull is derived from a `&T` (SharedReadOnly tag); the subsequent \
                  mutable reborrow to call increment_revision pops that tag under Stacked Borrows, \
                  so reading through the NonNull during revalidation is flagged as UB. The \
                  aliasing is intentional: the revision_id detects tree invalidation so the \
                  iterator can abort before touching stale cursor data, but Stacked Borrows \
                  cannot model that invariant."
    )]
    fn non_null_field_spec_enables_revalidation() {
        let tree_ptr: *mut NumericRangeTree =
            Box::into_raw(Box::new(build_tree(&[(1, 1.0), (2, 2.0)])));
        let ctx = MockContext::new(0, 0);
        // Any non-null pointer makes from_tree store the tree for revalidation.
        // SAFETY: `FieldSpec` is a plain C struct (generated by bindgen) with no
        // Rust-level non-zero validity requirements; a zero bit pattern is valid.
        // `from_tree` only checks the `field_spec` pointer for null and never dereferences it,
        // so the zeroed field values are never observed.
        let dummy_fs: ffi::FieldSpec = unsafe { std::mem::zeroed() };
        let filter = NumericFilter {
            field_spec: &dummy_fs,
            min: f64::NEG_INFINITY,
            max: f64::INFINITY,
            min_inclusive: true,
            max_inclusive: true,
            ..Default::default()
        };
        let field_ctx = make_field_ctx();

        // SAFETY: `tree_ptr` and `ctx` both outlive `iters`; field is Index.
        let mut iters = unsafe {
            NumericIteratorVariant::from_tree(
                &tree_ptr.as_ref().unwrap(),
                ctx.sctx(),
                &filter,
                &field_ctx,
            )
        };
        assert!(!iters.is_empty());

        let _ = iters[0].read().expect("initial read failed");

        // SAFETY: iterators store a NonNull (no live `&` to the tree), so this
        // write does not violate aliasing rules.
        unsafe { (*tree_ptr).increment_revision() };

        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { iters[0].revalidate(ctx.spec()) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted,
        );
        // SAFETY: `tree_ptr` was created by `Box::into_raw` above; `iters` is dropped
        // before this point and holds only a `NonNull` (not ownership), so no double-free.
        unsafe { drop(Box::from_raw(tree_ptr)) };
    }

    #[test]
    fn null_field_spec_disables_revalidation() {
        let tree_ptr: *mut NumericRangeTree =
            Box::into_raw(Box::new(build_tree(&[(1, 1.0), (2, 2.0)])));
        let ctx = MockContext::new(0, 0);
        // passthrough_filter() has field_spec = null → no tree snapshot taken.
        let filter = passthrough_filter();
        let field_ctx = make_field_ctx();

        // SAFETY: `tree_ptr` and `ctx` both outlive `iters`; field is Index.
        let mut iters = unsafe {
            NumericIteratorVariant::from_tree(
                tree_ptr.as_ref().unwrap(),
                ctx.sctx(),
                &filter,
                &field_ctx,
            )
        };
        assert!(!iters.is_empty());

        let _ = iters[0].read().expect("initial read failed");

        // SAFETY: iterators store a NonNull (no live `&` to the tree), so this
        // write does not violate aliasing rules.
        unsafe { (*tree_ptr).increment_revision() };

        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { iters[0].revalidate(ctx.spec()) }.expect("revalidate failed"),
            RQEValidateStatus::Ok,
        );

        // SAFETY: `tree_ptr` was created by `Box::into_raw` above; `iters` is dropped
        // before this point and holds only a `NonNull` (not ownership), so no double-free.
        unsafe { drop(Box::from_raw(tree_ptr)) };
    }
}

/// Tests for [`rqe_iterators::NumericIteratorVariant`] variant selection logic.
///
/// These tests verify that [`NumericIteratorVariant::new`] selects the correct
/// concrete reader variant based on the provided filter:
/// - `None` → [`NumericIteratorVariant::Unfiltered`]
/// - `Some(f)` where `f.is_numeric_filter()` → [`NumericIteratorVariant::Filtered`]
/// - `Some(f)` where `!f.is_numeric_filter()` → [`NumericIteratorVariant::Geo`]
mod variant {
    use ffi::IndexFlags_Index_StoreNumeric;
    use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
    use inverted_index::{NumericFilter, RSIndexResult};
    use numeric_range_tree::NumericIndex;
    use rqe_iterators::{FieldExpirationChecker, NumericIteratorVariant};
    use rqe_iterators_test_utils::MockContext;

    fn make_expiration_checker(ctx: &MockContext) -> FieldExpirationChecker {
        // SAFETY:
        // - `ctx.sctx()` is a valid `RedisSearchCtx` pointer for the duration of this test.
        // - `ctx.sctx().spec` is set to a valid `IndexSpec` pointer by `MockContext::new`.
        // Both remain alive for the lifetime of the returned checker.
        unsafe {
            FieldExpirationChecker::new(
                ctx.sctx(),
                FieldFilterContext {
                    field: FieldMaskOrIndex::Index(0),
                    predicate: FieldExpirationPredicate::Default,
                },
                0,
            )
        }
    }

    /// Build a minimal `NumericIndex` with a single record so the reader is non-trivial.
    fn make_index() -> NumericIndex {
        let mut idx = NumericIndex::new(false);
        idx.add_record(&RSIndexResult::build_numeric(1.0).doc_id(1).build());
        idx
    }

    #[test]
    /// `None` filter → `Unfiltered` variant; accessors reflect construction parameters.
    fn variant_unfiltered() {
        let ctx = MockContext::new(0, 0);
        let idx = make_index();
        let variant = NumericIteratorVariant::new(
            idx.reader(),
            None,
            make_expiration_checker(&ctx),
            None,
            1.0,
            5.0,
        );
        assert!(
            matches!(variant, NumericIteratorVariant::Unfiltered(_)),
            "expected Unfiltered variant for None filter"
        );
        assert_eq!(variant.range_min(), 1.0);
        assert_eq!(variant.range_max(), 5.0);
        assert_eq!(variant.flags(), IndexFlags_Index_StoreNumeric);
    }

    #[test]
    /// Numeric filter (null `geo_filter`) → `Filtered` variant; accessors reflect construction parameters.
    fn variant_filtered() {
        let ctx = MockContext::new(0, 0);
        let idx = make_index();
        // Default NumericFilter has geo_filter = null, so is_numeric_filter() == true.
        let filter = NumericFilter {
            min: 0.0,
            max: 10.0,
            ..Default::default()
        };
        let variant = NumericIteratorVariant::new(
            idx.reader(),
            Some(&filter),
            make_expiration_checker(&ctx),
            None,
            1.0,
            5.0,
        );
        assert!(
            matches!(variant, NumericIteratorVariant::Filtered(_)),
            "expected Filtered variant for numeric filter"
        );
        assert_eq!(variant.range_min(), 1.0);
        assert_eq!(variant.range_max(), 5.0);
        assert_eq!(variant.flags(), IndexFlags_Index_StoreNumeric);
    }

    #[test]
    /// Non-null `geo_filter` → `Geo` variant; accessors reflect construction parameters.
    fn variant_geo() {
        let ctx = MockContext::new(0, 0);
        let idx = make_index();
        let geo_filter = super::geo_filter_stub();
        // A non-null geo_filter pointer makes is_numeric_filter() return false.
        let filter = NumericFilter {
            geo_filter: &geo_filter as *const _ as *const _,
            ..Default::default()
        };
        let variant = NumericIteratorVariant::new(
            idx.reader(),
            Some(&filter),
            make_expiration_checker(&ctx),
            None,
            1.0,
            5.0,
        );
        assert!(
            matches!(variant, NumericIteratorVariant::Geo(_)),
            "expected Geo variant for geo filter"
        );
        assert_eq!(variant.range_min(), 1.0);
        assert_eq!(variant.range_max(), 5.0);
        assert_eq!(variant.flags(), IndexFlags_Index_StoreNumeric);
    }
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{
        ExpirationTest, MockExpirationChecker, RevalidateIndexType, RevalidateTest,
    };
    use numeric_range_tree::NumericIndexReader;
    use rqe_iterators::RQEValidateStatus;

    struct NumericExpirationTest {
        test: ExpirationTest,
    }

    impl NumericExpirationTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            // The numeric record has a value of `doc_id * 2.0`.
            RSIndexResult::build_numeric(doc_id as f64 * 2.0)
                .doc_id(doc_id)
                .build()
        }

        fn new(n_docs: u64, multi: bool) -> Self {
            Self {
                test: ExpirationTest::numeric(Box::new(Self::expected_record), n_docs, multi),
            }
        }

        fn create_iterator(&self) -> Numeric<'_, NumericIndexReader<'_>, MockExpirationChecker> {
            let reader = self.test.numeric_inverted_index().reader();
            let checker = self.test.create_mock_checker();

            // SAFETY: `numeric_range_tree()` returns a valid pointer that
            // outlives the returned iterator.
            unsafe {
                Numeric::new(
                    reader,
                    checker,
                    Some(self.test.context.numeric_range_tree_ref()),
                    None,
                    None,
                )
            }
        }

        fn test_read_expiration(&mut self) {
            let field_index = self.test.context.field_spec().index;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, field::FieldMaskOrIndex::Index(field_index));

            let mut it = self.create_iterator();
            self.test.read(&mut it);
        }

        fn test_skip_to_expiration(&mut self) {
            let field_index = self.test.context.field_spec().index;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, field::FieldMaskOrIndex::Index(field_index));

            let mut it = self.create_iterator();
            self.test.skip_to(&mut it);
        }
    }

    #[test]
    fn numeric_read_expiration() {
        NumericExpirationTest::new(10, false).test_read_expiration();
    }

    #[test]
    fn numeric_read_skip_multi_expiration() {
        NumericExpirationTest::new(10, true).test_read_expiration();
    }

    #[test]
    fn numeric_skip_to_expiration() {
        NumericExpirationTest::new(10, false).test_skip_to_expiration();
    }

    #[test]
    fn numeric_skip_to_expiration_multi() {
        NumericExpirationTest::new(10, true).test_skip_to_expiration();
    }

    /// Test that skip_to on a non-existent doc ID where the next doc found is
    /// NOT expired returns NotFound via the `skip_to_check_expiration` path.
    /// Exercises the NotFound branch when the seeked doc is not expired.
    #[test]
    fn numeric_skip_to_non_existent_with_expiration() {
        use crate::inverted_index::utils::MockExpirationChecker;
        use std::collections::HashSet;

        // Create docs with IDs 1, 3, 5, 7 (gaps at 2, 4, 6).
        let mut ii =
            InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
        let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(1).build());
        let _ = ii.add_record(&RSIndexResult::build_numeric(6.0).doc_id(3).build());
        let _ = ii.add_record(&RSIndexResult::build_numeric(10.0).doc_id(5).build());
        let _ = ii.add_record(&RSIndexResult::build_numeric(14.0).doc_id(7).build());

        // Mark doc 1 as expired
        let mut expired_docs = HashSet::new();
        expired_docs.insert(1);
        let checker = MockExpirationChecker::new(expired_docs);

        let context = MockContext::new(0, 0);
        let mut it = NumericBuilder::new(ii.reader())
            .range_tree(context.numeric_range_tree())
            .expiration_checker(checker)
            .build();

        // Skip to doc 2, which doesn't exist. The seeker finds doc 3
        // (the next available), which is NOT expired.
        // This exercises skip_to_check_expiration's NotFound branch for non-expired docs.
        let res = it.skip_to(2).expect("skip_to failed");
        let Some(SkipToOutcome::NotFound(record)) = res else {
            panic!("expected NotFound for doc 2, got {res:?}");
        };
        assert_eq!(record.doc_id, 3);
        assert_eq!(it.last_doc_id(), 3);
    }

    /// Test that `has_expiration` returns false when using an empty expiration checker.
    /// This simulates the case where expiration checking is disabled.
    #[test]
    fn numeric_no_expiration_with_invalid_field_index() {
        use crate::inverted_index::utils::MockExpirationChecker;
        use std::collections::HashSet;

        // Create docs with IDs 1, 2, 3.
        let mut ii =
            InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
        let _ = ii.add_record(&RSIndexResult::build_numeric(2.0).doc_id(1).build());
        let _ = ii.add_record(&RSIndexResult::build_numeric(4.0).doc_id(2).build());
        let _ = ii.add_record(&RSIndexResult::build_numeric(6.0).doc_id(3).build());

        // Use an empty MockExpirationChecker (has_expiration returns false)
        // to simulate RS_INVALID_FIELD_INDEX behavior.
        let checker = MockExpirationChecker::new(HashSet::new());

        let context = MockContext::new(0, 0);
        let mut it = NumericBuilder::new(ii.reader())
            .range_tree(context.numeric_range_tree())
            .expiration_checker(checker)
            .build();

        // Since expiration checking is disabled (has_expiration returns false),
        // we should see all docs including doc 1.
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 1);
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 2);
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 3);
        assert_eq!(it.read().expect("read failed"), None);
    }

    /// Test that revalidation with `last_doc_id == 0` returns Ok even when
    /// the underlying index has been modified (needs_revalidation is true).
    /// Exercises the `last_doc_id == 0` early return in `revalidate`.
    #[test]
    fn numeric_revalidate_needs_revalidation_before_reads() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = test.test.context.numeric_inverted_index();

        // Trigger GC on the index so needs_revalidation() returns true.
        test.test.remove_document_numeric(ii, 1);

        // Revalidate before any reads. last_doc_id is 0, so even though
        // needs_revalidation is true, we should get Ok.
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(test.test.context.spec) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // The iterator should still work — doc 1 was removed, so first doc is 3.
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 3);
    }

    struct NumericRevalidateTest {
        test: RevalidateTest,
    }

    impl NumericRevalidateTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            // The numeric record has a value of `doc_id * 2.0`.
            RSIndexResult::build_numeric(doc_id as f64 * 2.0)
                .doc_id(doc_id)
                .build()
        }

        fn new(n_docs: u64) -> Self {
            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Numeric,
                    Box::new(Self::expected_record),
                    n_docs,
                ),
            }
        }

        fn create_iterator(&self) -> Numeric<'_, NumericIndexReader<'_>, NoOpChecker> {
            let ii = self.test.context.numeric_inverted_index();
            let context = &self.test.context;

            NumericBuilder::new(ii.reader())
                .range_tree(context.numeric_range_tree())
                .build()
        }
    }

    #[test]
    fn numeric_revalidate_basic() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn numeric_revalidate_at_eof() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn numeric_revalidate_after_index_disappears() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let sctx = test.test.context.spec;

        // First, verify the iterator works normally and read at least one document
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(it.read().expect("failed to read").is_some());
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // For numeric iterators, we can simulate index disappearance by
        // manipulating the revision ID. check_abort() compares the stored
        // revision ID with the current one from the NumericRangeTree.
        let context = &test.test.context;
        // Simulate the range tree being modified by incrementing its revision ID
        // This simulates a scenario where the tree was modified (e.g., node split, removal)
        // while the iterator was suspended.
        {
            let rt = context.numeric_range_tree_mut();
            rt.increment_revision();
        }

        // Now Revalidate should return Aborted because the revision IDs don't match
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );
    }

    #[test]
    fn numeric_revalidate_after_document_deleted() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = test.test.context.numeric_inverted_index();

        test.test
            .revalidate_numeric_after_document_deleted(&mut it, ii);
    }
}
