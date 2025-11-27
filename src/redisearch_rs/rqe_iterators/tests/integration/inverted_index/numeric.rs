/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use inverted_index::{FilterNumericReader, NumericFilter, RSIndexResult};
use rqe_iterators::inverted_index::Numeric;

use crate::inverted_index::utils::{BaseTest, RevalidateTest};

struct NumericTest {
    test: BaseTest<inverted_index::numeric::Numeric>,
    revalidate_test: RevalidateTest<inverted_index::numeric::Numeric>,
}

impl NumericTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        // The numeric record has a value of `doc_id * 2.0`.
        RSIndexResult::numeric(doc_id as f64 * 2.0).doc_id(doc_id)
    }

    fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_StoreNumeric,
                Box::new(Self::expected_record),
                n_docs,
            ),
            revalidate_test: RevalidateTest::new(
                IndexFlags_Index_StoreNumeric,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }
}

#[test]
/// test reading from NumericFull iterator
fn numeric_full_read() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Numeric::new(reader);
    test.test.read(&mut it, test.test.docs_ids_iter());

    // same but using a passthrough filter
    let test = NumericTest::new(100);
    let filter = NumericFilter::default();
    let reader = test.test.ii.reader();
    let reader = FilterNumericReader::new(&filter, reader);
    let mut it = Numeric::new(reader);
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Numeric iterator
fn numeric_full_skip_to() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Numeric::new(reader);
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Numeric iterator with a filter
fn numeric_filter() {
    let test = NumericTest::new(100);
    let filter = NumericFilter {
        min: 50.0,
        max: 75.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, test.test.ii.reader());
    let mut it = Numeric::new(reader);
    let docs_ids = test
        .test
        .docs_ids_iter()
        // records have a numeric value of twice their doc id
        .filter(|id| *id * 2 >= 50 && *id * 2 <= 75);
    test.test.read(&mut it, docs_ids);
}

#[test]
fn numeric_full_revalidate_basic() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test.revalidate_basic(&mut it);
}

#[test]
fn numeric_full_revalidate_at_eof() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test.revalidate_at_eof(&mut it);
}

#[test]
fn numeric_full_revalidate_after_index_disappears() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test
        .revalidate_after_index_disappears(&mut it, true);
}

#[cfg(not(miri))] // Miri does not like UnsafeCell
#[test]
fn numeric_full_revalidate_after_document_deleted() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test
        .revalidate_after_document_deleted(&mut it);
}
