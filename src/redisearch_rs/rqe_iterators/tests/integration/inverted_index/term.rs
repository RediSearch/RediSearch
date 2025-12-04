/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{
    IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets, IndexFlags_Index_WideSchema,
    t_docId, t_fieldMask,
};
use field::{FieldExpirationPredicate, FieldMaskOrIndex};
use inverted_index::{FilterMaskReader, RSIndexResult, RSOffsetVector, full::Full};
use rqe_iterators::inverted_index::Term;

use crate::{
    ffi::query_term::QueryTermBuilder,
    inverted_index::utils::{BaseTest, ExpirationTest, RevalidateTest},
};

struct TermTest {
    test: BaseTest<Full>,
    revalidate_test: RevalidateTest<Full>,
    #[cfg(not(miri))]
    expiration_test: ExpirationTest<Full>,
    #[cfg(not(miri))]
    expiration_test_multi: ExpirationTest<Full>,
    #[cfg(not(miri))]
    expiration_test_wide_mask: ExpirationTest<Full>,
}

enum ReadMode {
    Normal,
    Multi,
    Wide,
}

impl TermTest {
    // # Safety
    // The returned RSIndexResult contains raw pointers to `term` and `offsets`.
    // These pointers are valid for 'static because the data is moved into the closure
    // in `new()` and lives for the entire duration of the test. The raw pointers are
    // only used within the test's lifetime, making this safe despite the 'static claim.
    fn expected_record(
        doc_id: t_docId,
        term: *mut ffi::RSQueryTerm,
        offsets: &Vec<u8>,
    ) -> RSIndexResult<'static> {
        RSIndexResult::term_with_term_ptr(
            term,
            RSOffsetVector::with_data(offsets.as_ptr() as _, offsets.len() as _),
            doc_id,
            doc_id as t_fieldMask,
            (doc_id / 2) as u32 + 1,
        )
    }

    fn new(n_docs: u64) -> Self {
        let flags = IndexFlags_Index_StoreFreqs
            | IndexFlags_Index_StoreTermOffsets
            | IndexFlags_Index_StoreFieldFlags
            | IndexFlags_Index_StoreByteOffsets;

        const TEST_STR: &str = "term";

        let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let offsets_clone = offsets.clone();
        #[cfg(not(miri))]
        let offsets_clone2 = offsets.clone();
        #[cfg(not(miri))]
        let offsets_clone3 = offsets.clone();
        #[cfg(not(miri))]
        let offsets_clone4 = offsets.clone();

        Self {
            test: BaseTest::new(
                flags,
                Box::new(move |doc_id| {
                    let term = QueryTermBuilder {
                        token: TEST_STR,
                        idf: 5.0,
                        id: 1,
                        flags: 0,
                        bm25_idf: 10.0,
                    }
                    .allocate();
                    Self::expected_record(doc_id, term, &offsets)
                }),
                n_docs,
            ),
            revalidate_test: RevalidateTest::new(
                IndexFlags_Index_StoreTermOffsets,
                Box::new(move |doc_id| {
                    let term2 = QueryTermBuilder {
                        token: TEST_STR,
                        idf: 5.0,
                        id: 1,
                        flags: 0,
                        bm25_idf: 10.0,
                    }
                    .allocate();
                    Self::expected_record(doc_id, term2, &offsets_clone)
                }),
                n_docs,
            ),
            #[cfg(not(miri))]
            expiration_test: ExpirationTest::new(
                IndexFlags_Index_StoreTermOffsets,
                Box::new(move |doc_id| {
                    let term = QueryTermBuilder {
                        token: TEST_STR,
                        idf: 5.0,
                        id: 1,
                        flags: 0,
                        bm25_idf: 10.0,
                    }
                    .allocate();
                    Self::expected_record(doc_id, term, &offsets_clone2)
                }),
                n_docs,
                false,
            ),
            #[cfg(not(miri))]
            expiration_test_multi: ExpirationTest::new(
                IndexFlags_Index_StoreTermOffsets,
                Box::new(move |doc_id| {
                    let term = QueryTermBuilder {
                        token: TEST_STR,
                        idf: 5.0,
                        id: 1,
                        flags: 0,
                        bm25_idf: 10.0,
                    }
                    .allocate();
                    Self::expected_record(doc_id, term, &offsets_clone3)
                }),
                n_docs,
                true,
            ),
            #[cfg(not(miri))]
            expiration_test_wide_mask: ExpirationTest::new(
                IndexFlags_Index_StoreTermOffsets | IndexFlags_Index_WideSchema,
                Box::new(move |doc_id| {
                    let term = QueryTermBuilder {
                        token: TEST_STR,
                        idf: 5.0,
                        id: 1,
                        flags: 0,
                        bm25_idf: 10.0,
                    }
                    .allocate();
                    Self::expected_record(doc_id, term, &offsets_clone4)
                }),
                n_docs,
                false,
            ),
        }
    }

    #[cfg(not(miri))]
    fn test_read_expiration(&mut self, mode: ReadMode) {
        let test = match mode {
            ReadMode::Normal => &mut self.expiration_test,
            ReadMode::Multi => &mut self.expiration_test_multi,
            ReadMode::Wide => &mut self.expiration_test_wide_mask,
        };

        const FIELD_MASK: t_fieldMask = 42;
        // Make every even document ID field expired
        let even_ids = test
            .doc_ids
            .iter()
            .filter(|id| **id % 2 == 0)
            .copied()
            .collect();

        test.mark_index_expired(even_ids, FieldMaskOrIndex::Mask(FIELD_MASK));

        let reader = test.ii.reader();
        let mut it = Term::with_context(
            reader,
            test.context(),
            FIELD_MASK,
            FieldExpirationPredicate::Default,
        );

        test.read(&mut it);
    }

    #[cfg(not(miri))]
    fn test_skip_to_expiration(&mut self) {
        const FIELD_MASK: t_fieldMask = 42;
        // Make every even document ID field expired
        let even_ids = self
            .expiration_test
            .doc_ids
            .iter()
            .filter(|id| **id % 2 == 0)
            .copied()
            .collect();

        self.expiration_test
            .mark_index_expired(even_ids, FieldMaskOrIndex::Mask(FIELD_MASK));

        let reader = self.expiration_test.ii.reader();
        let mut it = Term::with_context(
            reader,
            self.expiration_test.context(),
            FIELD_MASK,
            FieldExpirationPredicate::Default,
        );

        self.expiration_test.skip_to(&mut it);
    }
}

#[test]
/// test reading from Term iterator
fn term_full_read() {
    let test = TermTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Term::new(reader);
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Term iterator
fn term_full_skip_to() {
    let test = TermTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Term::new(reader);
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Term iterator with a filter
fn term_filter() {
    let test = TermTest::new(10);
    let reader = FilterMaskReader::new(1, test.test.ii.reader());
    let mut it = Term::new(reader);
    // results have their doc id as field mask so we filter by odd ids
    let docs_ids = test.test.docs_ids_iter().filter(|id| id % 2 == 1);
    test.test.read(&mut it, docs_ids);
}

#[test]
fn term_full_revalidate_basic() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader);
    test.revalidate_test.revalidate_basic(&mut it);
}

#[test]
fn term_full_revalidate_at_eof() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader);
    test.revalidate_test.revalidate_at_eof(&mut it);
}

#[test]
fn term_full_revalidate_after_index_disappears() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader);
    test.revalidate_test
        .revalidate_after_index_disappears(&mut it, true);
}

#[test]
fn term_full_revalidate_after_document_deleted() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader);
    test.revalidate_test
        .revalidate_after_document_deleted(&mut it);
}

#[cfg(not(miri))]
#[test]
fn term_read_expiration() {
    TermTest::new(100).test_read_expiration(ReadMode::Normal);
}

#[cfg(not(miri))]
#[test]
fn term_read_expiration_wide() {
    TermTest::new(100).test_read_expiration(ReadMode::Wide);
}

#[cfg(not(miri))]
#[test]
fn term_read_skip_multi_expiration() {
    TermTest::new(100).test_read_expiration(ReadMode::Multi);
}

#[cfg(not(miri))]
#[test]
fn term_skip_to_expiration() {
    TermTest::new(100).test_skip_to_expiration();
}
