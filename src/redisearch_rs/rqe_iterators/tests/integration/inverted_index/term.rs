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
use field::FieldMaskOrIndex;
use inverted_index::{FilterMaskReader, RSIndexResult, RSOffsetSlice, full::Full};
use query_term::RSQueryTerm;
use rqe_iterators::{NoOpChecker, RQEIterator, inverted_index::Term};

use crate::inverted_index::utils::{BaseTest, RevalidateIndexType, RevalidateTest};

fn new_term() -> Box<RSQueryTerm> {
    let mut term = RSQueryTerm::new(b"term", 1, 0);
    term.set_idf(5.0);
    term.set_bm25_idf(10.0);
    term
}

fn expected_record(
    doc_id: t_docId,
    field_mask: t_fieldMask,
    term: Option<Box<query_term::RSQueryTerm>>,
    offsets: &'static [u8],
) -> RSIndexResult<'static> {
    RSIndexResult::with_term(
        term,
        RSOffsetSlice::from_slice(offsets),
        doc_id,
        field_mask,
        (doc_id / 2) as u32 + 1,
    )
}

struct TermBaseTest {
    test: BaseTest<Full>,
}

impl TermBaseTest {
    fn new(n_docs: u64) -> Self {
        let flags = IndexFlags_Index_StoreFreqs
            | IndexFlags_Index_StoreTermOffsets
            | IndexFlags_Index_StoreFieldFlags
            | IndexFlags_Index_StoreByteOffsets;

        const OFFSETS: &'static [u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        Self {
            test: BaseTest::new(
                flags,
                Box::new(move |doc_id| {
                    let mut term = RSQueryTerm::new(b"term", 1, 0);
                    term.set_idf(5.0);
                    term.set_bm25_idf(10.0);
                    // Use doc_id as field_mask so we can test FilterMaskReader
                    expected_record(doc_id, doc_id as t_fieldMask, Some(term), OFFSETS)
                }),
                n_docs,
            ),
        }
    }

    fn create_iterator(
        &self,
    ) -> Term<'_, inverted_index::IndexReaderCore<'_, inverted_index::full::Full>, NoOpChecker>
    {
        let reader = self.test.ii.reader();

        unsafe {
            Term::new(
                reader,
                self.test.mock_ctx.sctx(),
                new_term(),
                1.0,
                NoOpChecker,
            )
        }
    }
}

#[test]
/// test reading from Term iterator
fn term_read() {
    let test = TermBaseTest::new(100);
    let mut it = test.create_iterator();

    // Read the first record and verify the term and weight are correct.
    let record = it.read().unwrap().expect("expected at least one record");
    assert_eq!(record.weight, 1.0);
    let term = record.as_term().expect("expected term record").query_term();
    let expected = new_term();
    assert_eq!(term, Some(&*expected));

    it.rewind();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Term iterator
fn term_skip_to() {
    let test = TermBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Term iterator with a filter
fn term_filter() {
    let test = TermBaseTest::new(10);
    let reader = FilterMaskReader::new(1, test.test.ii.reader());
    let mut it = unsafe {
        Term::new(
            reader,
            test.test.mock_ctx.sctx(),
            new_term(),
            1.0,
            NoOpChecker,
        )
    };
    // results have their doc id as field mask so we filter by odd ids
    let docs_ids = test.test.docs_ids_iter().filter(|id| id % 2 == 1);
    test.test.read(&mut it, docs_ids);
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{ExpirationTest, MockExpirationChecker};
    use rqe_iterators::{RQEIterator, RQEValidateStatus};

    struct TermExpirationTest {
        test: ExpirationTest,
    }

    impl TermExpirationTest {
        fn with_flags(flags: ffi::IndexFlags, n_docs: u64, multi: bool) -> Self {
            // Offsets are delta-varint-encoded when written via ForwardIndexEntry.
            // Writing values 0, 1, 2, 3... results in stored deltas 0, 1, 1, 1...
            const OFFSETS: &'static [u8] = &[0, 1, 1, 1, 1, 1, 1, 1, 1, 1];

            Self {
                test: ExpirationTest::term(
                    flags,
                    Box::new(move |doc_id| {
                        let mut term = RSQueryTerm::new(b"term", 1, 0);
                        term.set_idf(5.0);
                        term.set_bm25_idf(10.0);
                        // Use a field mask with all bits set so all docs match the filter
                        // and expiration is actually tested (not just field mask filtering).
                        // Use u32::MAX for non-wide tests to avoid overflow in the encoder.
                        expected_record(doc_id, u32::MAX as t_fieldMask, Some(term), OFFSETS)
                    }),
                    n_docs,
                    multi,
                ),
            }
        }

        fn new(n_docs: u64, multi: bool) -> Self {
            let flags = IndexFlags_Index_StoreFreqs
                | IndexFlags_Index_StoreTermOffsets
                | IndexFlags_Index_StoreFieldFlags
                | IndexFlags_Index_StoreByteOffsets;
            Self::with_flags(flags, n_docs, multi)
        }

        fn new_wide(n_docs: u64, multi: bool) -> Self {
            let flags = IndexFlags_Index_StoreFreqs
                | IndexFlags_Index_StoreTermOffsets
                | IndexFlags_Index_StoreFieldFlags
                | IndexFlags_Index_StoreByteOffsets
                | IndexFlags_Index_WideSchema;
            Self::with_flags(flags, n_docs, multi)
        }

        fn create_iterator(
            &self,
        ) -> Term<
            '_,
            inverted_index::FilterMaskReader<
                inverted_index::IndexReaderCore<'_, inverted_index::full::Full>,
            >,
            MockExpirationChecker,
        > {
            let field_mask = self.test.text_field_bit();
            let reader = self.test.term_inverted_index().reader(field_mask);
            let checker = self.test.create_mock_checker();
            unsafe { Term::new(reader, self.test.context.sctx, new_term(), 1.0, checker) }
        }

        fn create_iterator_wide(
            &self,
        ) -> Term<
            '_,
            inverted_index::FilterMaskReader<
                inverted_index::IndexReaderCore<'_, inverted_index::full::FullWide>,
            >,
            MockExpirationChecker,
        > {
            let field_mask = self.test.text_field_bit();
            let reader = self.test.term_inverted_index_wide().reader(field_mask);
            let checker = self.test.create_mock_checker();
            unsafe { Term::new(reader, self.test.context.sctx, new_term(), 1.0, checker) }
        }

        fn mark_even_ids_expired(&mut self) {
            let field_mask = self.test.text_field_bit();
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, FieldMaskOrIndex::Mask(field_mask));
        }

        fn test_read_expiration(&mut self) {
            self.mark_even_ids_expired();
            let mut it = self.create_iterator();
            self.test.read(&mut it);
        }

        fn test_read_expiration_wide(&mut self) {
            self.mark_even_ids_expired();
            let mut it = self.create_iterator_wide();
            self.test.read(&mut it);
        }

        fn test_skip_to_expiration(&mut self) {
            self.mark_even_ids_expired();
            let mut it = self.create_iterator();
            self.test.skip_to(&mut it);
        }
    }

    #[test]
    fn term_read_expiration() {
        TermExpirationTest::new(100, false).test_read_expiration();
    }

    #[test]
    fn term_read_expiration_wide() {
        TermExpirationTest::new_wide(100, false).test_read_expiration_wide();
    }

    #[test]
    fn term_read_skip_multi_expiration() {
        TermExpirationTest::new(100, true).test_read_expiration();
    }

    #[test]
    fn term_skip_to_expiration() {
        TermExpirationTest::new(100, false).test_skip_to_expiration();
    }

    struct TermRevalidateTest {
        test: RevalidateTest,
    }

    impl TermRevalidateTest {
        fn new(n_docs: u64) -> Self {
            // Offsets are delta-varint-encoded when written via ForwardIndexEntry.
            // Writing values 0, 1, 2, 3... results in stored deltas 0, 1, 1, 1...
            const OFFSETS: &'static [u8] = &[0, 1, 1, 1, 1, 1, 1, 1, 1, 1];

            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Term,
                    Box::new(move |doc_id| {
                        let mut term = RSQueryTerm::new(b"term", 1, 0);
                        term.set_idf(5.0);
                        term.set_bm25_idf(10.0);
                        // Use a field mask with all bits set so all docs match the filter.
                        expected_record(doc_id, u32::MAX as t_fieldMask, Some(term), OFFSETS)
                    }),
                    n_docs,
                ),
            }
        }

        fn create_iterator(
            &self,
        ) -> Term<
            '_,
            inverted_index::FilterMaskReader<
                inverted_index::IndexReaderCore<'_, inverted_index::full::Full>,
            >,
        > {
            let field_mask = self.test.context.text_field_bit();
            let reader = self.test.context.term_inverted_index().reader(field_mask);
            unsafe { Term::new(reader, self.test.context.sctx, new_term(), 1.0, NoOpChecker) }
        }
    }

    #[test]
    fn term_revalidate_basic() {
        let test = TermRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn term_revalidate_at_eof() {
        let test = TermRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn term_revalidate_after_index_disappears() {
        let test = TermRevalidateTest::new(10);
        let mut it = test.create_iterator();

        // First, verify the iterator works normally and read at least one document
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(it.read().expect("failed to read").is_some());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // TODO: test once check_abort() is implemented
    }

    #[test]
    #[ignore] //TODO
    fn term_revalidate_after_document_deleted() {
        // TODO: Implement once FieldMaskTrackingIndex exposes mutable access to inner index
        // for document deletion in tests.
        todo!()
    }
}
