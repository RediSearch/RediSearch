/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use approx::assert_abs_diff_eq;
use ffi::{
    IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets, IndexFlags_Index_WideSchema,
};
use field::FieldMaskOrIndex;
use index_result::{RSIndexResult, RSOffsetSlice};
use inverted_index::{FilterMaskReader, full::Full};
use query_term::RSQueryTerm;
use rqe_core::{DocId, FieldMask};
use rqe_iterators::{IteratorType, NoOpChecker, RQEIterator, inverted_index::Term};

use crate::inverted_index::utils::{BaseTest, RevalidateIndexType, RevalidateTest};

fn expected_record(
    doc_id: DocId,
    field_mask: FieldMask,
    term: Box<query_term::RSQueryTerm>,
    offsets: &'static [u8],
) -> RSIndexResult<'static> {
    RSIndexResult::build_term()
        .borrowed_record(Some(term), RSOffsetSlice::from_slice(offsets))
        .doc_id(doc_id)
        .field_mask(field_mask)
        .frequency((doc_id / 2) as u32 + 1)
        .build()
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
                    let mut term = RSQueryTerm::new("term", 1, 0);
                    term.set_idf(5.0);
                    term.set_bm25_idf(10.0);
                    // Use doc_id as field_mask so we can test FilterMaskReader
                    expected_record(doc_id, doc_id as FieldMask, term, OFFSETS)
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
                RSQueryTerm::new("term", 1, 0),
                1.0,
                NoOpChecker,
            )
        }
    }
}

#[test]
fn term_type() {
    let test = TermBaseTest::new(10);
    let it = test.create_iterator();
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);
}

#[test]
/// test reading from Term iterator
fn term_read() {
    let test = TermBaseTest::new(100);
    let mut it = test.create_iterator();

    // Read the first record and verify the term, weight, and IDF are correct.
    let record = it.read().unwrap().expect("expected at least one record");
    assert_eq!(record.weight, 1.0);
    let term = record
        .as_term()
        .expect("expected term record")
        .query_term()
        .expect("expected query term");

    // IDF is computed by Term::new() from MockContext's numDocuments (0) and unique_docs (101).
    // calculate_idf(0, 101) = floor(log2(1 + 1/101)) = floor(~0.014) = 0.0
    assert_eq!(term.idf(), 0.0);
    // calculate_idf_bm25(0, 101) — total_docs clamped to term_docs (101).
    assert_abs_diff_eq!(term.bm25_idf(), 0.004914014802429163);

    it.rewind();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Term iterator
fn term_skip_to() {
    let test = TermBaseTest::new(10);
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
            RSQueryTerm::new("term", 1, 0),
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
                        let mut term = RSQueryTerm::new("term", 1, 0);
                        term.set_idf(5.0);
                        term.set_bm25_idf(10.0);
                        // Use a field mask with all bits set so all docs match the filter
                        // and expiration is actually tested (not just field mask filtering).
                        // Use u32::MAX for non-wide tests to avoid overflow in the encoder.
                        expected_record(doc_id, u32::MAX as FieldMask, term, OFFSETS)
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
            unsafe {
                Term::new(
                    reader,
                    self.test.context.sctx,
                    RSQueryTerm::new("term", 1, 0),
                    1.0,
                    checker,
                )
            }
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
            unsafe {
                Term::new(
                    reader,
                    self.test.context.sctx,
                    RSQueryTerm::new("term", 1, 0),
                    1.0,
                    checker,
                )
            }
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
                        let mut term = RSQueryTerm::new("term", 1, 0);
                        term.set_idf(5.0);
                        term.set_bm25_idf(10.0);
                        // Use a field mask with all bits set so all docs match the filter.
                        expected_record(doc_id, u32::MAX as FieldMask, term, OFFSETS)
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
            unsafe {
                Term::new(
                    reader,
                    self.test.context.sctx,
                    RSQueryTerm::new("term", 1, 0),
                    1.0,
                    NoOpChecker,
                )
            }
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
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);
        assert!(it.read().expect("failed to read").is_some());
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);

        // Simulate the term's inverted index being garbage collected and
        // replaced by swapping the reader's stored index pointer to a
        // different (dummy) index. Redis_OpenInvertedIndex will still
        // return the original, so the pointer comparison will fail.
        let flags = test.test.context.term_inverted_index().flags();
        let dummy = Box::leak(Box::new(inverted_index::InvertedIndex::<
            inverted_index::full::Full,
        >::new(flags)));
        let mut dummy_ref: &inverted_index::InvertedIndex<inverted_index::full::Full> = dummy;

        it.swap_index(&mut dummy_ref);

        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);

        // Swap back and free the dummy for proper cleanup.
        it.swap_index(&mut dummy_ref);
        // SAFETY: `dummy_ref` now points back to the leaked dummy allocation.
        drop(unsafe {
            Box::from_raw(
                dummy_ref as *const _
                    as *mut inverted_index::InvertedIndex<inverted_index::full::Full>,
            )
        });
    }

    #[test]
    fn term_revalidate_after_index_gc_collected() {
        let test = TermRevalidateTest::new(10);

        // Build the iterator with a query term that does not exist in keysDict.
        // This simulates the GC having collected the entire inverted index for
        // that term: Redis_OpenInvertedIndex will return null when should_abort
        // tries to look it up.
        let field_mask = test.test.context.text_field_bit();
        let reader = test.test.context.term_inverted_index().reader(field_mask);
        let gc_collected_term = RSQueryTerm::new("gc_collected", 1, 0);
        // SAFETY: reader and sctx are valid pointers from the test context.
        let mut it = unsafe {
            Term::new(
                reader,
                test.test.context.sctx,
                gc_collected_term,
                1.0,
                NoOpChecker,
            )
        };

        // The reader still works because it reads from the actual inverted
        // index — only the query term stored in the result differs.
        assert!(it.read().expect("failed to read").is_some());

        // Revalidation calls should_abort which looks up "gc_collected" in
        // keysDict. The term is not there so Redis_OpenInvertedIndex returns
        // null, triggering the abort path.
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);
    }

    #[test]
    fn term_revalidate_after_document_deleted() {
        let test = TermRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = {
            use inverted_index::{full::Full, opaque::OpaqueEncoding};
            Full::from_mut_opaque(test.test.context.term_inverted_index_mut()).inner_mut()
        };

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }

    mod via_resume {
        use super::*;
        use crate::inverted_index::utils::via_resume::{
            revalidate_after_document_deleted, revalidate_at_eof, revalidate_basic,
        };
        use rqe_iterators::{ResumeOutcome, TypeErasedRQEIterator};
        use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};

        #[test]
        fn term_revalidate_basic() {
            let test = TermRevalidateTest::new(10);
            let it = test.create_iterator();
            revalidate_basic(&test.test, Box::new(it));
        }

        #[test]
        fn term_revalidate_at_eof() {
            let test = TermRevalidateTest::new(10);
            let it = test.create_iterator();
            revalidate_at_eof(&test.test, Box::new(it));
        }

        #[test]
        fn term_revalidate_after_index_disappears() {
            let test = TermRevalidateTest::new(10);
            let guard = test.test.context.spec_read();

            // First, verify the iterator works normally through a resume cycle.
            let it = Box::new(test.create_iterator());
            let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
                .expect("resume should not fail in this test")
                .expect_ok();
            assert!(it.read().expect("failed to read").is_some());
            revalidate_via_resume(it, &guard)
                .expect("resume should not fail in this test")
                .expect_ok();

            // Now simulate the term's inverted index being garbage collected and
            // replaced: build a fresh iterator and swap its stored index pointer to
            // a different (dummy) index. Redis_OpenInvertedIndex still returns the
            // original, so the pointer comparison fails and resume must abort.
            //
            // (Unlike the `revalidate` path, `resume` consumes the iterator and
            // erases its concrete type, so the swap is performed on a fresh
            // concrete iterator before the single aborting resume rather than
            // between resumes of the same iterator.)
            let mut it = Box::new(test.create_iterator());
            let flags = test.test.context.term_inverted_index().flags();
            let dummy_ptr = Box::into_raw(Box::new(inverted_index::InvertedIndex::<
                inverted_index::full::Full,
            >::new(flags)));
            // SAFETY: `dummy_ptr` was just allocated and is valid.
            let mut dummy_ref: &inverted_index::InvertedIndex<inverted_index::full::Full> =
                unsafe { &*dummy_ptr };
            it.swap_index(&mut dummy_ref);

            let outcome = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
                .expect("resume should not fail in this test");
            assert!(matches!(outcome, ResumeOutcome::Aborted));

            // The aborting resume consumed and dropped the iterator; its reader
            // held a non-owning pointer to the dummy index, so the dummy is still
            // leaked. Free it.
            // SAFETY: `dummy_ptr` was allocated via `Box::into_raw` and not freed.
            drop(unsafe { Box::from_raw(dummy_ptr) });
        }

        #[test]
        fn term_revalidate_after_index_gc_collected() {
            let test = TermRevalidateTest::new(10);

            // Build the iterator with a query term that does not exist in keysDict.
            // This simulates the GC having collected the entire inverted index for
            // that term: Redis_OpenInvertedIndex will return null when should_abort
            // tries to look it up.
            let field_mask = test.test.context.text_field_bit();
            let reader = test.test.context.term_inverted_index().reader(field_mask);
            let gc_collected_term = RSQueryTerm::new("gc_collected", 1, 0);
            // SAFETY: reader and sctx are valid pointers from the test context.
            let mut it = Box::new(unsafe {
                Term::new(
                    reader,
                    test.test.context.sctx,
                    gc_collected_term,
                    1.0,
                    NoOpChecker,
                )
            });

            // The reader still works because it reads from the actual inverted
            // index — only the query term stored in the result differs.
            assert!(it.read().expect("failed to read").is_some());

            // Revalidation calls should_abort which looks up "gc_collected" in
            // keysDict. The term is not there so Redis_OpenInvertedIndex returns
            // null, triggering the abort path.
            let guard = test.test.context.spec_read();
            let outcome = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
                .expect("resume should not fail in this test");
            assert!(matches!(outcome, ResumeOutcome::Aborted));
        }

        #[test]
        fn term_revalidate_after_document_deleted() {
            let test = TermRevalidateTest::new(10);
            let it = test.create_iterator();
            let ii = {
                use inverted_index::{full::Full, opaque::OpaqueEncoding};
                Full::from_mut_opaque(test.test.context.term_inverted_index_mut()).inner_mut()
            };

            revalidate_after_document_deleted(&test.test, Box::new(it), ii);
        }

        /// Regression test for the "skip the first result" bug.
        ///
        /// A freshly-created iterator that is suspended *before any `read()`*
        /// and then hit by a GC cycle (`NeedsReseek`) must still return its
        /// first document on the first post-resume `read()`. The shared
        /// `resume_in_place` re-seeks to the iterator's own `last_doc_id` (which
        /// is 0 until the first read), not the reader's block-initialized
        /// `first_doc_id`, so it must skip the re-seek entirely and leave the
        /// iterator positioned at the start.
        #[test]
        fn term_resume_before_first_read_keeps_first_doc() {
            let test = TermRevalidateTest::new(10);
            let it = Box::new(test.create_iterator());
            let ii = {
                use inverted_index::{full::Full, opaque::OpaqueEncoding};
                Full::from_mut_opaque(test.test.context.term_inverted_index_mut()).inner_mut()
            };

            // Bump the GC marker *without reading the iterator first* by deleting
            // a document that sits after the first one. This forces resume down
            // the `NeedsReseek` branch while the iterator's `last_doc_id` is
            // still 0, and leaves `doc_ids[0]` as the first live document.
            let first = test.test.doc_ids[0];
            test.test.remove_document(ii, test.test.doc_ids[5]);

            let guard = test.test.context.spec_read();
            let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
                .expect("resume should not fail in this test")
                .expect_ok();

            // The first document must not be skipped.
            let doc = it
                .read()
                .expect("failed to read")
                .expect("should not be at EOF");
            assert_eq!(doc.doc_id, first, "resume must not skip the first result");
        }
    }
}
