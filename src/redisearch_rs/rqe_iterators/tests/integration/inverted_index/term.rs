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
    inverted_index::utils::{BaseTest, RevalidateIndexType, RevalidateTest},
};

struct TermBaseTest {
    test: BaseTest<Full>,
}

impl TermBaseTest {
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
        }
    }
}

#[test]
/// test reading from Term iterator
fn term_read() {
    let test = TermBaseTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Term::new_simple(reader);
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Term iterator
fn term_skip_to() {
    let test = TermBaseTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Term::new_simple(reader);
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Term iterator with a filter
fn term_filter() {
    let test = TermBaseTest::new(10);
    let reader = FilterMaskReader::new(1, test.test.ii.reader());
    let mut it = Term::new_simple(reader);
    // results have their doc id as field mask so we filter by odd ids
    let docs_ids = test.test.docs_ids_iter().filter(|id| id % 2 == 1);
    test.test.read(&mut it, docs_ids);
}

#[cfg(not(miri))]
mod not_miri {
    use std::ptr;

    use super::*;
    use crate::inverted_index::utils::ExpirationTest;
    use ffi::RS_FIELDMASK_ALL;
    use inverted_index::{DecodedBy, Decoder, Encoder, TermDecoder, full::FullWide};
    use rqe_iterators::{RQEIterator, RQEValidateStatus};

    struct TermExpirationTest<E> {
        test: ExpirationTest<E>,
    }

    fn create_term() -> ptr::NonNull<ffi::RSQueryTerm> {
        let mut token = ffi::RSToken {
            str_: "term".as_bytes().as_ptr() as _,
            len: 4,
            _bitfield_align_1: [],
            _bitfield_1: ffi::__BindgenBitfieldUnit::new([0; _]),
            __bindgen_padding_0: 0,
        };
        let term = unsafe { ffi::NewQueryTerm(&mut token, 1) };
        ptr::NonNull::new(term).expect("term is null")
    }

    impl<E, D> TermExpirationTest<E>
    where
        E: Encoder + DecodedBy<Decoder = D>,
        D: Decoder + TermDecoder,
    {
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

        fn new(n_docs: u64, multi: bool, wide: bool) -> Self {
            let mut flags = IndexFlags_Index_StoreFreqs
                | IndexFlags_Index_StoreTermOffsets
                | IndexFlags_Index_StoreFieldFlags
                | IndexFlags_Index_StoreByteOffsets;
            if wide {
                flags |= IndexFlags_Index_WideSchema;
            }

            const TEST_STR: &str = "term";

            let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

            Self {
                test: ExpirationTest::new(
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
                    multi,
                ),
            }
        }

        fn create_iterator(
            &self,
            field_mask: t_fieldMask,
        ) -> Term<'_, inverted_index::IndexReaderCore<'_, E>> {
            let reader = self.test.ii.reader();

            Term::new(
                reader,
                self.test.mock_ctx.sctx(),
                field_mask,
                FieldExpirationPredicate::Default,
                create_term(),
                1.0,
            )
        }

        fn test_read_expiration(&mut self) {
            const FIELD_MASK: t_fieldMask = 42;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, FieldMaskOrIndex::Mask(FIELD_MASK));

            let mut it = self.create_iterator(FIELD_MASK);
            self.test.read(&mut it);
        }

        fn test_skip_to_expiration(&mut self) {
            const FIELD_MASK: t_fieldMask = 42;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, FieldMaskOrIndex::Mask(FIELD_MASK));

            let mut it = self.create_iterator(FIELD_MASK);
            self.test.skip_to(&mut it);
        }
    }

    #[test]
    fn term_read_expiration() {
        TermExpirationTest::<Full>::new(100, false, false).test_read_expiration();
    }

    #[test]
    fn term_read_expiration_wide() {
        TermExpirationTest::<FullWide>::new(100, false, true).test_read_expiration();
    }

    #[test]
    fn term_read_skip_multi_expiration() {
        TermExpirationTest::<Full>::new(100, true, false).test_read_expiration();
    }

    #[test]
    fn term_skip_to_expiration() {
        TermExpirationTest::<Full>::new(100, false, false).test_skip_to_expiration();
    }

    struct TermRevalidateTest {
        test: RevalidateTest<Full>,
    }

    impl TermRevalidateTest {
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
            const TEST_STR: &str = "term";

            let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Term,
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
            }
        }

        fn create_iterator(&self) -> Term<'_, inverted_index::IndexReaderCore<'_, Full>> {
            let reader = unsafe { (*self.test.ii.get()).reader() };
            let context = &self.test.context;

            Term::new(
                reader,
                context.sctx,
                RS_FIELDMASK_ALL,
                FieldExpirationPredicate::Default,
                create_term(),
                1.0,
            )
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
        use inverted_index::{InvertedIndex, full::Full};

        let test = TermRevalidateTest::new(10);

        // Create a dummy index to simulate the "new" index that would be returned
        // by the lookup after GC.
        let flags = ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets;
        let dummy_idx: InvertedIndex<Full> = InvertedIndex::new(flags);

        // Increment the gc_marker to ensure needs_revalidation() returns true
        // This simulates the index being garbage collected.
        dummy_idx.gc_marker_inc();

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

        // Simulate index disappearance by swapping the iterator's index pointer
        // with a different (dummy) index. This simulates the GC scenario where
        // the index was garbage collected and recreated.

        // Get mutable access to the reader and swap its index
        {
            let reader = it.reader_mut();
            let mut dummy_idx_ref = &dummy_idx;
            reader.swap_index(&mut dummy_idx_ref);
        }

        // Now Revalidate should return Aborted because the stored index
        // doesn't match what the lookup returns.
        /* FIXME
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Aborted
        ); */

        // Restore the original index pointer for proper cleanup before dropping the iterator
        {
            let reader = it.reader_mut();
            let original_idx = unsafe { &*test.test.ii.get() };
            let mut original_idx_ref = original_idx;
            reader.swap_index(&mut original_idx_ref);
        }

        // Now drop the iterator explicitly while both indices are still alive
        drop(it);
    }

    #[test]
    fn term_revalidate_after_document_deleted() {
        let test = TermRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_after_document_deleted(&mut it);
    }
}
