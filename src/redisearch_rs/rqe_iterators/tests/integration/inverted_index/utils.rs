/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags, t_docId};
use inverted_index::{
    DecodedBy, Encoder, InvertedIndex, RSIndexResult, RSResultKind, test_utils::TermRecordCompare,
};
use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome};
use std::cell::UnsafeCell;

/// Test basic read and skip_to functionality for a given iterator.
pub(super) struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    pub(super) ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
}

/// assert that both records are equal
pub fn check_record(record: &RSIndexResult, expected: &RSIndexResult) {
    if record.kind() == RSResultKind::Term {
        // the term record is not encoded in the II so we can't compare it directly
        assert_eq!(TermRecordCompare(record), TermRecordCompare(expected));
    } else {
        assert_eq!(record, expected);
    }
}

impl<E: Encoder> BaseTest<E> {
    pub(super) fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of odd document IDs for testing, starting from 1.
        let doc_ids = (0..=n_docs)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::<E>::new(ii_flags);

        for doc_id in doc_ids.iter() {
            let record = create_record(*doc_id);
            ii.add_record(&record).expect("failed to add record");
        }

        Self {
            doc_ids,
            ii,
            expected_record,
        }
    }

    /// Iterator over all the document ids present in the inverted index.
    pub(super) fn docs_ids_iter(&self) -> impl Iterator<Item = u64> {
        self.doc_ids.iter().map(|id| *id)
    }

    /// test read functionality for a given iterator.
    ///
    /// `docs_ids` is an iterator over the expected document ids to read.
    pub(super) fn read<'index, I>(&self, it: &mut I, doc_ids: impl Iterator<Item = u64>)
    where
        I: RQEIterator<'index>,
    {
        let expected_record = &*self.expected_record;

        for doc_id in doc_ids {
            let record = it
                .read()
                .expect("failed to read")
                .expect("expected result not eof");

            check_record(record, &expected_record(record.doc_id));
            assert_eq!(it.last_doc_id(), doc_id);
            assert_eq!(it.current().unwrap().doc_id, doc_id);
            assert!(!it.at_eof());
        }

        // We should have read all the documents
        assert_eq!(it.read().unwrap(), None);
        assert!(it.at_eof());
        assert_eq!(it.num_estimated(), self.doc_ids.len());
        assert_eq!(it.num_estimated(), self.ii.unique_docs() as usize);

        // try reading at eof
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    /// Test skip_to functionality for a given iterator.
    ///
    /// Since the index contains only ODD doc IDs (1, 3, 5, 7, ...), when we skip to an EVEN doc ID,
    /// we expect `NotFound` with the next odd doc ID returned.
    pub(super) fn skip_to<'index, I>(&self, it: &mut I)
    where
        I: RQEIterator<'index>,
    {
        // Test skipping to any id between 1 and the last id.
        let expected_record = &*self.expected_record;
        // Test skipping to any id between 1 and the last id
        let mut i = 1;
        for id in self.doc_ids.iter().copied() {
            // First, test skipping to the even doc ID that comes before this odd ID
            // (except for the first iteration where i=1 and id=1).
            // Since doc IDs are odd numbers (1, 3, 5, ...), the even numbers don't exist.
            if i < id {
                it.rewind();
                let res = it.skip_to(i);
                // Expect NotFound because `i` doesn't exist in the index (it's an even number).
                // The iterator should return the next available document, which is `id`.
                let Ok(Some(SkipToOutcome::NotFound(record))) = res else {
                    panic!("skip_to {i} should succeed with NotFound: {res:?}");
                };

                check_record(record, &expected_record(id));
                assert_eq!(it.last_doc_id(), id);
                assert_eq!(it.current().unwrap().doc_id, id);
                i += 1;
            }
            // Now test skipping to the exact doc ID that exists in the index.
            it.rewind();
            let res = it.skip_to(id);
            // Expect Found because `id` is an odd number that exists in the index.
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            check_record(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
            i += 1;
        }

        // Test reading after skipping to the last id
        assert!(matches!(it.read(), Ok(None)));
        let last_doc_id = it.last_doc_id();
        assert!(matches!(it.skip_to(last_doc_id + 1), Ok(None)));
        assert!(it.at_eof());

        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert_eq!(it.current().unwrap().doc_id, 0);
        assert!(!it.at_eof());

        // Test skipping to all ids that exist
        for id in self.doc_ids.iter().copied() {
            let res = it.skip_to(id);
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            check_record(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
        }

        // Test skipping to an id that exceeds the last id
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert_eq!(it.current().unwrap().doc_id, 0);
        assert!(!it.at_eof());
        let res = it.skip_to(self.doc_ids.last().unwrap() + 1);
        assert!(matches!(res, Ok(None)));
        // we just rewound
        assert_eq!(it.last_doc_id(), 0);
        assert_eq!(it.current().unwrap().doc_id, 0);
        assert!(it.at_eof());
    }
}

/// Test the revalidation of the iterator.
pub(super) struct RevalidateTest<E> {
    #[allow(dead_code)]
    doc_ids: Vec<t_docId>,
    // FIXME: horrible hack so we can get a mutable reference to the InvertedIndex while holding an immutable one through the iterator.
    // We should get rid of it once we have designed a proper way to manage concurrent access to the II.
    pub(super) ii: UnsafeCell<InvertedIndex<E>>,
}

impl<E: Encoder + DecodedBy> RevalidateTest<E> {
    pub(super) fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of odd document IDs for testing, starting from 1.
        let doc_ids = (0..=n_docs)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::<E>::new(ii_flags);
        for doc_id in doc_ids.iter() {
            let record = create_record(*doc_id);
            ii.add_record(&record).expect("failed to add record");
        }

        Self {
            doc_ids,
            ii: UnsafeCell::new(ii),
        }
    }

    /// test basic revalidation functionality - should return `RQEValidateStatus::Ok`` when index is valid
    pub(super) fn revalidate_basic<'index, I>(&self, it: &mut I)
    where
        I: for<'iterator> RQEIterator<'index>,
    {
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(matches!(it.read(), Ok(Some(_))));
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
    }

    /// test revalidation functionality when iterator is at EOF
    pub(super) fn revalidate_at_eof<'index, I>(&self, it: &mut I)
    where
        I: for<'iterator> RQEIterator<'index>,
    {
        // Read all documents to reach EOF
        while let Some(_record) = it.read().expect("failed to read") {}
        assert!(it.at_eof());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
    }

    /// test revalidate returns `Aborted` when the underlying index disappears
    pub(super) fn revalidate_after_index_disappears<'index, I>(
        &self,
        it: &mut I,
        full_iterator: bool,
    ) where
        I: for<'iterator> RQEIterator<'index>,
    {
        // First, verify the iterator works normally and read at least one document
        // TODO: update this comment once we actually implement CheckAbort:
        // This is important because CheckAbort functions need current->data.term.term to be set
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(it.read().expect("failed to read").is_some());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        if full_iterator {
            // Full iterators don't have sctx, so they can't detect disappearance
            // They will always return Ok regardless of index state
            assert_eq!(
                it.revalidate().expect("revalidate failed"),
                RQEValidateStatus::Ok
            );
        } else {
            todo!()
        }
    }

    /// Remove the document with the given id from the inverted index.
    #[cfg(not(miri))] // Miri does not like UnsafeCell
    pub(super) fn remove_document(&self, doc_id: t_docId) {
        let ii = unsafe { &mut *self.ii.get() };

        let scan_delta = ii
            .scan_gc(
                |d| d != doc_id,
                None::<fn(&RSIndexResult, &inverted_index::IndexBlock)>,
            )
            .expect("scan GC failed")
            .expect("no GC scan delta");
        let info = ii.apply_gc(scan_delta);
        assert_eq!(info.entries_removed, 1);
    }

    /// test revalidate returns `Moved` when the document at the iterator position is deleted from the index.
    #[cfg(not(miri))] // Miri does not like UnsafeCell
    pub(super) fn revalidate_after_document_deleted<'index, I>(&self, it: &mut I)
    where
        I: for<'iterator> RQEIterator<'index>,
    {
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // First, read a few documents to establish a position
        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[0]);

        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[1]);

        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[2]);

        assert_eq!(it.last_doc_id(), self.doc_ids[2]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

        // Nothing changed in the index so revalidate does nothing
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // Remove an element before the current iteration position.
        self.remove_document(self.doc_ids[0]);
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert_eq!(it.last_doc_id(), self.doc_ids[2]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

        // Remove an element after the current iteration position.
        self.remove_document(self.doc_ids[4]);
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert_eq!(it.last_doc_id(), self.doc_ids[2]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

        // Remove the element at the current position of the iterator.
        // When validating we won't be able to skip to this element, so we should get RQEValidateStatus::Moved.
        self.remove_document(self.doc_ids[2]);
        let res = it.revalidate().expect("revalidate failed");
        let current_doc = match res {
            RQEValidateStatus::Moved {
                current: Some(current),
            } => current,
            _ => panic!("wrong revalidate result: {:?}", res),
        };
        assert_eq!(current_doc.doc_id, self.doc_ids[3]);
        // iterator advanced to the next element
        assert_eq!(it.last_doc_id(), self.doc_ids[3]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[3]);

        // read the next element, docs_ids[4] has been removed so iterator should return the one after.
        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[5]);
        assert_eq!(it.last_doc_id(), self.doc_ids[5]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[5]);

        // edge case: iterator is at the last document which is then removed.
        assert!(!it.at_eof());
        let last_doc_id = *self.doc_ids.last().unwrap();
        let doc = match it.skip_to(last_doc_id) {
            Ok(Some(SkipToOutcome::Found(doc))) => doc,
            _ => panic!("skip_to {last_doc_id} should succeed"),
        };
        assert_eq!(doc.doc_id, last_doc_id);
        assert_eq!(it.last_doc_id(), last_doc_id);
        assert_eq!(it.current().unwrap().doc_id, last_doc_id);

        self.remove_document(last_doc_id);
        // revalidate should return Moved without current doc and be at EOF.
        let res = it.revalidate().expect("revalidate failed");
        assert!(matches!(res, RQEValidateStatus::Moved { current: None }));
        assert!(it.at_eof());
    }
}

#[cfg(not(miri))]
// Those tests rely on ffi calls which are not supported in miri.
pub(super) mod not_miri {
    use ffi::{
        FieldExpiration, IndexFlags, IndexSpec, QueryEvalCtx, RedisSearchCtx, SchemaRule, t_docId,
        t_expirationTimePoint, t_fieldIndex,
    };
    use field::FieldMaskOrIndex;
    use inverted_index::{Encoder, InvertedIndex, RSIndexResult};
    use rqe_iterators::{RQEIterator, SkipToOutcome};
    use std::{pin::Pin, ptr};

    use super::check_record;

    /// Test fields expiration.
    pub struct ExpirationTest<E> {
        pub(crate) doc_ids: Vec<t_docId>,
        pub(crate) ii: InvertedIndex<E>,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        mock_ctx: Pin<Box<MockContext>>,
    }

    impl<E: Encoder> ExpirationTest<E> {
        pub(crate) fn new(
            ii_flags: IndexFlags,
            expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
            n_docs: u64,
            multi: bool,
        ) -> Self {
            let create_record = &*expected_record;
            // Generate a set of document IDs for testing.
            let doc_ids = (1..=n_docs).map(|i| i as t_docId).collect::<Vec<_>>();

            let mut ii = InvertedIndex::<E>::new(ii_flags);

            for doc_id in doc_ids.iter() {
                let record = create_record(*doc_id);
                ii.add_record(&record).expect("failed to add record");
                if multi {
                    // add each record twice in multi mode
                    ii.add_record(&record).expect("failed to add record");
                }
            }

            let mock_ctx = MockContext::new(0, 0);

            Self {
                doc_ids,
                ii,
                expected_record,
                mock_ctx,
            }
        }

        /// Mark the index as expired for the given document IDs.
        pub(crate) fn mark_index_expired(&mut self, ids: Vec<t_docId>, index: FieldMaskOrIndex) {
            let mock_ctx = unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut self.mock_ctx)) };
            mock_ctx.mark_index_expired(ids, index);
        }

        pub(crate) fn context(&self) -> ptr::NonNull<RedisSearchCtx> {
            ptr::NonNull::new(&self.mock_ctx.sctx as *const _ as *mut _)
                .expect("mock context should not be null")
        }

        /// test read of expired documents.
        pub(crate) fn read<'index, I>(&self, it: &mut I)
        where
            I: RQEIterator<'index>,
        {
            let expected_record = &*self.expected_record;

            for doc_id in self.doc_ids.iter().step_by(2).copied() {
                let record = it
                    .read()
                    .expect("failed to read")
                    .expect("expected result not eof");

                check_record(record, &expected_record(record.doc_id));
                assert_eq!(it.last_doc_id(), doc_id);
                assert_eq!(it.current().unwrap().doc_id, doc_id);
                assert!(!it.at_eof());
            }

            // We should have read all the documents
            assert_eq!(it.read().unwrap(), None);
            assert!(it.at_eof());
            assert_eq!(it.num_estimated(), self.doc_ids.len());
            assert_eq!(it.num_estimated(), self.ii.unique_docs() as usize);

            // try reading at eof
            assert!(matches!(it.read(), Ok(None)));
            assert!(it.at_eof());
        }

        /// test skip_to on expired documents.
        pub(crate) fn skip_to<'index, I>(&self, it: &mut I)
        where
            I: RQEIterator<'index>,
        {
            let expected_record = &*self.expected_record;

            let last_id = self.doc_ids.last().copied().unwrap();

            // Skip to odd IDs should work
            let odd_ids = self.doc_ids.iter().filter(|id| **id % 2 != 0).copied();
            for doc_id in odd_ids {
                let record = match it
                    .skip_to(doc_id)
                    .expect("skip_to failed")
                    .expect("expected result not eof")
                {
                    SkipToOutcome::Found(res) => res,
                    SkipToOutcome::NotFound(_) => panic!("Document not found"),
                };

                check_record(record, &expected_record(doc_id));
                assert_eq!(it.last_doc_id(), doc_id);
                assert_eq!(it.current().unwrap().doc_id, doc_id);
                assert!(!it.at_eof());
            }

            // Test skipping to even IDs - should skip to next odd ID
            it.rewind();

            let even_ids = self
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0 && **id != last_id)
                .copied();
            for doc_id in even_ids {
                let record = match it
                    .skip_to(doc_id)
                    .expect("skip_to failed")
                    .expect("expected result not eof")
                {
                    SkipToOutcome::Found(_) => panic!("Should not find even ID"),
                    SkipToOutcome::NotFound(res) => res,
                };

                check_record(record, &expected_record(doc_id + 1));
                assert_eq!(it.last_doc_id(), doc_id + 1);
                assert_eq!(it.current().unwrap().doc_id, doc_id + 1);
                assert!(!it.at_eof());
            }

            if last_id % 2 == 0 {
                // the last id is odd, so trying to skip to it should move to eof
                assert!(it.skip_to(last_id).expect("skip_to failed").is_none());
                assert!(it.at_eof());
            }

            // iterator has reached eof
            assert!(it.skip_to(last_id + 1).expect("skip_to failed").is_none());

            // Test skipping to ID beyond range
            it.rewind();
            assert!(it.skip_to(last_id + 1).expect("skip_to failed").is_none());
        }
    }

    /// Mock search context used in tests requiring access to the context.
    struct MockContext {
        rule: SchemaRule,
        spec: IndexSpec,
        sctx: RedisSearchCtx,
        qctx: QueryEvalCtx,
    }

    impl Default for MockContext {
        fn default() -> Self {
            let rule: SchemaRule = unsafe { std::mem::zeroed() };
            let spec: IndexSpec = unsafe { std::mem::zeroed() };
            let sctx: RedisSearchCtx = unsafe { std::mem::zeroed() };
            let qctx: QueryEvalCtx = unsafe { std::mem::zeroed() };

            Self {
                rule,
                spec,
                sctx,
                qctx,
            }
        }
    }

    impl Drop for MockContext {
        fn drop(&mut self) {
            unsafe {
                ffi::array_free(self.spec.fieldIdToIndex as _);
            }

            unsafe {
                ffi::TimeToLiveTable_Destroy(&mut self.spec.docs.ttl as _);
            }
        }
    }

    impl MockContext {
        fn new(max_doc_id: t_docId, num_docs: usize) -> Pin<Box<Self>> {
            // Need to Pin the whole struct so pointers remain valid.
            let mut boxed = Box::pin(Self::default());

            // SAFETY: We need to set up self-referential pointers after pinning.
            // The struct is now pinned and won't move, so these pointers will remain valid.
            unsafe {
                let ptr = boxed.as_mut().get_unchecked_mut();

                // Initialize SchemaRule
                ptr.rule.index_all = false;

                // Initialize IndexSpec
                ptr.spec.rule = &mut ptr.rule;
                ptr.spec.monitorDocumentExpiration = true; // Only depends on API availability, so always true
                ptr.spec.monitorFieldExpiration = true; // Only depends on API availability, so always true
                ptr.spec.docs.maxDocId = max_doc_id;
                ptr.spec.docs.size = if num_docs > 0 {
                    num_docs
                } else {
                    max_doc_id as usize
                };
                ptr.spec.stats.numDocuments = ptr.spec.docs.size;

                // Initialize RedisSearchCtx
                ptr.sctx.spec = &mut ptr.spec;

                // Initialize QueryEvalCtx
                ptr.qctx.sctx = &mut ptr.sctx;
                ptr.qctx.docTable = &mut ptr.spec.docs;
            }

            boxed
        }

        /// Mark the given field of the given documents as expired.
        fn mark_index_expired(&mut self, ids: Vec<t_docId>, field: FieldMaskOrIndex) {
            // Already expired
            let expiration = t_expirationTimePoint {
                tv_nsec: 1,
                tv_sec: 1,
            };

            for id in ids {
                self.ttl_add(id, field, expiration);
            }

            // Set up the mock current time further in the future than the expiration point.
            self.sctx.time.current = t_expirationTimePoint {
                tv_sec: 100,
                tv_nsec: 100,
            };
        }

        /// Add a TTL entry for the given field in the given document.
        fn ttl_add(
            &mut self,
            doc_id: t_docId,
            field: FieldMaskOrIndex,
            expiration: t_expirationTimePoint,
        ) {
            self.verify_ttl_init();

            let fe = match field {
                FieldMaskOrIndex::Index(index) => {
                    let fe_entry = FieldExpiration {
                        index,
                        point: expiration,
                    };

                    let fe = unsafe {
                        ffi::array_new_sz(std::mem::size_of::<FieldExpiration>() as u16, 0, 1)
                    };
                    let fe = fe.cast();
                    unsafe {
                        *fe = fe_entry;
                    };

                    fe
                }
                FieldMaskOrIndex::Mask(_mask) => todo!(),
            };

            let doc_expiration_time = ffi::t_expirationTimePoint {
                tv_sec: i64::MAX,
                tv_nsec: i64::MAX,
            };

            unsafe {
                ffi::TimeToLiveTable_Add(self.spec.docs.ttl, doc_id, doc_expiration_time, fe as _);
            }
        }

        /// Ensure the spec TTL table is initialized.
        fn verify_ttl_init(&mut self) {
            if !self.spec.fieldIdToIndex.is_null() {
                return;
            }

            // By default, set a max-length array (128 text fields) with fieldId(i) -> index(i)
            let arr =
                unsafe { ffi::array_new_sz(std::mem::size_of::<t_fieldIndex>() as u16, 0, 128) };
            let arr = arr.cast::<t_fieldIndex>();

            for i in 0..128 as t_fieldIndex {
                unsafe {
                    arr.offset(i as isize).write(i);
                }
            }

            self.spec.fieldIdToIndex = arr as _;
            unsafe {
                ffi::TimeToLiveTable_VerifyInit(&mut self.spec.docs.ttl);
            }
        }
    }
}

#[cfg(not(miri))]
pub use not_miri::ExpirationTest;
