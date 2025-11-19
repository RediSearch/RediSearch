/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{
    FieldExpiration, IndexFlags, IndexFlags_Index_StoreByteOffsets,
    IndexFlags_Index_StoreFieldFlags, IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric,
    IndexFlags_Index_StoreTermOffsets, IndexSpec, QueryEvalCtx, RedisSearchCtx, SchemaRule,
    t_docId, t_expirationTimePoint, t_fieldIndex, t_fieldMask,
};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{
    DecodedBy, Encoder, FilterMaskReader, FilterNumericReader, InvertedIndex, NumericFilter,
    RSIndexResult, RSOffsetVector, RSResultKind, full::Full, test_utils::TermRecordCompare,
};
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome,
    inverted_index::{Numeric, Term},
};
use std::{cell::UnsafeCell, pin::Pin, ptr};

mod c_mocks;

/// Test basic read and skip_to functionality for a given iterator.
struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
}

/// assert that both records are equal
fn check_record(record: &RSIndexResult, expected: &RSIndexResult) {
    if record.kind() == RSResultKind::Term {
        // the term record is not encoded in the II so we can't compare it directly
        assert_eq!(TermRecordCompare(record), TermRecordCompare(expected));
    } else {
        assert_eq!(record, expected);
    }
}

impl<E: Encoder> BaseTest<E> {
    fn new(
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
    fn docs_ids_iter(&self) -> impl Iterator<Item = u64> {
        self.doc_ids.iter().map(|id| *id)
    }

    /// test read functionality for a given iterator.
    ///
    /// `docs_ids` is an iterator over the expected document ids to read.
    fn read<'index, I>(&self, it: &mut I, doc_ids: impl Iterator<Item = u64>)
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
    fn skip_to<'index, I>(&self, it: &mut I)
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
struct RevalidateTest<E> {
    #[allow(dead_code)]
    doc_ids: Vec<t_docId>,
    // FIXME: horrible hack so we can get a mutable reference to the InvertedIndex while holding an immutable one through the iterator.
    // We should get rid of it once we have designed a proper way to manage concurrent access to the II.
    ii: UnsafeCell<InvertedIndex<E>>,
}

impl<E: Encoder + DecodedBy> RevalidateTest<E> {
    fn new(
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
    fn revalidate_basic<'index, I>(&self, it: &mut I)
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
    fn revalidate_at_eof<'index, I>(&self, it: &mut I)
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
    fn revalidate_after_index_disappears<'index, I>(&self, it: &mut I, full_iterator: bool)
    where
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
    fn remove_document(&self, doc_id: t_docId) {
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
    fn revalidate_after_document_deleted<'index, I>(&self, it: &mut I)
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

/// Test fields expiration.
struct ExpirationTest<E> {
    doc_ids: Vec<t_docId>,
    ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
    mock_ctx: Pin<Box<MockContext>>,
}

impl<E: Encoder> ExpirationTest<E> {
    fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of document IDs for testing.
        let doc_ids = (1..=n_docs).map(|i| i as t_docId).collect::<Vec<_>>();

        let mut ii = InvertedIndex::<E>::new(ii_flags);

        for doc_id in doc_ids.iter() {
            let record = create_record(*doc_id);
            ii.add_record(&record).expect("failed to add record");
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
    fn mark_index_expired(&mut self, ids: Vec<t_docId>, index: t_fieldIndex) {
        let mock_ctx = unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut self.mock_ctx)) };
        mock_ctx.mark_index_expired(ids, index);
    }

    fn context(&self) -> ptr::NonNull<RedisSearchCtx> {
        ptr::NonNull::new(&self.mock_ctx.sctx as *const _ as *mut _)
            .expect("mock context should not be null")
    }

    /// test read of expired documents.
    fn read<'index, I>(&self, it: &mut I)
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
    fn skip_to<'index, I>(&self, it: &mut I)
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

        // Test skipping to ID beyond range
        it.rewind();
        assert!(it.skip_to(last_id + 1).expect("skip_to failed").is_none());
    }
}

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

    fn mark_index_expired(&mut self, ids: Vec<t_docId>, index: t_fieldIndex) {
        // Already expired
        let expiration = t_expirationTimePoint {
            tv_nsec: 1,
            tv_sec: 1,
        };

        for id in ids {
            self.ttl_add_index(id, index, expiration);
        }

        // Set up the mock current time further in the future than the expiration point.
        self.set_current_time(t_expirationTimePoint {
            tv_sec: 100,
            tv_nsec: 100,
        });
    }

    fn ttl_add_index(
        &mut self,
        doc_id: t_docId,
        field: t_fieldIndex,
        expiration: t_expirationTimePoint,
    ) {
        self.verify_ttl_init();

        let fe_entry = FieldExpiration {
            index: field,
            point: expiration,
        };
        let doc_expiration_time = ffi::t_expirationTimePoint {
            tv_sec: i64::MAX,
            tv_nsec: i64::MAX,
        };
        let fe =
            unsafe { ffi::array_new_sz(std::mem::size_of::<FieldExpiration>() as u16, 128, 1) };
        let fe = fe.cast();
        unsafe {
            *fe = fe_entry;
        };

        unsafe {
            ffi::TimeToLiveTable_Add(self.spec.docs.ttl, doc_id, doc_expiration_time, fe as _);
        }
    }

    fn verify_ttl_init(&mut self) {
        if !self.spec.fieldIdToIndex.is_null() {
            return;
        }

        // By default, set a max-length array (128 text fields) with fieldId(i) -> index(i)
        // FIXME: valgrind
        let arr = unsafe { ffi::array_new_sz(std::mem::size_of::<t_fieldIndex>() as u16, 128, 0) };
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

    fn set_current_time(&mut self, time: t_expirationTimePoint) {
        self.sctx.time.current = time;
    }
}

struct NumericTest {
    test: BaseTest<inverted_index::numeric::Numeric>,
    revalidate_test: RevalidateTest<inverted_index::numeric::Numeric>,
    expiration_test: ExpirationTest<inverted_index::numeric::Numeric>,
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
            expiration_test: ExpirationTest::new(
                IndexFlags_Index_StoreNumeric,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }

    fn test_read_expiration(&mut self) {
        const FIELD_INDEX: t_fieldIndex = 42;
        // Make every even document ID field expired
        let even_ids = self
            .expiration_test
            .doc_ids
            .iter()
            .filter(|id| **id % 2 == 0)
            .copied()
            .collect();

        self.expiration_test
            .mark_index_expired(even_ids, FIELD_INDEX);

        let reader = self.expiration_test.ii.reader();
        let mut it = Numeric::with_context(
            reader,
            FieldFilterContext {
                field: FieldMaskOrIndex::Index(FIELD_INDEX),
                predicate: FieldExpirationPredicate::Default,
            },
            self.expiration_test.context(),
        );

        self.expiration_test.read(&mut it);
    }

    fn test_skip_to_expiration(&mut self) {
        const FIELD_INDEX: t_fieldIndex = 42;
        // Make every even document ID field expired
        let even_ids = self
            .expiration_test
            .doc_ids
            .iter()
            .filter(|id| **id % 2 == 0)
            .copied()
            .collect();

        self.expiration_test
            .mark_index_expired(even_ids, FIELD_INDEX);

        let reader = self.expiration_test.ii.reader();
        let mut it = Numeric::with_context(
            reader,
            FieldFilterContext {
                field: FieldMaskOrIndex::Index(FIELD_INDEX),
                predicate: FieldExpirationPredicate::Default,
            },
            self.expiration_test.context(),
        );

        self.expiration_test.skip_to(&mut it);
    }
}

#[test]
/// test reading from Numeric iterator
fn numeric_full_read() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
    test.test.read(&mut it, test.test.docs_ids_iter());

    // same but using a passthrough filter
    let test = NumericTest::new(100);
    let filter = NumericFilter::default();
    let reader = test.test.ii.reader();
    let reader = FilterNumericReader::new(&filter, reader);
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Numeric iterator
fn numeric_full_skip_to() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
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
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
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
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
    test.revalidate_test.revalidate_basic(&mut it);
}

#[test]
fn numeric_full_revalidate_at_eof() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
    test.revalidate_test.revalidate_at_eof(&mut it);
}

#[test]
fn numeric_full_revalidate_after_index_disappears() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
    test.revalidate_test
        .revalidate_after_index_disappears(&mut it, true);
}

#[cfg(not(miri))] // Miri does not like UnsafeCell
#[test]
fn numeric_full_revalidate_after_document_deleted() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());
    test.revalidate_test
        .revalidate_after_document_deleted(&mut it);
}

#[test]
fn numeric_read_expiration() {
    NumericTest::new(100).test_read_expiration();
}

#[test]
fn numeric_skip_to_expiration() {
    NumericTest::new(100).test_skip_to_expiration();
}

struct TermTest {
    test: BaseTest<Full>,
    revalidate_test: RevalidateTest<Full>,
}

impl TermTest {
    // # Safety
    // The returned RSIndexResult contains raw pointers to `term` and `offsets`.
    // These pointers are valid for 'static because the data is moved into the closure
    // in `new()` and lives for the entire duration of the test. The raw pointers are
    // only used within the test's lifetime, making this safe despite the 'static claim.
    fn expected_record(
        doc_id: t_docId,
        term: &Box<ffi::RSQueryTerm>,
        offsets: &Vec<u8>,
    ) -> RSIndexResult<'static> {
        let term: *const _ = &*term;

        RSIndexResult::term_with_term_ptr(
            term as _,
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
        let test_str_ptr = TEST_STR.as_ptr() as *mut _;
        let term = Box::new(ffi::RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        });
        let term2 = Box::new(ffi::RSQueryTerm {
            str_: test_str_ptr,
            len: TEST_STR.len(),
            idf: 5.0,
            id: 1,
            flags: 0,
            bm25_idf: 10.0,
        });

        let offsets = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let offsets_clone = offsets.clone();

        Self {
            test: BaseTest::new(
                flags,
                Box::new(move |doc_id| Self::expected_record(doc_id, &term, &offsets)),
                n_docs,
            ),
            revalidate_test: RevalidateTest::new(
                IndexFlags_Index_StoreTermOffsets,
                Box::new(move |doc_id| Self::expected_record(doc_id, &term2, &offsets_clone)),
                n_docs,
            ),
        }
    }
}

#[test]
/// test reading from Term iterator
fn term_full_read() {
    let test = TermTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Term iterator
fn term_full_skip_to() {
    let test = TermTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Term iterator with a filter
fn term_filter() {
    let test = TermTest::new(10);
    let reader = FilterMaskReader::new(1, test.test.ii.reader());
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    // results have their doc id as field mask so we filter by odd ids
    let docs_ids = test.test.docs_ids_iter().filter(|id| id % 2 == 1);
    test.test.read(&mut it, docs_ids);
}

#[test]
fn term_full_revalidate_basic() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    test.revalidate_test.revalidate_basic(&mut it);
}

#[test]
fn term_full_revalidate_at_eof() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    test.revalidate_test.revalidate_at_eof(&mut it);
}

#[test]
fn term_full_revalidate_after_index_disappears() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    test.revalidate_test
        .revalidate_after_index_disappears(&mut it, true);
}

#[cfg(not(miri))] // Miri does not like UnsafeCell
#[test]
fn term_full_revalidate_after_document_deleted() {
    let test = TermTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Term::new(reader, FieldMaskOrIndex::mask_all());
    test.revalidate_test
        .revalidate_after_document_deleted(&mut it);
}

#[test]
fn skip_multi_id() {
    // Add multiple entries with the same docId
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(3.0).doc_id(1));

    let mut it = Numeric::new(ii.reader(), FieldFilterContext::index_invalid_default());

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
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));

    let mut it = Numeric::new(ii.reader(), FieldFilterContext::index_invalid_default());

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
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(3.0).doc_id(1));

    // Create an iterator that reads only entries with value >= 2.0
    let filter = NumericFilter {
        min: 2.0,
        max: 3.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, ii.reader());
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());

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
        let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(id));
    }

    // Create an iterator that reads only entries with value 2.0
    let filter = NumericFilter {
        min: 2.0,
        max: 2.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, ii.reader());
    let mut it = Numeric::new(reader, FieldFilterContext::index_invalid_default());

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    assert_eq!(it.skip_to(1).expect("skip_to failed"), None);
}
