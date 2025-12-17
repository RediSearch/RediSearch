/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{pin::Pin, ptr};

use ffi::{
    IndexFlags, IndexSpec, NumericRangeTree, QueryEvalCtx, RedisSearchCtx, SchemaRule, t_docId,
};
#[cfg(not(miri))]
use field::FieldMaskOrIndex;
use inverted_index::{
    Encoder, InvertedIndex, RSIndexResult, RSResultKind, test_utils::TermRecordCompare,
};
use rqe_iterators::{RQEIterator, SkipToOutcome};

/// Mock search context creating fake objects for testing.
/// It can be used to test expiration but not validation.
/// Use [`TestContext`] instead to test revalidation.
pub(crate) struct MockContext {
    rule: SchemaRule,
    spec: IndexSpec,
    sctx: RedisSearchCtx,
    qctx: QueryEvalCtx,
    /// fake NumericRangeTree, cannot be used but those tests do not call revalidate()
    numeric_range_tree: NumericRangeTree,
}

impl Default for MockContext {
    fn default() -> Self {
        let rule: SchemaRule = unsafe { std::mem::zeroed() };
        let spec: IndexSpec = unsafe { std::mem::zeroed() };
        let sctx: RedisSearchCtx = unsafe { std::mem::zeroed() };
        let qctx: QueryEvalCtx = unsafe { std::mem::zeroed() };
        let numeric_range_tree: NumericRangeTree = unsafe { std::mem::zeroed() };

        Self {
            rule,
            spec,
            sctx,
            qctx,
            numeric_range_tree,
        }
    }
}

// Only the miri version will allocate those as it needs to do ffi calls
#[cfg(not(miri))]
impl Drop for MockContext {
    fn drop(&mut self) {
        unsafe {
            ffi::array_free(self.spec.fieldIdToIndex as _);
        }

        unsafe {
            if !self.spec.docs.ttl.is_null() {
                ffi::TimeToLiveTable_Destroy(&mut self.spec.docs.ttl as _);
            }
        }
    }
}

impl MockContext {
    pub(crate) fn new(max_doc_id: t_docId, num_docs: usize) -> Pin<Box<Self>> {
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
            ptr.spec.stats.scoring.numDocuments = ptr.spec.docs.size;

            // Initialize RedisSearchCtx
            ptr.sctx.spec = &mut ptr.spec;

            // Initialize QueryEvalCtx
            ptr.qctx.sctx = &mut ptr.sctx;
            ptr.qctx.docTable = &mut ptr.spec.docs;
        }

        boxed
    }

    #[cfg(not(miri))] // those functions do ffi calls which are not supported by miri
    /// Mark the given field of the given documents as expired.
    fn mark_index_expired(&mut self, ids: Vec<t_docId>, field: FieldMaskOrIndex) {
        use ffi::t_expirationTimePoint;

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

    #[cfg(not(miri))]
    /// Add a TTL entry for the given field in the given document.
    fn ttl_add(
        &mut self,
        doc_id: t_docId,
        field: FieldMaskOrIndex,
        expiration: ffi::t_expirationTimePoint,
    ) {
        use ffi::FieldExpiration;

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
            FieldMaskOrIndex::Mask(mask) => {
                let mut entries = vec![];
                // Add a FieldExpiration for each bit set in the mask
                let mut value = mask;
                while value != 0 {
                    let index = value.trailing_zeros();
                    let fe_entry = FieldExpiration {
                        index: index as u16,
                        point: expiration,
                    };
                    entries.push(fe_entry);
                    value &= value - 1;
                }

                let fe = unsafe {
                    ffi::array_new_sz(
                        std::mem::size_of::<FieldExpiration>() as u16,
                        0,
                        entries.len() as u32,
                    )
                };

                let fe = fe.cast::<FieldExpiration>();
                for (i, fe_entry) in entries.into_iter().enumerate() {
                    unsafe {
                        *fe.offset(i as isize) = fe_entry;
                    }
                }

                fe
            }
        };

        let doc_expiration_time = ffi::t_expirationTimePoint {
            tv_sec: i64::MAX,
            tv_nsec: i64::MAX,
        };

        unsafe {
            ffi::TimeToLiveTable_Add(self.spec.docs.ttl, doc_id, doc_expiration_time, fe as _);
        }
    }

    #[cfg(not(miri))]
    /// Ensure the spec TTL table is initialized.
    fn verify_ttl_init(&mut self) {
        use ffi::t_fieldIndex;

        if !self.spec.fieldIdToIndex.is_null() {
            return;
        }

        // By default, set a max-length array (128 text fields) with fieldId(i) -> index(i)
        let arr = unsafe { ffi::array_new_sz(std::mem::size_of::<t_fieldIndex>() as u16, 0, 128) };
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

    pub(crate) fn sctx(&self) -> ptr::NonNull<RedisSearchCtx> {
        ptr::NonNull::new(&self.sctx as *const _ as *mut _)
            .expect("mock context should not be null")
    }

    pub(crate) fn numeric_range_tree(&self) -> ptr::NonNull<NumericRangeTree> {
        ptr::NonNull::new(&self.numeric_range_tree as *const _ as *mut _)
            .expect("NumericRangeTree should not be null")
    }
}

/// Test basic read and skip_to functionality for a given iterator.
pub(super) struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    pub(super) ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
    pub(super) mock_ctx: Pin<Box<MockContext>>,
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

        let mock_ctx = MockContext::new(0, 0);

        Self {
            doc_ids,
            ii,
            expected_record,
            mock_ctx,
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

#[cfg(not(miri))]
// Those tests rely on ffi calls which are not supported in miri.
pub(super) mod not_miri {
    use super::*;
    use ffi::{
        IndexFlags, IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
        IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric,
        IndexFlags_Index_StoreTermOffsets, t_docId,
    };
    use field::FieldMaskOrIndex;
    use inverted_index::{DecodedBy, Encoder, InvertedIndex, RSIndexResult};
    use query_error::QueryError;
    use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome};
    use std::{
        cell::UnsafeCell,
        ffi::CString,
        pin::Pin,
        ptr,
        sync::atomic::{AtomicBool, Ordering},
    };

    /// ---------- Expiration Tests ----------

    /// Test fields expiration.
    pub struct ExpirationTest<E> {
        pub(crate) doc_ids: Vec<t_docId>,
        pub(crate) ii: InvertedIndex<E>,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        pub(crate) mock_ctx: Pin<Box<MockContext>>,
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

    /// ---------- Revalidate Tests ----------

    /// Wrapper around RedisModuleCtx ensuring its resources are properly cleaned up.
    struct ModuleCtx {
        ctx: ptr::NonNull<ffi::RedisModuleCtx>,
    }

    impl ModuleCtx {
        fn new() -> Self {
            // The ffi calls we call here relies on the Redis module API being initialized.
            redis_mock::init_redis_module_mock();

            let ctx = unsafe {
                let get_thread_safe_context = ffi::RedisModule_GetThreadSafeContext
                    .expect("RedisModule_GetThreadSafeContext not implemented");
                get_thread_safe_context(ptr::null_mut())
            };

            unsafe {
                // init global variables needed by C code
                ffi::Indexes_Init(ctx);
            }

            Self {
                ctx: ptr::NonNull::new(ctx).expect("Failed to create ModuleCtx"),
            }
        }

        fn as_ptr(&self) -> *mut ffi::RedisModuleCtx {
            self.ctx.as_ptr()
        }
    }

    impl Drop for ModuleCtx {
        fn drop(&mut self) {
            unsafe {
                let free_thread_safe_context = ffi::RedisModule_FreeThreadSafeContext
                    .expect("RedisModule_FreeThreadSafeContext not implemented");
                free_thread_safe_context(self.ctx.as_ptr());
            }
        }
    }

    /// Search context created using ffi calls to be able to test revalidation.
    pub struct TestContext {
        _ctx: ModuleCtx,
        pub sctx: ptr::NonNull<ffi::RedisSearchCtx>,
        pub spec: ptr::NonNull<ffi::IndexSpec>,

        inner: TestContextInner,
    }

    enum TestContextInner {
        Numeric {
            field_spec: ptr::NonNull<ffi::FieldSpec>,
            numeric_range_tree: ptr::NonNull<ffi::NumericRangeTree>,
        },
        Term,
    }

    /// Create a spec and search context from the given schema and index name.
    fn create_spec_sctx(
        ctx: &ModuleCtx,
        schema: &str,
        index_name: &str,
    ) -> (
        ptr::NonNull<ffi::IndexSpec>,
        ptr::NonNull<ffi::RedisSearchCtx>,
    ) {
        let args = schema
            .split(" ")
            .map(|s| CString::new(s).expect("Failed to create CString"))
            .collect::<Vec<_>>();
        let mut args_ptr = args.iter().map(|s| s.as_ptr()).collect::<Vec<_>>();

        let index_name = CString::new(index_name).unwrap();
        let mut query_error = QueryError::default();

        let spec_ref = unsafe {
            ffi::IndexSpec_ParseC(
                index_name.as_ptr(),
                args_ptr.as_mut_ptr(),
                args_ptr.len() as i32,
                &mut query_error as *mut QueryError as _,
            )
        };
        assert!(query_error.is_ok());

        let spec = unsafe { ffi::StrongRef_Get(spec_ref) as *mut ffi::IndexSpec };
        let spec = ptr::NonNull::new(spec).expect("IndexSpec should not be null");

        // Add the spec to the global dictionary so it can be found by name
        unsafe {
            ffi::Spec_AddToDict(spec.as_ref().own_ref.rm);
        }

        // Create RedisSearchCtx
        let sctx = unsafe { ffi::NewSearchCtxC(ctx.as_ptr(), index_name.as_ptr(), false) };
        let sctx = ptr::NonNull::new(sctx).expect("RedisSearchCtx should not be null");

        (spec, sctx)
    }

    impl TestContext {
        fn numeric() -> Self {
            let ctx = ModuleCtx::new();
            // Create IndexSpec for NUMERIC field
            let (spec, sctx) = create_spec_sctx(&ctx, "SCHEMA num_field NUMERIC", "numeric_idx");

            // We need to properly set up the numeric range tree
            // so that NumericCheckAbort can find it and check revision IDs
            let field_name = CString::new("num_field").unwrap();
            let fs = unsafe {
                ffi::IndexSpec_GetFieldWithLength(
                    spec.as_ptr(),
                    field_name.as_ptr(),
                    field_name.as_bytes().len(),
                )
            };
            let fs = ptr::NonNull::new(fs as _).expect("FieldSpec should not be null");

            // Create the numeric range tree through the proper API
            let numeric_range_tree =
                unsafe { ffi::openNumericOrGeoIndex(spec.as_ptr(), fs.as_ptr(), true) };
            let numeric_range_tree =
                ptr::NonNull::new(numeric_range_tree).expect("NumericRangeTree should not be null");

            Self {
                _ctx: ctx,
                sctx,
                spec,
                inner: TestContextInner::Numeric {
                    field_spec: fs,
                    numeric_range_tree,
                },
            }
        }

        fn term() -> Self {
            let ctx = ModuleCtx::new();
            let (spec, sctx) = create_spec_sctx(&ctx, "SCHEMA text_field TEXT", "term_idx");

            Self {
                _ctx: ctx,
                sctx,
                spec,
                inner: TestContextInner::Term,
            }
        }

        pub fn numeric_range_tree(&self) -> ptr::NonNull<ffi::NumericRangeTree> {
            match self.inner {
                TestContextInner::Numeric {
                    numeric_range_tree, ..
                } => numeric_range_tree,
                _ => panic!("TestContext is not a Numeric context"),
            }
        }

        pub fn field_spec(&self) -> &ffi::FieldSpec {
            match self.inner {
                TestContextInner::Numeric { field_spec, .. } => unsafe { field_spec.as_ref() },
                _ => panic!("TestContext is not a Numeric context"),
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            unsafe {
                ffi::SearchCtx_Free(self.sctx.as_ptr());
            }

            unsafe {
                // Use the main thread to free resources
                ffi::RSGlobalConfig.freeResourcesThread = false;
            }

            // Remove spec from globals (this may free associated indices)
            unsafe {
                ffi::IndexSpec_RemoveFromGlobals(self.spec.as_ref().own_ref, false);
            }
        }
    }

    /// Guard object that manages globally allocated resources.
    /// Uses libc::atexit to register a cleanup function that releases globally allocated resources
    /// when the process exits, ensuring it's called exactly once after all tests complete.
    struct GlobalGuard;

    impl GlobalGuard {
        // atexit() is only available on Linux.
        // This means means global resources are not cleaned up on non-Linux platforms.
        // It's not that bad as those are tests and the real goal here is to detect actual memory leaks
        // using Valgrind which is only available on Linux as well.
        #[cfg(target_os = "linux")]
        fn new() -> Self {
            static REGISTERED: AtomicBool = AtomicBool::new(false);

            // Register cleanup function exactly once using atexit
            if !REGISTERED.swap(true, Ordering::SeqCst) {
                extern "C" fn cleanup() {
                    unsafe {
                        // specDict_g is allocated when calling Indexes_Init()
                        if !ffi::specDict_g.is_null() {
                            ffi::RS_dictRelease(ffi::specDict_g);
                        }
                    }

                    unsafe {
                        // SchemaPrefixes_g is allocated when calling Indexes_Init()
                        if !ffi::SchemaPrefixes_g.is_null() {
                            ffi::SchemaPrefixes_Free(ffi::SchemaPrefixes_g);
                        }
                    }

                    unsafe {
                        // DefaultStopWordList is allocated when calling IndexSpec_ParseC()
                        ffi::StopWordList_FreeGlobals();
                    }
                }

                unsafe {
                    libc::atexit(cleanup);
                }
            }

            Self
        }

        #[cfg(not(target_os = "linux"))]
        fn new() -> Self {
            Self {}
        }
    }

    pub enum RevalidateIndexType {
        Numeric,
        Term,
    }

    /// Test the revalidation of the iterator.
    pub struct RevalidateTest<E> {
        #[allow(dead_code)]
        doc_ids: Vec<t_docId>,
        // FIXME: horrible hack so we can get a mutable reference to the InvertedIndex while holding an immutable one through the iterator.
        // We should get rid of it once we have designed a proper way to manage concurrent access to the II.
        pub ii: UnsafeCell<InvertedIndex<E>>,
        pub context: TestContext,
        _guard: GlobalGuard,
    }

    impl<E: Encoder + DecodedBy> RevalidateTest<E> {
        pub fn new(
            index_type: RevalidateIndexType,
            expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
            n_docs: u64,
        ) -> Self {
            let create_record = &*expected_record;
            // Generate a set of odd document IDs for testing, starting from 1.
            let doc_ids = (0..=n_docs)
                .map(|i| (2 * i + 1) as t_docId)
                .collect::<Vec<_>>();

            let (ii_flags, context) = match index_type {
                RevalidateIndexType::Numeric => {
                    (IndexFlags_Index_StoreNumeric, TestContext::numeric())
                }
                RevalidateIndexType::Term => (
                    IndexFlags_Index_StoreFreqs
                        | IndexFlags_Index_StoreTermOffsets
                        | IndexFlags_Index_StoreFieldFlags
                        | IndexFlags_Index_StoreByteOffsets,
                    TestContext::term(),
                ),
            };

            let mut ii = InvertedIndex::<E>::new(ii_flags);
            for doc_id in doc_ids.iter() {
                let record = create_record(*doc_id);
                ii.add_record(&record).expect("failed to add record");
            }

            Self {
                doc_ids,
                ii: UnsafeCell::new(ii),
                context,
                _guard: GlobalGuard::new(),
            }
        }

        /// test basic revalidation functionality - should return `RQEValidateStatus::Ok`` when index is valid
        pub fn revalidate_basic<'index, I>(&self, it: &mut I)
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
        pub fn revalidate_at_eof<'index, I>(&self, it: &mut I)
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

        /// Remove the document with the given id from the inverted index.
        pub fn remove_document(&self, doc_id: t_docId) {
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
        pub fn revalidate_after_document_deleted<'index, I>(&self, it: &mut I)
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
}

#[cfg(not(miri))]
pub use not_miri::{ExpirationTest, RevalidateIndexType, RevalidateTest};
