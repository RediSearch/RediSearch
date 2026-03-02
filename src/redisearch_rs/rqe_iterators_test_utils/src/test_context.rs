/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test context for creating search contexts with proper FFI setup.

use std::{
    ffi::CString,
    ptr::{self, NonNull},
    sync::{
        Mutex, Once,
        atomic::{AtomicU64, Ordering},
    },
};

use ffi::{IndexFlags, IndexFlags_Index_WideSchema, t_docId};

/// Global counter for generating unique index names across tests.
static INDEX_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Mutex to serialize TestContext creation and cleanup.
///
/// The C code's global state is not thread-safe. When tests run in parallel with `cargo test`,
/// concurrent TestContext creation or cleanup can corrupt this global state, causing segfaults
/// or panics. This mutex ensures only one TestContext is created or destroyed at a time.
static CONTEXT_MUTEX: Mutex<()> = Mutex::new(());

/// Ensures global C state initialization happens exactly once.
static INIT_ONCE: Once = Once::new();

/// Generate a unique index name to avoid conflicts when tests run in parallel.
fn unique_index_name(prefix: &str) -> String {
    let id = INDEX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}
use field::FieldMaskOrIndex;
use inverted_index::RSIndexResult;
use numeric_range_tree::{NumericIndex, NumericRangeTree};
use query_error::QueryError;

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

        // Initialize global C state exactly once to avoid corruption when
        // multiple TestContexts are created across parallel tests.
        INIT_ONCE.call_once(|| unsafe {
            ffi::Indexes_Init(ctx);
        });

        Self {
            ctx: ptr::NonNull::new(ctx).expect("Failed to create ModuleCtx"),
        }
    }

    const fn as_ptr(&self) -> *mut ffi::RedisModuleCtx {
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
        numeric_range_tree: ptr::NonNull<NumericRangeTree>,
    },
    Term {
        field_spec: ptr::NonNull<ffi::FieldSpec>,
        inverted_index: ptr::NonNull<ffi::InvertedIndex>,
    },
    Wildcard {
        inverted_index: ptr::NonNull<ffi::InvertedIndex>,
    },
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
    /// Create a new [`TestContext`] with a numeric inverted index having the given records.
    ///
    /// # Arguments
    /// * `records` - An iterator over the records to be indexed.
    /// * `multi` - Whether each record should be added twice.
    pub fn numeric<I>(records: I, multi: bool) -> Self
    where
        I: Iterator<Item = RSIndexResult<'static>>,
    {
        // Serialize TestContext creation to avoid concurrent access to C global state
        let _lock = CONTEXT_MUTEX.lock().unwrap();

        let ctx = ModuleCtx::new();
        // Create IndexSpec for NUMERIC field with unique name to avoid parallel test conflicts
        let index_name = unique_index_name("numeric_idx");
        let (spec, sctx) = create_spec_sctx(&ctx, "SCHEMA num_field NUMERIC", &index_name);

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
        let numeric_range_tree = unsafe {
            ffi::openNumericOrGeoIndex(spec.as_ptr(), fs.as_ptr(), true)
                as *mut numeric_range_tree::NumericRangeTree
        };
        let numeric_range_tree = unsafe {
            ptr::NonNull::new(numeric_range_tree)
                .expect("NumericRangeTree should not be null")
                .as_mut()
        };

        // Add numeric data to the range tree
        for record in records {
            let record_val = record.as_numeric().unwrap();
            numeric_range_tree.add(record.doc_id as t_docId, record_val, false, 0);

            if multi {
                numeric_range_tree.add(record.doc_id as t_docId, record_val, true, 0);
            }
        }

        Self {
            _ctx: ctx,
            sctx,
            spec,
            inner: TestContextInner::Numeric {
                field_spec: fs,
                numeric_range_tree: NonNull::from_mut(numeric_range_tree),
            },
        }
    }

    /// Create a new [`TestContext`] with a term inverted index having the given records.
    ///
    /// # Arguments
    /// * `flags` - The index flags to use for the inverted index. If `IndexFlags_Index_WideSchema`
    ///   is set, the spec will be created with MAXTEXTFIELDS option.
    /// * `records` - An iterator over the records to be indexed.
    /// * `multi` - Whether each record should be added twice.
    pub fn term<I>(flags: IndexFlags, records: I, multi: bool) -> Self
    where
        I: Iterator<Item = RSIndexResult<'static>>,
    {
        // Serialize TestContext creation to avoid concurrent access to C global state
        let _lock = CONTEXT_MUTEX.lock().unwrap();

        let ctx = ModuleCtx::new();

        // Use MAXTEXTFIELDS option if wide schema is requested
        let schema = if (flags & IndexFlags_Index_WideSchema) != 0 {
            "MAXTEXTFIELDS SCHEMA text_field TEXT"
        } else {
            "SCHEMA text_field TEXT"
        };
        // Use unique index name to avoid conflicts when tests run in parallel
        let index_name = unique_index_name("term_idx");
        let (spec, sctx) = create_spec_sctx(&ctx, schema, &index_name);

        // Get the field spec for the text field
        let field_name = CString::new("text_field").unwrap();
        let fs = unsafe {
            ffi::IndexSpec_GetFieldWithLength(
                spec.as_ptr(),
                field_name.as_ptr(),
                field_name.as_bytes().len(),
            )
        };
        let field_spec = ptr::NonNull::new(fs as _).expect("FieldSpec should not be null");

        // Get the term inverted index from the spec using Redis_OpenInvertedIndex.
        // This creates the index and adds it to the spec's keysDict properly.
        let term = CString::new("term").unwrap();
        let mut is_new = false;
        let inverted_index = unsafe {
            ffi::Redis_OpenInvertedIndex(
                sctx.as_ptr(),
                term.as_ptr(),
                term.as_bytes().len(),
                true, // write mode
                &mut is_new,
            )
        };
        let inverted_index =
            ptr::NonNull::new(inverted_index).expect("InvertedIndex should not be null");

        // Populate with the records
        for record in records {
            Self::write_forward_index_entry(inverted_index.as_ptr(), &record);
            if multi {
                Self::write_forward_index_entry(inverted_index.as_ptr(), &record);
            }
        }

        Self {
            _ctx: ctx,
            sctx,
            spec,
            inner: TestContextInner::Term {
                field_spec,
                inverted_index,
            },
        }
    }

    /// Create a new [`TestContext`] with a doc-ids-only inverted index for wildcard queries.
    ///
    /// # Arguments
    /// * `doc_ids` - An iterator over the document IDs to be indexed.
    pub fn wildcard<I>(doc_ids: I) -> Self
    where
        I: Iterator<Item = u64>,
    {
        // Serialize TestContext creation to avoid concurrent access to C global state
        let _lock = CONTEXT_MUTEX.lock().unwrap();

        let ctx = ModuleCtx::new();
        // Create IndexSpec with unique name to avoid parallel test conflicts
        let index_name = unique_index_name("wildcard_idx");
        let (mut spec, sctx) = create_spec_sctx(&ctx, "SCHEMA text_field TEXT", &index_name);

        let mut memsize = 0;
        let ii_ptr = inverted_index_ffi::NewInvertedIndex_Ex(
            ffi::IndexFlags_Index_DocIdsOnly,
            false,
            false,
            &mut memsize,
        );
        let ii = ptr::NonNull::new(ii_ptr.cast()).expect("Failed to create InvertedIndex");

        // Populate with virtual records for each document ID
        for doc_id in doc_ids {
            let record = RSIndexResult::virt().doc_id(doc_id);
            // SAFETY: ii is a valid pointer created via NewInvertedIndex_Ex
            unsafe {
                inverted_index_ffi::InvertedIndex_WriteEntryGeneric(
                    ii_ptr,
                    &record as *const _ as *mut _,
                );
            }
        }

        // Set spec.existingDocs so Wildcard::should_abort() can find the index
        // during revalidation (it compares spec.existingDocs with the reader's index).
        unsafe {
            spec.as_mut().existingDocs = ii_ptr.cast();
        }

        Self {
            _ctx: ctx,
            sctx,
            spec,
            inner: TestContextInner::Wildcard { inverted_index: ii },
        }
    }

    /// Write a record to an inverted index using the ForwardIndexEntry FFI.
    fn write_forward_index_entry(idx: *mut ffi::InvertedIndex, record: &RSIndexResult) {
        let term = CString::new("term").unwrap();

        // Create VarintVectorWriter for offsets
        let vw = varint_ffi::NewVarintVectorWriter(16);
        let vw_nonnull = ptr::NonNull::new(vw).expect("VectorWriter should not be null");

        // Write offset data - write 10 offset values [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        // to match what the tests expect
        for i in 0..10u32 {
            varint_ffi::VVW_Write(Some(vw_nonnull), i);
        }

        // Create ForwardIndexEntry
        let mut entry = ffi::ForwardIndexEntry {
            next: ptr::null_mut(),
            docId: record.doc_id,
            freq: record.freq,
            __bindgen_padding_0: 0,
            fieldMask: record.field_mask,
            term: term.as_ptr(),
            len: term.as_bytes().len() as u32,
            hash: 0,
            vw: vw.cast(), // Cast varint::VectorWriter* to ffi::VarintVectorWriter*
        };

        // Write the entry to the inverted index
        unsafe {
            ffi::InvertedIndex_WriteForwardIndexEntry(idx, &mut entry);
        }

        varint_ffi::VVW_Free(Some(vw_nonnull));
    }

    /// Get the numeric range tree for this context.
    /// Panics if this is not a numeric context.
    pub fn numeric_range_tree(&self) -> ptr::NonNull<numeric_range_tree::NumericRangeTree> {
        match self.inner {
            TestContextInner::Numeric {
                numeric_range_tree, ..
            } => numeric_range_tree,
            _ => panic!("TestContext is not a Numeric context"),
        }
    }

    /// Get a reference to the numeric range tree for this context.
    /// Panics if this is not a numeric context.
    pub fn numeric_range_tree_ref(&self) -> &numeric_range_tree::NumericRangeTree {
        unsafe { self.numeric_range_tree().as_ref() }
    }

    /// Get a mutable reference to the numeric range tree for this context.
    /// Panics if this is not a numeric context.
    #[allow(clippy::mut_from_ref)]
    pub fn numeric_range_tree_mut(&self) -> &mut numeric_range_tree::NumericRangeTree {
        unsafe { self.numeric_range_tree().as_mut() }
    }

    /// Get the field spec for this context.
    /// Panics if this is a Wildcard context (which has no field spec).
    pub const fn field_spec(&self) -> &ffi::FieldSpec {
        match self.inner {
            TestContextInner::Numeric { field_spec, .. }
            | TestContextInner::Term { field_spec, .. } => unsafe { field_spec.as_ref() },
            TestContextInner::Wildcard { .. } => panic!("Wildcard context has no field spec"),
        }
    }

    /// Get the term inverted index for this context (non-wide schema).
    /// Panics if this is not a term context or if it uses wide schema.
    pub fn term_inverted_index(
        &self,
    ) -> &inverted_index::FieldMaskTrackingIndex<inverted_index::full::Full> {
        match &self.inner {
            TestContextInner::Term { inverted_index, .. } => {
                // SAFETY: inverted_index is a valid pointer created via Redis_OpenInvertedIndex
                // and the FFI InvertedIndex type is a repr(C) enum that wraps the same data.
                let ii: *const inverted_index_ffi::InvertedIndex = inverted_index.as_ptr().cast();
                use inverted_index::{full::Full, opaque::OpaqueEncoding};
                Full::from_opaque(unsafe { &*ii })
            }
            _ => panic!("TestContext is not a Term context"),
        }
    }

    /// Get a mutable reference to the opaque term inverted index for this context.
    /// Panics if this is not a term context.
    #[allow(clippy::mut_from_ref)] // need to get a mut for the revalidate_after_document_deleted test
    pub fn term_inverted_index_mut(&self) -> &mut inverted_index_ffi::InvertedIndex {
        match &self.inner {
            TestContextInner::Term { inverted_index, .. } => {
                // SAFETY: inverted_index is a valid pointer created via Redis_OpenInvertedIndex
                // and the FFI InvertedIndex type is a repr(C) enum that wraps the same data.
                let ii: *mut inverted_index_ffi::InvertedIndex = inverted_index.as_ptr().cast();
                unsafe { &mut *ii }
            }
            _ => panic!("TestContext is not a Term context"),
        }
    }

    /// Get the term inverted index for this context (wide schema).
    /// Panics if this is not a term context or if it doesn't use wide schema.
    pub fn term_inverted_index_wide(
        &self,
    ) -> &inverted_index::FieldMaskTrackingIndex<inverted_index::full::FullWide> {
        match &self.inner {
            TestContextInner::Term { inverted_index, .. } => {
                // SAFETY: inverted_index is a valid pointer created via Redis_OpenInvertedIndex
                // and the FFI InvertedIndex type is a repr(C) enum that wraps the same data.
                let ii: *const inverted_index_ffi::InvertedIndex = inverted_index.as_ptr().cast();
                use inverted_index::{full::FullWide, opaque::OpaqueEncoding};
                FullWide::from_opaque(unsafe { &*ii })
            }
            _ => panic!("TestContext is not a Term context"),
        }
    }

    /// Returns the bitmask for the test's full-text field.
    ///
    /// The `ftId` is the full-text field ID, distinct from the general `index` field.
    /// Use this when filtering term records by field or marking field expiration.
    pub const fn text_field_bit(&self) -> ffi::t_fieldMask {
        1 << self.field_spec().ftId
    }

    /// Get the wildcard (doc-ids-only) inverted index for this context.
    /// Returns a reference to the FFI inverted index wrapper.
    /// Panics if this is not a wildcard context.
    #[allow(clippy::mut_from_ref)] // need to get a mut for the revalidate_after_document_deleted test
    pub fn wildcard_inverted_index(&self) -> &mut inverted_index_ffi::InvertedIndex {
        match &self.inner {
            TestContextInner::Wildcard { inverted_index } => {
                // SAFETY: inverted_index is a valid pointer created via NewInvertedIndex_Ex
                let ii: *mut inverted_index_ffi::InvertedIndex = inverted_index.as_ptr().cast();
                unsafe { &mut *ii }
            }
            _ => panic!("TestContext is not a Wildcard context"),
        }
    }

    /// Get a raw pointer to the wildcard inverted index suitable for FFI.
    /// Panics if this is not a wildcard context.
    pub fn wildcard_index_ptr(&self) -> *const ffi::InvertedIndex {
        match &self.inner {
            TestContextInner::Wildcard { inverted_index } => inverted_index.as_ptr(),
            _ => panic!("TestContext is not a Wildcard context"),
        }
    }

    /// Get the ffi inverted index for this context.
    #[allow(clippy::mut_from_ref)] // need to get a mut for the revalidate_after_document_deleted test
    pub fn numeric_inverted_index(&self) -> &mut NumericIndex {
        let tree = self.numeric_range_tree_mut();
        let index = tree
            .indexed_iter()
            .find_map(|(index, node)| {
                if node.has_range() && node.is_leaf() {
                    Some(index)
                } else {
                    None
                }
            })
            .expect("There must be at least one leaf!");
        tree.node_mut(index).range_mut().unwrap().entries_mut()
    }

    /// Initialize the TTL table if not already initialized.
    fn verify_ttl_init(&mut self) {
        // SAFETY: self.spec is a valid pointer created via IndexSpec_ParseC.
        unsafe {
            let spec = self.spec.as_mut();
            // Enable expiration monitoring (required for expiration checks to work)
            spec.monitorDocumentExpiration = true;
            spec.monitorFieldExpiration = true;
            ffi::TimeToLiveTable_VerifyInit(&mut spec.docs.ttl);
        }
    }

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
                // Single field by index
                let fe = unsafe {
                    ffi::array_new_sz(std::mem::size_of::<FieldExpiration>() as u16, 0, 1)
                };
                let fe = fe.cast::<FieldExpiration>();
                unsafe {
                    *fe = FieldExpiration {
                        index,
                        point: expiration,
                    };
                }
                fe
            }
            FieldMaskOrIndex::Mask(mask) => {
                // Multiple fields by mask - count bits to determine array size
                let count = mask.count_ones();
                let fe = unsafe {
                    ffi::array_new_sz(std::mem::size_of::<FieldExpiration>() as u16, 0, count)
                };
                let fe = fe.cast::<FieldExpiration>();

                // Add a FieldExpiration for each bit set in the mask
                let mut value = mask;
                let mut i = 0isize;
                while value != 0 {
                    let index = value.trailing_zeros();
                    unsafe {
                        *fe.offset(i) = FieldExpiration {
                            index: index as u16,
                            point: expiration,
                        };
                    }
                    value &= value - 1;
                    i += 1;
                }
                fe
            }
        };

        // Document expiration time set to far future (we only care about field expiration)
        let doc_expiration_time = ffi::t_expirationTimePoint {
            tv_sec: i64::MAX,
            tv_nsec: i64::MAX,
        };

        // SAFETY: self.spec is valid, TTL table is initialized, fe is a valid array
        unsafe {
            ffi::TimeToLiveTable_Add(
                self.spec.as_ref().docs.ttl,
                doc_id,
                doc_expiration_time,
                fe as _,
            );
        }
    }

    /// Mark the given field of the given documents as expired.
    ///
    /// Sets the field expiration time to the past and the current query time
    /// to the future, so expiration checks will consider these fields expired.
    pub fn mark_index_expired(&mut self, ids: Vec<t_docId>, field: FieldMaskOrIndex) {
        // Expiration time in the past
        let expiration = ffi::t_expirationTimePoint {
            tv_sec: 1,
            tv_nsec: 1,
        };

        for id in ids {
            self.ttl_add(id, field, expiration);
        }

        // Set the current time to the future so expiration checks see these as expired
        // SAFETY: self.sctx is a valid pointer created via NewSearchCtxC
        unsafe {
            self.sctx.as_mut().time.current = ffi::t_expirationTimePoint {
                tv_sec: 100,
                tv_nsec: 100,
            };
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Serialize cleanup to avoid concurrent access to C global state.
        // This matches the lock acquired during creation.
        let _lock = CONTEXT_MUTEX.lock().unwrap();

        // Note: the wildcard inverted index is freed by IndexSpec_RemoveFromGlobals
        // below, via spec->existingDocs. No explicit free needed here.

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
pub struct GlobalGuard;

impl Default for GlobalGuard {
    // atexit() is only available on Linux.
    // This means means global resources are not cleaned up on non-Linux platforms.
    // It's not that bad as those are tests and the real goal here is to detect actual memory leaks
    // using Valgrind which is only available on Linux as well.
    #[cfg(target_os = "linux")]
    fn default() -> Self {
        use std::sync::atomic::{AtomicBool, Ordering};

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
    fn default() -> Self {
        Self {}
    }
}
