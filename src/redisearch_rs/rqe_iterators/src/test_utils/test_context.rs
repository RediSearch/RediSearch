/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test context for creating search contexts with proper FFI setup.

use std::{ffi::CString, ptr};

use ffi::t_docId;
use inverted_index::{NumericFilter, RSIndexResult};
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
    /// Create a new [`TestContext`] with a numeric inverted index having the given records
    pub fn numeric<I>(records: I) -> Self
    where
        I: Iterator<Item = RSIndexResult<'static>>,
    {
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

        // Add numeric data to the range tree
        for record in records {
            let record_val = record.as_numeric().unwrap();
            unsafe {
                ffi::NumericRangeTree_Add(
                    numeric_range_tree.as_ptr(),
                    record.doc_id as t_docId,
                    record_val,
                    0,
                );
            }
        }

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

    /// Create a new [`TestContext`] with a term inverted index.
    pub fn term() -> Self {
        let ctx = ModuleCtx::new();
        let (spec, sctx) = create_spec_sctx(&ctx, "SCHEMA text_field TEXT", "term_idx");

        Self {
            _ctx: ctx,
            sctx,
            spec,
            inner: TestContextInner::Term,
        }
    }

    /// Get the numeric range tree for this context.
    /// Panics if this is not a numeric context.
    pub fn numeric_range_tree(&self) -> ptr::NonNull<ffi::NumericRangeTree> {
        match self.inner {
            TestContextInner::Numeric {
                numeric_range_tree, ..
            } => numeric_range_tree,
            _ => panic!("TestContext is not a Numeric context"),
        }
    }

    /// Get the field spec for this context.
    /// Panics if this is not a numeric context.
    pub fn field_spec(&self) -> &ffi::FieldSpec {
        match self.inner {
            TestContextInner::Numeric { field_spec, .. } => unsafe { field_spec.as_ref() },
            _ => panic!("TestContext is not a Numeric context"),
        }
    }

    /// Get the ffi inverted index for this context.
    pub fn numeric_inverted_index(&self) -> &mut inverted_index_ffi::InvertedIndex {
        // Create a numeric filter to find ranges
        let mut filter = NumericFilter::default();
        filter.ascending = false;
        filter.field_spec = self.field_spec();

        // Find a range that covers our data to get the inverted index
        let ranges = unsafe {
            ffi::NumericRangeTree_Find(
                self.numeric_range_tree().as_ptr(),
                // cast inverted_index::NumericFilter to ffi::NumericFilter
                &filter as *const _ as *const ffi::NumericFilter,
            )
        };
        assert!(!ranges.is_null());
        unsafe {
            assert!(ffi::Vector_Size(ranges) > 0);
        }
        let mut range: *mut ffi::NumericRange = std::ptr::null_mut();
        unsafe {
            let range_out = &mut range as *mut *mut ffi::NumericRange;
            assert!(ffi::Vector_Get(ranges, 0, range_out.cast()) == 1);
        }
        assert!(!range.is_null());
        let range = unsafe { &*range };
        let ii = range.entries;
        assert!(!ii.is_null());
        let ii: *mut inverted_index_ffi::InvertedIndex = ii.cast();
        let ii = unsafe { &mut *ii };

        unsafe {
            ffi::Vector_Free(ranges);
        }

        ii
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
pub struct GlobalGuard;

impl GlobalGuard {
    // atexit() is only available on Linux.
    // This means means global resources are not cleaned up on non-Linux platforms.
    // It's not that bad as those are tests and the real goal here is to detect actual memory leaks
    // using Valgrind which is only available on Linux as well.
    #[cfg(target_os = "linux")]
    pub fn new() -> Self {
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
    pub fn new() -> Self {
        Self {}
    }
}
