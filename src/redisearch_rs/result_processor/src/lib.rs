/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Result processors transform entries retrieved from the inverted index when fulfilling
//! a user-provided query (e.g. scoring, filtering, sorting, paginating, etc.).
//!
//! Result processors form a chain, assembled by the query planner.
//! Processing is lazy: when the last processor in the chain is asked to yield a result, it
//! will in turn ask for entries from the previous processor, recursively until it reached
//! the beginning of the chain.
//! At the head of the chain, you will always find an index iterator, yielding entries from
//! the database indexes.

pub mod counter;
/// Test harness (`Chain`, `from_iter`, `ResultRP`) for driving result processors in pure Rust.
///
/// Exposed behind the `test-utils` feature so other crates (e.g. `redisearch_disk`) can build
/// their own result processors against the real [`Context`]/[`Upstream`] machinery in tests,
/// since [`Context`] has no public constructor. Always available to this crate's own tests.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use pin_project::pin_project;
use search_result::SearchResult;
#[cfg(debug_assertions)]
use std::any::{TypeId, type_name};
use std::{
    ffi::c_int,
    marker::{PhantomData, PhantomPinned},
    pin::Pin,
    ptr::{self, NonNull},
};

/// Errors that can be returned by [`ResultProcessor`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Error {
    /// Execution halted because of timeout
    TimedOut,
    /// Aborted because of error. The QueryState (parent->status) should have
    /// more information.
    Error,
}

/// Terminal recovery failure; the owning pipeline retains the diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainError;

/// Implemented by types that participate in the result processor chain.
///
/// # Search Result
///
/// The search result storage is allocated by the caller of the result processor chain.
/// The search result is populated by the very first result processor and a mutable reference
/// to it is passed down from processor to processor. The `ResultProcessor::next` method receives
/// this reference as the second argument. Calling upstream processors through `Upstream::next`
/// likewise requires a mutable reference to a search result.
///
/// # Example
///
/// ```rust
/// # use result_processor::{ResultProcessor, Error, Context};
/// # use search_result::SearchResult;
/// #
/// # // Link both Rust-provided and C-provided symbols
/// # extern crate redisearch_rs;
/// # // Mock or stub the ones that aren't provided by the line above
/// # redis_mock::mock_or_stub_missing_redis_c_symbols!();
///
/// /// A simple result processor that simply prints out the search result received from the previous processor
/// /// before passing it on.
/// struct Logger;
///
/// impl ResultProcessor for Logger {
///    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType::MAX;
///
///     fn next(&mut self, mut cx: Context, res: &mut SearchResult<'_>) -> Result<Option<()>, Error> {
///         let mut upstream = cx
///             .upstream()
///             .expect("There is no processor upstream of this counter.");
///
///         while upstream.next(res)?.is_some() {
///             eprintln!("{res:?}");
///         }
///
///         Ok(None)
///     }
/// }
/// ```
pub trait ResultProcessor {
    /// The type of this result processor.
    const TYPE: ffi::ResultProcessorType;

    /// Pull the next [`ffi::SearchResult`] from this result processor into the provided `res` location.
    ///
    /// Calling this method should return `Ok(Some(ffi::SearchResult))` as long as there are search results,
    /// and once they’ve all been exhausted, will return `Ok(None)` to indicate that iteration is finished.
    ///
    /// For exceptional error cases, this method should return `Err(Error)`.
    ///
    /// `Ok(None)` and errors end iteration. Blocking waits are internal to the
    /// execution call; denied ownership readmission propagates [`Error::TimedOut`].
    fn next(&mut self, cx: Context, res: &mut SearchResult) -> Result<Option<()>, Error>;

    /// Recover already-available output under exclusive pipeline access.
    ///
    /// Uses [`Self::next`]'s result ownership conventions. The caller stops after
    /// exhaustion or [`DrainError`]. The default is a recovery barrier; it does
    /// not invoke [`Self::next`] or any upstream processor.
    fn drain(
        &mut self,
        _cx: DrainContext,
        _res: &mut SearchResult,
    ) -> Result<Option<()>, DrainError> {
        Ok(None)
    }
}

/// Exclusive recovery access, restricted to upstream [`ResultProcessor::drain`].
///
/// Created only by the admitted FFI entry. Reborrowing prevents simultaneous
/// parent/upstream access. It carries no synchronization operation of its own.
///
/// ```compile_fail
/// use result_processor::DrainContext;
/// use search_result::SearchResult;
/// fn restart(mut cx: DrainContext, row: &mut SearchResult) {
///     cx.upstream().unwrap().next(row);
/// }
/// ```
pub struct DrainContext<'a>(Context<'a>);

impl DrainContext<'_> {
    /// Borrow the previous processor for recovery, if present.
    pub fn upstream(&mut self) -> Option<DrainUpstream<'_>> {
        self.0.upstream().map(DrainUpstream)
    }

    /// Borrow the owning [`ffi::QueryProcessingCtx`] until the next reborrow.
    pub const fn parent(&mut self) -> Option<&ffi::QueryProcessingCtx> {
        self.0.parent()
    }

    /// Account for a discarded recovered row using [`Context::subtract_total_results`].
    pub fn subtract_total_results(&mut self, n: u32) {
        self.0.subtract_total_results(n);
    }
}

/// An upstream recovery borrow with no execution entry point.
pub struct DrainUpstream<'a>(Upstream<'a>);

impl DrainUpstream<'_> {
    /// Recover a row through the upstream [`ResultProcessor::drain`] callback.
    pub fn drain(&mut self, res: &mut SearchResult<'_>) -> Result<Option<()>, DrainError> {
        // SAFETY: every upstream contains the C prefix, but only Rust processors
        // contain Header's debug suffix. Project the field without borrowing that suffix.
        let drain = unsafe { (*self.0.ptr.as_ptr()).drain }
            .expect("result processor `Drain` vtable function was null");
        // SAFETY: the reborrow excludes another caller and res is initialized and exclusive.
        match unsafe { drain(self.0.ptr.as_ptr(), res) } {
            ffi::RPDrainStatus_RP_DRAIN_OK => Ok(Some(())),
            ffi::RPDrainStatus_RP_DRAIN_EOF => Ok(None),
            ffi::RPDrainStatus_RP_DRAIN_ERROR => Err(DrainError),
            code => panic!("result processor returned unknown drain code {code}"),
        }
    }
}

/// This type allows result processors to access its context (the owning QueryIterator, upstream result processors, etc.)
pub struct Context<'a> {
    ptr: NonNull<Header>,
    _borrow: PhantomData<&'a mut Header>,
}

impl Context<'_> {
    /// Create a new context for calling the given type-erased result processor
    pub(crate) fn new(header: Pin<&mut Header>) -> Self {
        // Safety: Context & Upstream correctly treat the pointer as pinned
        let ptr = unsafe { NonNull::from(Pin::into_inner_unchecked(header)) };

        Self {
            ptr,
            _borrow: PhantomData,
        }
    }

    /// The previous result processor in the pipeline if present.
    ///
    /// Returns `None` when the result processor has no upstream.
    pub fn upstream(&mut self) -> Option<Upstream<'_>> {
        // Safety: We have to trust that the upstream pointer set by our QueryIterator parent
        // is correct.
        let upstream = NonNull::new(unsafe { self.ptr.as_ref().upstream })?;

        Some(Upstream {
            ptr: upstream,
            _borrow: PhantomData,
        })
    }

    /// Returns the owning [`ffi::QueryProcessingCtx`] of the pipeline.
    pub const fn parent(&mut self) -> Option<&ffi::QueryProcessingCtx> {
        // Safety: We trust that this result processor's pointer is valid.
        let query_processing_context_ptr = unsafe { self.ptr.as_ref() }.parent;

        // Safety: We trust that the pointer to the parent context, if set, is
        // set to an appropriate structure.
        unsafe { query_processing_context_ptr.as_ref() }
    }

    /// Decrease the pipeline's running total result count by `n`.
    ///
    /// A result is counted (`totalResults++`) by the index processor as soon as it is produced
    /// upstream (`setSearchResult`, `result_processor.c`). A processor that later drops a result it
    /// received must subtract it back, exactly as the C scorer does on `RS_SCORE_FILTEROUT`
    /// (`result_processor.c`'s `totalResults--`). The disk async loader uses this when it skips a
    /// document (non-resident key, or a docid that changed under the query).
    ///
    /// No-op when the processor has no parent.
    pub fn subtract_total_results(&mut self, n: u32) {
        // Safety: the parent pointer, if non-null, is the `QueryProcessingCtx` the C pipeline
        // installed on this result processor; it outlives the processor, the chain runs on a single
        // thread, and the C pipeline mutates this same field. Provenance comes from the raw pointer
        // C provided, so writing through it is sound.
        let parent = unsafe { self.ptr.as_ref() }.parent.cast_mut();
        if !parent.is_null() {
            // SAFETY: the context exclusively borrows this counter. Project only the field:
            // the pinned parent may also own the pointer to the currently borrowed chain.
            let total = unsafe { &mut (*parent).totalResults };
            *total = total.saturating_sub(n);
        }
    }
}

/// The previous result processor in the pipeline.
#[derive(Debug)]
pub struct Upstream<'a> {
    ptr: NonNull<Header>,
    _borrow: PhantomData<&'a mut Header>,
}

impl Upstream<'_> {
    pub const fn ty(&self) -> ffi::ResultProcessorType {
        // SAFETY: this field is in the common C prefix of the live upstream allocation.
        unsafe { (*self.ptr.as_ptr()).ty }
    }

    /// Pull the next [`ffi::SearchResult`] from this result processor into the provided `res` location.
    ///
    /// Returns `Ok(Some(()))` if a search result was successfully pulled from the processor
    /// and `Ok(None)` to indicate the end of search results has been reached.
    ///
    /// # Errors
    ///
    /// Returns errors according to [`ResultProcessor::next`].
    pub fn next(&mut self, res: &mut SearchResult<'_>) -> Result<Option<()>, Error> {
        // SAFETY: the live upstream has the common C prefix. Do not create a
        // reference to Header's Rust-only debug suffix for a C processor.
        let next = unsafe { (*self.ptr.as_ptr()).next }
            .expect("result processor `Next` vtable function was null");

        // Safety: At the end of the day we're calling to arbitrary code at this point... But provided
        // the QueryIterator and other result processors are implemented correctly, this should be safe.
        let ret_code = unsafe { next(self.ptr.as_ptr(), res) };

        match ret_code as ffi::RPStatus {
            ffi::RPStatus_RS_RESULT_OK => Ok(Some(())),
            ffi::RPStatus_RS_RESULT_EOF => Ok(None),
            ffi::RPStatus_RS_RESULT_PAUSED => {
                unimplemented!("result processor returned unsupported error code PAUSED")
            }
            ffi::RPStatus_RS_RESULT_TIMEDOUT => Err(Error::TimedOut),
            ffi::RPStatus_RS_RESULT_ERROR => Err(Error::Error),
            code => {
                unimplemented!("result processor returned unknown error code {code}")
            }
        }
    }
}

/// Properties of Result Processors that are accessed by C code through FFI. This type is named `Header` because it
/// must be the first member of the [`ResultProcessor`] struct in order to guarantee that we can cast a `*mut ResultProcessor<P>`
/// to a `*mut Header` (which is what the C code expects to receive).
///
/// Duplicates [`ffi::ResultProcessor`] to add pinning-related safety features.
///
/// # Safety
///
/// Result processors intrusively linked, where one result processor has the pointer to the
/// previous (forming an intrusively singly-linked list). This places a big safety invariant on all code
/// that touches this data structure: Result processors must *never* be moved while part of a QueryIterator
/// chain. Accidentally moving a processor will create a broken link and undefined behaviour.
///
/// This invariant is implicit in the C code where its uncommon to move the heap allocated objects (which would
/// mean memcopying them around); In Rust we need to explicitly manage this invariant however, as move semantics make
/// it easy to break accidentally. The compiler is free to move values as it sees fit (Rust calls this trivially moveable).
/// As a result a Rust reference (&T or &mut T) is a stable "handle" to a value but a pointer (*const T or *mut T) is not.
///
/// For intrusive data types like this we need to tell the compiler "don't move this please I have pointers to it" which is called
/// pinning in Rust.
///
/// We wrap a reference in the `Pin<T>` type (Pin<&mut T>) which disallows moving the pointee from its location in memory.
/// Crucially though, the way Pin disallows is not magic, it simply doesn't implement any methods and traits that would
/// allow a caller to move the value. Unfortunately this means banning all mutable access to the value T (you cannot get a
/// &mut T from a Pin<&mut T> for example) since with a &mut T you can always move the value very easily (via mem::replace for example).
///
/// So Rust has another technique that lets you treat memory locations a "pinned" while still being able to mutate
/// them through "pin projections" essentially a "proxy type" that behaves like a regular Rust type (including allowing
/// moving & mutation) but will forward all accesses and mutations to the backing, actually pinned type
/// (this is what the `#[pin_project]` below does!)
///
/// For details refer to the [`Pin`] documentation which explains the concept of "pinning" a Rust value in memory and its implications.
#[pin_project]
#[repr(C)]
#[derive(Debug)]
struct Header {
    /// Reference to the parent QueryProcessingCtx that owns this result processor
    parent: *const ffi::QueryProcessingCtx,
    /// Previous result processor in the chain
    upstream: *mut Header,
    /// Type of result processor
    ty: ffi::ResultProcessorType,
    rp_gil_time: ffi::rs_wall_clock_ns_t,
    /// "VTable" function. Pulls [`ffi::SearchResult`]s out of this result processor.
    ///
    /// Populates the result pointed to by `res`. The existing data of `res` is
    /// not read, so it is the responsibility of the caller to ensure that there
    /// are no refcount leaks in the structure.
    ///
    /// Users can use [`search_result::SearchResult::clear`] to reset the structure without freeing it.
    ///
    /// The populated structure (if [`ffi::RPStatus_RS_RESULT_OK`] is returned) does contain references
    /// to document data. Callers *MUST* ensure they are eventually freed.
    next: Option<unsafe extern "C" fn(self_: *mut Header, res: *mut SearchResult) -> c_int>,
    /// "VTable" function. Frees the processor and any internal data related to it.
    free: Option<unsafe extern "C" fn(self_: *mut Header)>,

    /// Exclusive recovery entry matching [`ffi::ResultProcessor::Drain`].
    drain: Option<unsafe extern "C" fn(*mut Header, *mut SearchResult) -> ffi::RPDrainStatus>,

    // the following fields are Rust-specific and do not map to the C (ffi::ResultProcessor) type
    /// The TypeId of the inner ResultProcessor implementation, for debugging purposes
    #[cfg(debug_assertions)]
    inner_ty_id: TypeId,
    /// The Rust typename of the inner ResultProcessor implementation, for debugging purposes
    #[cfg(debug_assertions)]
    inner_ty_name: &'static str,

    /// ResultProcessor *must* be !Unpin to ensure they can never be moved, and they never receive
    /// LLVM `noalias` annotations; See <https://github.com/rust-lang/rust/issues/63818>.
    /// FIXME: Remove once <https://github.com/rust-lang/rust/issues/63818> is closed and replace with the recommended fix.
    _unpin: PhantomPinned,
}

/// Wrapper for [`ResultProcessor`] implementations performing required FFI translations
/// so result processors written in Rust can be used by the rest of the C codebase.
#[pin_project]
#[derive(Debug)]
#[repr(C)]
pub struct ResultProcessorWrapper<P> {
    #[pin]
    header: Header,
    result_processor: P,
}

impl<P> ResultProcessorWrapper<P>
where
    P: ResultProcessor + 'static,
{
    /// Construct a new FFI-ResultProcessor from the provided [`crate::ResultProcessor`] implementer.
    pub fn new(result_processor: P) -> Self {
        Self {
            header: Header {
                parent: ptr::null_mut(), // will be set by `QITR_PushRP` when inserting this result processor into the chain
                upstream: ptr::null_mut(), // will be set by `QITR_PushRP` when inserting this result processor into the chain
                ty: P::TYPE,
                rp_gil_time: 0,
                next: Some(Self::result_processor_next),
                free: Some(Self::result_processor_free),
                drain: Some(Self::result_processor_drain),
                #[cfg(debug_assertions)]
                inner_ty_id: TypeId::of::<P>(),
                #[cfg(debug_assertions)]
                inner_ty_name: type_name::<P>(),
                _unpin: PhantomPinned,
            },
            result_processor,
        }
    }

    /// Converts a heap-allocated `ResultProcessor` into a raw pointer.
    ///
    /// The caller is responsible for the memory previously managed by the `Box`, in particular
    /// the caller should properly destroy the `ResultProcessor` and deallocate the memory by calling
    /// `Self::from_ptr`.
    ///
    /// # Safety
    ///
    /// The caller *must* continue to treat the pointer as pinned.
    #[inline]
    pub unsafe fn into_ptr(me: Pin<Box<Self>>) -> NonNull<Self> {
        // This function must be kept in sync with `Self::from_ptr` below.

        // Safety: The caller promised to continue to treat the returned pointer
        // as pinned and never move out of it.
        let ptr = Box::into_raw(unsafe { Pin::into_inner_unchecked(me) });

        // Safety: we know the ptr we get from Box::into_raw is never null
        unsafe { NonNull::new_unchecked(ptr) }
    }

    /// Reborrows the concrete [`ResultProcessor`] implementation `P` behind a raw
    /// `ffi::ResultProcessor` pointer previously produced by [`Self::into_ptr`].
    ///
    /// Lets an FFI entry point that only has the raw `rp` pointer reach into the concrete
    /// Rust-side state, without going through the `next`/`free` VTable.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must have come from [`Self::into_ptr`] for this exact `P` and not yet been freed.
    /// 2. The returned reference must not be used to move out of the wrapped `result_processor`.
    /// 3. No `&mut` to the pointee may exist or be created concurrently for lifetime `'a`.
    #[inline]
    pub unsafe fn inner_from_raw<'a>(ptr: NonNull<ffi::ResultProcessor>) -> &'a P {
        let ptr = ptr.cast::<Self>();
        debug_assert!(ptr.is_aligned());
        // Safety: invariant 1, upheld by the caller.
        unsafe { Self::debug_assert_same_type(ptr.cast()) };
        // Safety: invariants 1 and 2, upheld by the caller.
        unsafe { &(*ptr.as_ptr()).result_processor }
    }

    /// Constructs a `Box<ResultProcessor>` from a raw pointer.
    ///
    /// The returned `Box` will own the raw pointer, in particular dropping the `Box`
    /// will deallocate the `ResultProcessor`. This function should only be used by the [`Self::result_processor_free`].
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure the pointer was previously created through [`Self::into_ptr`].
    /// 2. The caller has to be careful to never call this method twice for the same pointer, otherwise a
    ///    double-free or other memory corruptions will occur.
    /// 3. The caller *must* also ensure that `ptr` continues to be treated as pinned.
    #[inline]
    unsafe fn from_ptr(ptr: NonNull<Self>) -> Pin<Box<Self>> {
        // This function must be kept in sync with `Self::into_ptr` above.

        // Safety:
        // 1 -> This function will only ever be called through the `result_processor_free` vtable method below.
        //      We therefore know - through construction - that the pointer was previously created through `into_ptr`.
        // 2 -> Has to be upheld by the caller
        let b = unsafe { Box::from_raw(ptr.as_ptr()) };
        // Safety: 3 -> Caller has to uphold the pin contract
        unsafe { Pin::new_unchecked(b) }
    }

    /// VTable function exposing the [`ResultProcessor::next`] method. This is exposed through the `next` field of [`Header`].
    ///
    /// # Safety
    ///
    /// The caller (C code) must uphold the following safety invariants:
    /// 1. `ptr` must be a non-null, well-aligned, valid pointer to a result processor (struct [`Header`]).
    /// 2. `res` must be a non-null, well-aligned, valid pointer to an *initialized* [`ffi::SearchResult`].
    /// 3. The caller holds exclusive execution access to the chain and output
    ///    until this call returns. A blocking upstream must not release that
    ///    access while this entry's mutable borrows survive.
    unsafe extern "C" fn result_processor_next(ptr: *mut Header, res: *mut SearchResult) -> c_int {
        let ptr = NonNull::new(ptr).unwrap();
        debug_assert!(ptr.is_aligned());

        // Safety:
        // 1. ptr is non-null and well-aligned
        // 2. all additional safety invariants have to be upheld by the caller (invariant 1.)
        unsafe { Self::debug_assert_same_type(ptr) };

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics on this type -
        // ensures that we can safely cast to `Self` here.
        // Additionally, when debug assertions are enabled, we perform an additional assertion above.
        let me = unsafe { ptr.cast::<Self>().as_mut() };
        // Safety: Context contines to to treat `me` as pinned
        let me = unsafe { Pin::new_unchecked(me) };
        let me = me.project();

        let cx = Context::new(me.header);

        let mut res = NonNull::new(res).unwrap();
        debug_assert!(res.is_aligned());

        // Safety: We have done as much checking as we can at the start of the function (checking alignment & non-null-ness).
        let res = unsafe { res.as_mut() };

        match me.result_processor.next(cx, res) {
            Ok(Some(())) => ffi::RPStatus_RS_RESULT_OK as c_int,
            Ok(None) => ffi::RPStatus_RS_RESULT_EOF as c_int,
            Err(Error::TimedOut) => ffi::RPStatus_RS_RESULT_TIMEDOUT as c_int,
            Err(Error::Error) => ffi::RPStatus_RS_RESULT_ERROR as c_int,
        }
    }

    /// FFI recovery entry for [`ResultProcessor::drain`].
    ///
    /// # Safety
    ///
    /// The caller owns the execution domain exclusively for the entire call.
    /// Both pointers are aligned and [valid]; the processor is pinned and the
    /// output is initialized and exclusive. No ownership release may occur
    /// while this entry's mutable borrows survive.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    unsafe extern "C" fn result_processor_drain(
        ptr: *mut Header,
        res: *mut SearchResult,
    ) -> ffi::RPDrainStatus {
        let ptr = NonNull::new(ptr).unwrap();
        debug_assert!(ptr.is_aligned());
        // SAFETY: the caller supplies this wrapper's live, pinned header.
        unsafe { Self::debug_assert_same_type(ptr) };
        // SAFETY: exclusive domain admission excludes overlapping RP borrows.
        let me = unsafe { ptr.cast::<Self>().as_mut() };
        // SAFETY: the caller preserves the wrapper's pinning invariant.
        let me = unsafe { Pin::new_unchecked(me) }.project();
        let cx = DrainContext(Context::new(me.header));
        let mut res = NonNull::new(res).unwrap();
        debug_assert!(res.is_aligned());
        // SAFETY: the caller supplies initialized, exclusively borrowed output.
        match me.result_processor.drain(cx, unsafe { res.as_mut() }) {
            Ok(Some(())) => ffi::RPDrainStatus_RP_DRAIN_OK,
            Ok(None) => ffi::RPDrainStatus_RP_DRAIN_EOF,
            Err(DrainError) => ffi::RPDrainStatus_RP_DRAIN_ERROR,
        }
    }

    /// VTable function dropping the `Box` backing this result processor.
    /// This is exposed through the `free` field of [`Header`] and only ever called by C code.
    ///
    /// # Safety
    ///
    /// The caller (C code) must uphold the following safety invariants:
    /// 1. `ptr` must be a non-null, well-aligned, valid pointer to a result processor (struct [`Header`]).
    unsafe extern "C" fn result_processor_free(me: *mut Header) {
        debug_assert!(me.is_aligned());

        let me = NonNull::new(me).unwrap();
        debug_assert!(me.is_aligned());

        // Safety:
        // 1. me is non-null and well-aligned
        // 2. all additional safety invariants have to be upheld by the caller (invariant 1.)
        unsafe { Self::debug_assert_same_type(me) };

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics
        // and constructor of this type - ensures that we can safely cast to `Self` here.
        // Additionally, when debug assertions are enabled, we perform an additional assertion above.
        // For all other safety guarantees (invariants 2. and 3.) we have to trust the QueryIterator implementation to be correct.
        drop(unsafe { Self::from_ptr(me.cast::<Self>()) });
    }

    /// Assert that the given `Header` has the expected inner Rust ResultProcessor type. This check is only performed with debug_assertions
    /// enabled.
    ///
    /// # Safety
    ///
    /// 1. `me` must be a well-aligned, valid pointer to a result processor (struct [`Header`]).
    #[cfg_attr(not(debug_assertions), expect(clippy::missing_const_for_fn))]
    unsafe fn debug_assert_same_type(_me: NonNull<Header>) {
        #[cfg(debug_assertions)]
        {
            // Safety: all invariants have to be upheld by the caller
            let header = unsafe { _me.as_ref() };
            assert_eq!(
                header.inner_ty_id,
                TypeId::of::<P>(),
                "Type mismatch: Expected result processor of type `{}`, but got `{}`",
                type_name::<P>(),
                header.inner_ty_name,
            )
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::test_utils::{Chain, ResultRP};

    #[test]
    fn mixed_c_header_drain() {
        unsafe extern "C" fn drain(
            _: *mut ffi::ResultProcessor,
            _: *mut ffi::SearchResult,
        ) -> ffi::RPDrainStatus {
            ffi::RPDrainStatus_RP_DRAIN_EOF
        }
        unsafe extern "C" fn next(
            _: *mut ffi::ResultProcessor,
            _: *mut ffi::SearchResult,
        ) -> c_int {
            ffi::RPStatus_RS_RESULT_EOF as c_int
        }
        // Allocate exactly the C prefix: a Rust Header would hide an oversized borrow.
        let mut source = Box::new(ffi::ResultProcessor {
            parent: ptr::null_mut(),
            upstream: ptr::null_mut(),
            type_: ffi::ResultProcessorType_RP_INDEX,
            rpGILTime: 0,
            Next: Some(next),
            Free: None,
            Drain: Some(drain),
        });
        let mut upstream = Upstream {
            ptr: NonNull::from(source.as_mut()).cast(),
            _borrow: PhantomData,
        };
        assert_eq!(upstream.ty(), ffi::ResultProcessorType_RP_INDEX);
        let mut row = SearchResult::new();
        assert_eq!(upstream.next(&mut row), Ok(None));
        assert_eq!(DrainUpstream(upstream).drain(&mut row), Ok(None));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn default_drain_does_not_restart_next_or_modify_output() {
        let mut chain = Chain::new();
        chain.append(ResultRP::new_ok_some());
        let mut row = SearchResult::new();
        let status = chain.next(&mut row);
        assert_eq!(status, ffi::RPStatus_RS_RESULT_OK as c_int);
        row.set_score(42.0);
        let status = chain.drain(&mut row);
        assert_eq!(status, ffi::RPDrainStatus_RP_DRAIN_EOF);
        assert_eq!(row.score(), 42.0);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn drain_propagates_rows_and_terminal_statuses_without_next() {
        struct Source(Option<Result<Option<()>, DrainError>>);
        impl ResultProcessor for Source {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;
            fn next(&mut self, _: Context, _: &mut SearchResult) -> Result<Option<()>, Error> {
                panic!("Drain must not restart Next")
            }
            fn drain(
                &mut self,
                _: DrainContext,
                row: &mut SearchResult,
            ) -> Result<Option<()>, DrainError> {
                let status = self.0.take().expect("terminal result must not be retried");
                if status == Ok(Some(())) {
                    row.set_score(42.0);
                }
                status
            }
        }
        struct Forward;
        impl ResultProcessor for Forward {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;
            fn next(&mut self, _: Context, _: &mut SearchResult) -> Result<Option<()>, Error> {
                panic!("Drain must not restart Next")
            }
            fn drain(
                &mut self,
                mut cx: DrainContext,
                row: &mut SearchResult,
            ) -> Result<Option<()>, DrainError> {
                let status = cx.upstream().unwrap().drain(row)?;
                if status.is_some() {
                    cx.subtract_total_results(1);
                    row.set_score(row.score() + 1.0);
                }
                Ok(status)
            }
        }
        for (status, expected) in [
            (Ok(Some(())), ffi::RPDrainStatus_RP_DRAIN_OK),
            (Ok(None), ffi::RPDrainStatus_RP_DRAIN_EOF),
            (Err(DrainError), ffi::RPDrainStatus_RP_DRAIN_ERROR),
        ] {
            let mut chain = Chain::new();
            chain.set_total_results(2);
            chain.append(Source(Some(status)));
            chain.append(Forward);
            let mut row = SearchResult::new();
            let actual = chain.drain(&mut row);
            assert_eq!(actual, expected);
            if status == Ok(Some(())) {
                assert_eq!(row.score(), 43.0);
                assert_eq!(chain.total_results(), 1);
            } else {
                assert_eq!(chain.total_results(), 2);
            }
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn drain_context_handles_absent_parent_and_upstream() {
        struct Root;
        impl ResultProcessor for Root {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;
            fn next(&mut self, _: Context, _: &mut SearchResult) -> Result<Option<()>, Error> {
                Ok(None)
            }
            fn drain(
                &mut self,
                mut cx: DrainContext,
                _: &mut SearchResult,
            ) -> Result<Option<()>, DrainError> {
                assert!(cx.parent().is_none());
                assert!(cx.upstream().is_none());
                cx.subtract_total_results(1);
                Ok(None)
            }
        }
        let rp = Box::pin(ResultProcessorWrapper::new(Root));
        // SAFETY: ptr remains pinned until reconstructed and dropped below.
        let ptr = unsafe { ResultProcessorWrapper::into_ptr(rp) };
        // SAFETY: the wrapper and initialized output are exclusively accessible.
        let status = unsafe {
            ResultProcessorWrapper::<Root>::result_processor_drain(
                ptr.cast().as_ptr(),
                &mut SearchResult::new(),
            )
        };
        assert_eq!(status, ffi::RPDrainStatus_RP_DRAIN_EOF);
        // SAFETY: this is the sole reconstruction of the still-pinned wrapper.
        drop(unsafe { ResultProcessorWrapper::from_ptr(ptr) });
    }

    // Compile time check to ensure that `Header` (which currently duplicates `ffi::ResultProcessor`)
    // has the exact same size, alignment, and field layout.
    const _: () = {
        // Header is larger than ffi::ResultProcessor because it has additional Rust-debugging fields
        assert!(std::mem::size_of::<Header>() >= std::mem::size_of::<ffi::ResultProcessor>());

        assert!(std::mem::align_of::<Header>() == std::mem::align_of::<ffi::ResultProcessor>());
        assert!(
            ::std::mem::offset_of!(Header, parent)
                == ::std::mem::offset_of!(ffi::ResultProcessor, parent)
        );
        assert!(
            ::std::mem::offset_of!(Header, upstream)
                == ::std::mem::offset_of!(ffi::ResultProcessor, upstream)
        );
        assert!(
            ::std::mem::offset_of!(Header, ty)
                == ::std::mem::offset_of!(ffi::ResultProcessor, type_)
        );
        assert!(
            ::std::mem::offset_of!(Header, rp_gil_time)
                == ::std::mem::offset_of!(ffi::ResultProcessor, rpGILTime)
        );
        assert!(
            ::std::mem::offset_of!(Header, next)
                == ::std::mem::offset_of!(ffi::ResultProcessor, Next)
        );
        assert!(
            ::std::mem::offset_of!(Header, free)
                == ::std::mem::offset_of!(ffi::ResultProcessor, Free)
        );
        assert!(
            ::std::mem::offset_of!(Header, drain)
                == ::std::mem::offset_of!(ffi::ResultProcessor, Drain)
        );
    };

    /// Assert that Rust error types translate to the correct C ret code
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn error_to_ret_code() {
        fn check(error: Error, expected: i32) {
            let mut chain = Chain::new();
            chain.append(ResultRP::new_err(error));

            let found = chain.next(&mut SearchResult::new());

            assert_eq!(found, expected);
        }

        check(Error::Error, ffi::RPStatus_RS_RESULT_ERROR as i32);
        check(Error::TimedOut, ffi::RPStatus_RS_RESULT_TIMEDOUT as i32);
    }

    /// Assert that returning `Ok(None)` from Rust translates to EOF in C
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn none_signals_eof() {
        let mut chain = Chain::new();
        chain.append(ResultRP::new_ok_none());

        let found = chain.next(&mut SearchResult::new());

        assert_eq!(found, ffi::RPStatus_RS_RESULT_EOF as i32);
    }

    /// Assert that `Ok(Some(())` in Rust translates to the `OK` in C
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn ok_some_signals_ok() {
        let mut chain = Chain::new();
        chain.append(ResultRP::new_ok_some());

        let found = chain.next(&mut SearchResult::new());

        assert_eq!(found, ffi::RPStatus_RS_RESULT_OK as i32);
    }

    /// Assert that C return codes translate to the correct Rust error types
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn c_ret_code_to_error() {
        // This function sets up a result processor in memory that mimics a C result processor
        // sidestepping all the the rust logic
        fn new_upstream(ret_code: c_int) -> NonNull<Header> {
            #[repr(C)]
            struct RP {
                header: Header,
                ret_code: c_int,
            }

            unsafe extern "C" fn result_processor_next(
                me: *mut Header,
                _res: *mut SearchResult,
            ) -> c_int {
                // SAFETY: only this fixture installs the callback on its pinned RP.
                unsafe { me.cast::<RP>().as_ref().unwrap().ret_code }
            }

            unsafe extern "C" fn result_processor_free(me: *mut Header) {
                // SAFETY: the chain invokes Free once on the allocation created below.
                unsafe { drop(Box::from_raw(me.cast::<RP>())) }
            }

            let b = Box::new(RP {
                header: Header {
                    parent: ptr::null_mut(),
                    upstream: ptr::null_mut(),
                    ty: ffi::ResultProcessorType_RP_MAX,
                    rp_gil_time: 0,
                    next: Some(result_processor_next),
                    free: Some(result_processor_free),
                    drain: None,

                    #[cfg(debug_assertions)]
                    inner_ty_id: TypeId::of::<()>(),
                    #[cfg(debug_assertions)]
                    inner_ty_name: "Mock C result processor",

                    _unpin: PhantomPinned,
                },

                ret_code,
            });
            NonNull::new(Box::into_raw(b)).unwrap().cast()
        }

        struct RP;
        impl ResultProcessor for RP {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

            fn next(
                &mut self,
                mut cx: Context,
                res: &mut SearchResult,
            ) -> Result<Option<()>, Error> {
                let mut upstream = cx.upstream().unwrap();
                upstream.next(res)
            }
        }

        fn check(code: i32, expected: Result<Option<()>, Error>) {
            let mut chain = Chain::new();
            // SAFETY: the fixture transfers a live, pinned header and matching destructor.
            unsafe { chain.push_raw(new_upstream(code)) };
            chain.append(RP);

            let (cx, rp) = chain.last_as_context_and_inner::<RP>();

            // we don't care about the exact search result value here
            let res = rp.next(cx, &mut SearchResult::new());
            assert_eq!(res, expected);
        }

        check(ffi::RPStatus_RS_RESULT_OK as i32, Ok(Some(())));
        check(ffi::RPStatus_RS_RESULT_EOF as i32, Ok(None));
        check(ffi::RPStatus_RS_RESULT_ERROR as i32, Err(Error::Error));
        check(
            ffi::RPStatus_RS_RESULT_TIMEDOUT as i32,
            Err(Error::TimedOut),
        );
    }

    /// Assert that the search result is passed correctly
    #[test]
    #[cfg_attr(
        miri,
        ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
    )]
    fn search_result_passing() {
        struct Upstream;
        impl ResultProcessor for Upstream {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

            fn next(&mut self, _cx: Context, res: &mut SearchResult) -> Result<Option<()>, Error> {
                res.set_score(42.0);
                Ok(Some(()))
            }
        }

        struct RP;
        impl ResultProcessor for RP {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

            fn next(
                &mut self,
                mut cx: Context,
                res: &mut SearchResult,
            ) -> Result<Option<()>, Error> {
                let mut upstream = cx.upstream().unwrap();
                upstream.next(res)
            }
        }

        let mut chain = Chain::new();

        chain.append(Upstream);
        chain.append(RP);

        let (cx, rp) = chain.last_as_context_and_inner::<RP>();

        let mut res = SearchResult::new();
        rp.next(cx, &mut res).unwrap().unwrap();

        assert_eq!(res.score(), 42.0);
    }

    #[test]
    fn wrapper_proper_alignment() {
        let mut chain = Chain::new();
        chain.append(ResultRP::new_ok_some());

        // Safety: we just check the alignment
        let ptr = unsafe { chain.last_raw() };
        assert!(
            ptr.cast::<ffi::ResultProcessor>().is_aligned(),
            "Pointer should be properly aligned"
        );
    }

    #[test]
    fn wrapper_initializes_null_fields() {
        let counter = Box::pin(ResultProcessorWrapper::new(ResultRP::new_ok_some()));

        assert!(counter.header.parent.is_null(), "Parent should be null");
        assert!(counter.header.upstream.is_null(), "Upstream should be null");
    }

    #[test]
    fn wrapper_initializes_function_pointers() {
        let counter = Box::pin(ResultProcessorWrapper::new(ResultRP::new_ok_some()));

        assert!(counter.header.next.is_some(), "Next function should be set");
        assert!(counter.header.free.is_some(), "Free function should be set");
        assert!(
            counter.header.drain.is_some(),
            "Drain function should be set"
        );
    }
}
