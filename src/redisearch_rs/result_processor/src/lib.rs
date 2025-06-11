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

use libc::{c_int, timespec};
use pin_project::pin_project;
use std::{
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
///
/// /// A simple result processor that simply prints out the search result received from the previous processor
/// /// before passing it on.
/// struct Logger;
///
/// impl ResultProcessor for Logger {
///    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType::MAX;
///
///     fn next(&mut self, mut cx: Context, res: &mut ffi::SearchResult) -> Result<Option<()>, Error> {
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
    /// In both cases `Ok(None)` and `Err(_)` indicate to the caller that calling `next`
    /// will not yield values anymore, thus ending iteration.
    fn next(&mut self, cx: Context, res: &mut ffi::SearchResult) -> Result<Option<()>, Error>;
}

/// This type allows result processors to access its context (the owning QueryIterator, upstream result processors, etc.)
pub struct Context<'a> {
    ptr: NonNull<Header>,
    _borrow: PhantomData<&'a mut Header>,
}

impl Context<'_> {
    /// Create a new context object from a raw `Header` pointer.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that `ptr` remains valid for the entire lifetime of the returned `Context`
    unsafe fn from_raw(ptr: NonNull<Header>) -> Self {
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
}

/// The previous result processor in the pipeline.
#[derive(Debug)]
pub struct Upstream<'a> {
    ptr: NonNull<Header>,
    _borrow: PhantomData<&'a mut Header>,
}

impl Upstream<'_> {
    /// Pull the next [`ffi::SearchResult`] from this result processor into the provided `res` location.
    ///
    /// Returns `Ok(Some(()))` if a search result was successfully pulled from the processor
    /// and `Ok(None)` to indicate the end of search results has been reached.
    ///
    /// # Errors
    ///
    /// Returns `Err(_)` for exceptional error cases.
    pub fn next(&mut self, res: &mut ffi::SearchResult) -> Result<Option<()>, Error> {
        // Safety: We have to trust that the upstream pointer set by our QueryIterator parent
        // is correct.
        let next = unsafe { self.ptr.as_ref() }
            .next
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
/// We wrap a reference in the Pin<T> type (Pin<&mut T>) which disallows moving the pointee from its location in memory.
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
    /// Reference to the parent QueryIterator that owns this result processor
    parent: *mut ffi::QueryIterator,
    /// Previous result processor in the chain
    upstream: *mut Header,
    /// Type of result processor
    ty: ffi::ResultProcessorType,
    gil_time: timespec,
    /// "VTable" function. Pulls [`ffi::SearchResult`]s out of this result processor.
    ///
    /// Populates the result pointed to by `res`. The existing data of `res` is
    /// not read, so it is the responsibility of the caller to ensure that there
    /// are no refcount leaks in the structure.
    ///
    /// Users can use [`ffi::SearchResult_Clear`] to reset the structure without freeing it.
    ///
    /// The populated structure (if [`ffi::RPStatus_RS_RESULT_OK`] is returned) does contain references
    /// to document data. Callers *MUST* ensure they are eventually freed.
    next: Option<unsafe extern "C" fn(self_: *mut Header, res: *mut ffi::SearchResult) -> c_int>,
    /// "VTable" function. Frees the processor and any internal data related to it.
    free: Option<unsafe extern "C" fn(self_: *mut Header)>,
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
    P: ResultProcessor,
{
    /// Construct a new FFI-ResultProcessor from the provided [`crate::ResultProcessor`] implementer.
    pub fn new(result_processor: P) -> Self {
        Self {
            header: Header {
                parent: ptr::null_mut(), // will be set by `QITR_PushRP` when inserting this result processor into the chain
                upstream: ptr::null_mut(), // will be set by `QITR_PushRP` when inserting this result processor into the chain
                ty: P::TYPE,
                gil_time: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                next: Some(Self::result_processor_next),
                free: Some(Self::result_processor_free),
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
    pub unsafe fn into_ptr(me: Pin<Box<Self>>) -> *mut Self {
        // This function must be kept in sync with `Self::from_ptr` below.

        // Safety: The caller promised to continue to treat the returned pointer
        // as pinned and never move out of it.
        Box::into_raw(unsafe { Pin::into_inner_unchecked(me) })
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
    unsafe fn from_ptr(ptr: *mut Self) -> Pin<Box<Self>> {
        // This function must be kept in sync with `Self::into_ptr` above.

        // Safety:
        // 1 -> This function will only ever be called through the `result_processor_free` vtable method below.
        //      We therefore know - through construction - that the pointer was previously created through `into_ptr`.
        // 2 -> Has to be upheld by the caller
        let b = unsafe { Box::from_raw(ptr.cast()) };
        // Safety: 3 -> Caller has to uphold the pin contract
        unsafe { Pin::new_unchecked(b) }
    }

    /// VTable function exposing the [`ResultProcessor::next`] method. This is exposed through the `next` field of [`Header`].
    unsafe extern "C" fn result_processor_next(
        ptr: *mut Header,
        res: *mut ffi::SearchResult,
    ) -> c_int {
        let ptr = NonNull::new(ptr).unwrap();
        debug_assert!(ptr.is_aligned());

        let mut res = NonNull::new(res).unwrap();
        debug_assert!(res.is_aligned());

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics on this type -
        // ensures that we can safely cast to `Self` here.
        let me = unsafe { ptr.cast::<Self>().as_mut() };

        // Safety: ptr outlives the current scope
        let cx = unsafe { Context::from_raw(ptr) };

        // Safety: We have done as much checking as we can at the start of the function (checking alignment & non-null-ness).
        let res = unsafe { res.as_mut() };

        match me.result_processor.next(cx, res) {
            Ok(Some(())) => ffi::RPStatus_RS_RESULT_OK as c_int,
            Ok(None) => ffi::RPStatus_RS_RESULT_EOF as c_int,
            Err(Error::TimedOut) => ffi::RPStatus_RS_RESULT_TIMEDOUT as c_int,
            Err(Error::Error) => ffi::RPStatus_RS_RESULT_ERROR as c_int,
        }
    }

    /// VTable function dropping the `Box` backing this result processor.
    /// This is exposed through the `free` field of [`Header`] and only ever called by C code.
    unsafe extern "C" fn result_processor_free(me: *mut Header) {
        debug_assert!(me.is_aligned());

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics
        //  and constructor of this type - ensures that we can safely cast to `Self` here.
        //  For all other safety guarantees (invariants 2. and 3.) we have to trust the QueryIterator implementation to be correct.
        drop(unsafe { Self::from_ptr(me.cast::<Self>()) });
    }
}

#[cfg(test)]
pub(crate) mod test {
    #![expect(unused)]

    use super::*;
    use ffi::{RPStatus_RS_RESULT_OK, SearchResult};

    // Compile time check to ensure that `Header` (which currently duplicates `ffi::ResultProcessor`)
    // has the exact same size, alignment, and field layout.
    const _: () = {
        assert!(std::mem::size_of::<Header>() == std::mem::size_of::<ffi::ResultProcessor>(),);
        assert!(std::mem::align_of::<Header>() == std::mem::align_of::<ffi::ResultProcessor>(),);
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
            ::std::mem::offset_of!(Header, gil_time)
                == ::std::mem::offset_of!(ffi::ResultProcessor, GILTime)
        );
        assert!(
            ::std::mem::offset_of!(Header, next)
                == ::std::mem::offset_of!(ffi::ResultProcessor, Next)
        );
        assert!(
            ::std::mem::offset_of!(Header, free)
                == ::std::mem::offset_of!(ffi::ResultProcessor, Free)
        );
    };

    /// Create a ResultProcessor from an `Iterator` for testing purposes
    pub fn from_iter<I>(i: I) -> IterResultProcessor<I::IntoIter>
    where
        I: IntoIterator<Item = SearchResult>,
    {
        IterResultProcessor {
            iter: i.into_iter(),
        }
    }

    /// ResultProcessor that yields items from an inner `Iterator`
    #[derive(Debug)]
    pub struct IterResultProcessor<I> {
        iter: I,
    }

    impl<I> ResultProcessor for IterResultProcessor<I>
    where
        I: Iterator<Item = SearchResult>,
    {
        const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX + 1;

        fn next(&mut self, _cx: Context, out: &mut ffi::SearchResult) -> Result<Option<()>, Error> {
            if let Some(res) = self.iter.next() {
                *out = res;
                Ok(Some(()))
            } else {
                Ok(None)
            }
        }
    }

    const SEARCH_RESULT_INIT: ffi::SearchResult = ffi::SearchResult {
        docId: 0,
        score: 0.0,
        scoreExplain: ptr::null_mut(),
        dmd: ptr::null(),
        indexResult: ptr::null_mut(),
        rowdata: ffi::RLookupRow {
            sv: ptr::null(),
            dyn_: ptr::null_mut(),
            ndyn: 0,
        },
        flags: 0,
    };

    struct ResultRP {
        res: Option<Result<Option<()>, Error>>,
    }
    impl ResultRP {
        fn new_err(error: Error) -> Self {
            Self {
                res: Some(Err(error)),
            }
        }
        fn new_ok_some() -> Self {
            Self {
                res: Some(Ok(Some(()))),
            }
        }
        fn new_ok_none() -> Self {
            Self {
                res: Some(Ok(None)),
            }
        }
    }
    impl ResultProcessor for ResultRP {
        const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

        fn next(
            &mut self,
            _cx: Context,
            _res: &mut ffi::SearchResult,
        ) -> Result<Option<()>, Error> {
            self.res.take().unwrap()
        }
    }

    /// Assert that Rust error types translate to the correct C ret code
    #[test]
    fn error_to_ret_code() {
        fn check(error: Error, expected: i32) {
            let rp = Box::pin(ResultProcessorWrapper::new(ResultRP::new_err(error)));
            let rp = NonNull::new(unsafe { ResultProcessorWrapper::into_ptr(rp) }).unwrap();
            let mut rp: NonNull<Header> = rp.cast();

            #[allow(const_item_mutation)] // we don't care about the exact search result value here
            let found =
                unsafe { (rp.as_mut().next.unwrap())(rp.as_ptr(), &mut SEARCH_RESULT_INIT) };

            assert_eq!(found, expected);

            // we need to correctly free the result processor at the end
            unsafe {
                let mut rp = rp.cast::<Header>();
                (rp.as_mut().free.unwrap())(rp.as_ptr());
            }
        }

        check(Error::Error, ffi::RPStatus_RS_RESULT_ERROR as i32);
        check(Error::TimedOut, ffi::RPStatus_RS_RESULT_TIMEDOUT as i32);
    }

    /// Assert that returning `Ok(None)` from Rust translates to EOF in C
    #[test]
    fn none_signals_eof() {
        let rp = Box::pin(ResultProcessorWrapper::new(ResultRP::new_ok_none()));
        let rp = NonNull::new(unsafe { ResultProcessorWrapper::into_ptr(rp) }).unwrap();
        let mut rp: NonNull<Header> = rp.cast();

        assert_eq!(
            unsafe {
                // we don't care about the exact search result value here
                #[allow(const_item_mutation)]
                (rp.as_mut().next.unwrap())(rp.as_ptr(), &mut SEARCH_RESULT_INIT)
            },
            ffi::RPStatus_RS_RESULT_EOF as i32
        );

        // we need to correctly free the result processor at the end
        unsafe {
            let mut rp = rp.cast::<Header>();
            (rp.as_mut().free.unwrap())(rp.as_ptr());
        }
    }

    /// Assert that `Ok(Some(())` in Rust translates to the `OK` in C
    #[test]
    fn ok_some_signals_ok() {
        let rp = Box::pin(ResultProcessorWrapper::new(ResultRP::new_ok_some()));
        let rp = NonNull::new(unsafe { ResultProcessorWrapper::into_ptr(rp) }).unwrap();
        let mut rp: NonNull<Header> = rp.cast();

        assert_eq!(
            unsafe {
                // we don't care about the exact search result value here
                #[allow(const_item_mutation)]
                (rp.as_mut().next.unwrap())(rp.as_ptr(), &mut SEARCH_RESULT_INIT)
            },
            ffi::RPStatus_RS_RESULT_OK as i32
        );

        // we need to correctly free the result processor at the end
        unsafe {
            let mut rp = rp.cast::<Header>();
            (rp.as_mut().free.unwrap())(rp.as_ptr());
        }
    }

    /// Assert that C return codes translate to the correct Rust error types
    #[test]
    fn c_ret_code_to_error() {
        fn new_upstream(ret_code: c_int) -> NonNull<Header> {
            #[repr(C)]
            struct RP {
                header: Header,
                ret_code: c_int,
            }

            unsafe extern "C" fn result_processor_next(
                me: *mut Header,
                _res: *mut ffi::SearchResult,
            ) -> c_int {
                unsafe { me.cast::<RP>().as_ref().unwrap().ret_code }
            }

            unsafe extern "C" fn result_processor_free(me: *mut Header) {
                unsafe { drop(Box::from_raw(me.cast::<RP>())) }
            }

            let b = Box::new(RP {
                header: Header {
                    parent: ptr::null_mut(),
                    upstream: ptr::null_mut(),
                    ty: ffi::ResultProcessorType_RP_MAX,
                    gil_time: timespec {
                        tv_sec: 0,
                        tv_nsec: 0,
                    },
                    next: Some(result_processor_next),
                    free: Some(result_processor_free),
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
                res: &mut ffi::SearchResult,
            ) -> Result<Option<()>, Error> {
                let mut upstream = cx.upstream().unwrap();
                upstream.next(res)
            }
        }

        fn check(code: i32, expected: Result<Option<()>, Error>) {
            let mut upstream = new_upstream(code);

            let mut rp = Box::pin(ResultProcessorWrapper::new(RP));

            // Emulate what `QITR_PushRP` would be doing
            rp.header.upstream = upstream.as_ptr();

            let mut rp = NonNull::new(unsafe { ResultProcessorWrapper::into_ptr(rp) }).unwrap();

            let cx = unsafe { Context::from_raw(rp.cast()) };

            let res = unsafe {
                // we don't care about the exact search result value here
                #[allow(const_item_mutation)]
                rp.as_mut()
                    .result_processor
                    .next(cx, &mut SEARCH_RESULT_INIT)
            };

            assert_eq!(res, expected);

            // we need to correctly free both result processors at the end
            unsafe {
                let mut rp = rp.cast::<Header>();
                (rp.as_mut().free.unwrap())(rp.as_ptr());

                (upstream.as_mut().free.unwrap())(upstream.as_ptr());
            }
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
    fn search_result_passing() {
        fn new_upstream() -> NonNull<Header> {
            unsafe extern "C" fn result_processor_next(
                _me: *mut Header,
                res: *mut ffi::SearchResult,
            ) -> c_int {
                unsafe { res.as_mut() }.unwrap().score = 42.0;

                RPStatus_RS_RESULT_OK as c_int
            }

            unsafe extern "C" fn result_processor_free(me: *mut Header) {
                unsafe { drop(Box::from_raw(me)) }
            }

            let b = Box::new(Header {
                parent: ptr::null_mut(),
                upstream: ptr::null_mut(),
                ty: ffi::ResultProcessorType_RP_MAX,
                gil_time: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                next: Some(result_processor_next),
                free: Some(result_processor_free),
                _unpin: PhantomPinned,
            });
            NonNull::new(Box::into_raw(b)).unwrap()
        }

        struct RP;
        impl ResultProcessor for RP {
            const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

            fn next(
                &mut self,
                mut cx: Context,
                res: &mut ffi::SearchResult,
            ) -> Result<Option<()>, Error> {
                let mut upstream = cx.upstream().unwrap();
                upstream.next(res)
            }
        }

        let mut upstream = new_upstream();

        let mut rp = Box::pin(ResultProcessorWrapper::new(RP));

        // Emulate what `QITR_PushRP` would be doing
        rp.header.upstream = upstream.as_ptr();

        let mut rp = NonNull::new(unsafe { ResultProcessorWrapper::into_ptr(rp) }).unwrap();

        let cx = unsafe { Context::from_raw(rp.cast()) };

        let mut res = SEARCH_RESULT_INIT;
        unsafe {
            rp.as_mut()
                .result_processor
                .next(cx, &mut res)
                .unwrap()
                .unwrap();
        };
        assert_eq!(res.score, 42.0);

        // we need to correctly free both result processors at the end
        unsafe {
            let mut rp = rp.cast::<Header>();
            (rp.as_mut().free.unwrap())(rp.as_ptr());

            (upstream.as_mut().free.unwrap())(upstream.as_ptr());
        }
    }

    /// Mock implementation of `SearchResult_Clear` for tests
    ///
    /// this doesn't actually free anything, so will leak resources but hopefully this is fine for the few Rust
    /// tests for now
    #[unsafe(no_mangle)]
    unsafe extern "C" fn SearchResult_Clear(r: *mut ffi::SearchResult) {
        let r = unsafe { r.as_mut().unwrap() };

        // This won't affect anything if the result is null
        r.score = 0.0;

        // SEDestroy(r->scoreExplain);
        r.scoreExplain = ptr::null_mut();

        // IndexResult_Free(r->indexResult);
        r.indexResult = ptr::null_mut();

        r.flags = 0;
        // RLookupRow_Wipe(&r->rowdata);

        r.dmd = ptr::null();
        //   DMD_Return(r->dmd);
    }
}
// types:
pub type DocId = u64;

use ffi::RSSortingVector;
use ffi::RSValue;

/// Row data for a lookup key. This abstracts the question of "where" the data comes from.
///
/// This stores the values received by an iteration over a RLookup.
///
/// cbindgen:field-names=[sv, dyn, ndyn]
#[repr(C)]
pub struct RLookupRow {
    /// contains sortable values for the row, is depending on the filed sorting
    pub sorting_vector: *const RSSortingVector,

    /// contains the dynamic values of the row
    pub dyn_values: *mut *mut RSValue,

    /// the number of dynamic values in the row
    pub num_values: libc::size_t,
}
