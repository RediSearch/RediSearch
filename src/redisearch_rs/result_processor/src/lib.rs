/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::{c_int, timespec};
use pin_project::pin_project;
use std::{
    marker::PhantomPinned,
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

/// Result processors are in charge of transforming the entries retrieved from the inverted index when
/// trying to fulfil a user-provided query.
///
/// The processing kind is varied—e.g. it may entail scoring, filtering, sorting, paginating, etc.
///
/// # Structure
///
/// Result processors are structured as a chain, assembled by the query planner.
/// Processing is lazy: when the last processor in the chain is asked to yield a result, it
/// will in turn ask for entries from the previous processor, recursively until it reached
/// the beginning of the chain.
/// At the head of the chain, you will always find an index iterator, yielding entries from
/// the database indexes.
///
/// # Result storage
///
/// The processing output is accumulated in a [`ffi::SearchResult`] object.
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
    result_processor: Pin<&'a mut Header>,
}

impl Context<'_> {
    /// The previous result processor in the pipeline if present.
    ///
    /// Returns `None` when the result processor has no upstream.
    pub fn upstream(&mut self) -> Option<Upstream<'_>> {
        // Safety: We have to trust that the upstream pointer set by our QueryIterator parent
        // is correct.
        let result_processor = unsafe { self.result_processor.upstream.as_mut()? };

        // Safety: Refer to the pinning comment of ffi::ResultProcessor for why
        // this necessary and reasonably safe.
        let result_processor = unsafe { Pin::new_unchecked(result_processor) };

        Some(Upstream { result_processor })
    }
}

/// The previous result processor in the pipeline.
#[derive(Debug)]
pub struct Upstream<'a> {
    result_processor: Pin<&'a mut Header>,
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
        let next = self
            .result_processor
            .next
            .expect("result processor `Next` vtable function was null");

        // Safety: The `next` function is required to treat the `*mut Header` pointer as pinned.
        let result_processor = unsafe { self.result_processor.as_mut().get_unchecked_mut() };
        // Safety: At the end of the day we're calling to arbitrary code at this point... But provided
        // the QueryIterator and other result processors are implemented correctly, this should be safe.
        let ret_code = unsafe { next(ptr::from_mut(result_processor), res) };

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
/// it easy to break accidentally. For details refer to the [`Pin`] documentation which explains the concept of "pinning"
/// a Rust value in memory and its implications.
///
/// This crate has to ensure that these pinning invariants are upheld internally, but when sending pointers through the FFI boundary
/// this is impossible to guarantee. Thankfully, as mentioned above its quite rare to move out of pointers, so its reasonably safe.
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

impl Header {
    #[cfg(test)]
    pub fn as_context(self: Pin<&mut Self>) -> Context<'_> {
        Context {
            result_processor: self,
        }
    }
}

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

        // Safety: The caller has to ensure they never call this function twice in a memory-unsafe way. But since this
        // is not public anyway, it should be easier to verify.
        let b = unsafe { Box::from_raw(ptr.cast()) };
        // Safety: Also responsibility of the caller.
        unsafe { Pin::new_unchecked(b) }
    }

    unsafe extern "C" fn result_processor_next(
        me: *mut Header,
        res: *mut ffi::SearchResult,
    ) -> c_int {
        let me = NonNull::new(me).unwrap();
        debug_assert!(me.is_aligned());

        let mut res = NonNull::new(res).unwrap();
        debug_assert!(res.is_aligned());

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics on this type -
        // ensures that we can safely cast to `Self` here.
        let me = unsafe { me.cast::<Self>().as_mut() };
        // Safety: We must ensure that the `me` pointer remains pinned even if its cast to a mutable reference now
        let mut me = unsafe { Pin::new_unchecked(me) };

        // Through the magic of pin-projection we can mutable access the result processor in a fully Rust-compatible
        // safe way.
        let me = me.as_mut().project();

        let cx = Context {
            result_processor: me.header,
        };

        // Safety: We have done as much checking as we can at the start of the function (checking alignment & non-null-ness).
        let res = unsafe { res.as_mut() };

        match me.result_processor.next(cx, res) {
            Ok(Some(())) => ffi::RPStatus_RS_RESULT_OK as c_int,
            Ok(None) => ffi::RPStatus_RS_RESULT_EOF as c_int,
            Err(Error::TimedOut) => ffi::RPStatus_RS_RESULT_TIMEDOUT as c_int,
            Err(Error::Error) => ffi::RPStatus_RS_RESULT_ERROR as c_int,
        }
    }

    unsafe extern "C" fn result_processor_free(me: *mut Header) {
        debug_assert!(me.is_aligned());

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics
        //  and constructor of this type - ensures that we can safely cast to `Self` here.
        //  For all other safety guarantees we have to trust the QueryIterator implementation to be correct.
        drop(unsafe { Self::from_ptr(me.cast::<Self>()) });
    }
}

#[cfg(test)]
pub(crate) mod test {
    #![expect(unused)]

    use super::*;
    use ffi::{RPStatus_RS_RESULT_OK, SearchResult};

    // Header duplicates ffi::ResultProcessor, so we need to make sure it has the exact same size, alignment and field layout.
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

        fn next(&mut self, cx: Context, res: &mut ffi::SearchResult) -> Result<Option<()>, Error> {
            self.res.take().unwrap()
        }
    }

    /// Assert that Rust error types translate to the correct C ret code
    #[test]
    fn error_to_ret_code() {
        fn check(error: Error, expected: i32) {
            let mut res = SEARCH_RESULT_INIT;

            let mut rp = Box::pin(ResultProcessorWrapper::new(ResultRP::new_err(error)));
            let h = ptr::from_mut(&mut rp.header);
            let found = unsafe { (rp.header.next.unwrap())(h, ptr::from_mut(&mut res)) };

            assert_eq!(found, expected);
        }

        check(Error::Error, ffi::RPStatus_RS_RESULT_ERROR as i32);
        check(Error::TimedOut, ffi::RPStatus_RS_RESULT_TIMEDOUT as i32);
    }

    /// Assert that returning `Ok(None)` from Rust translates to EOF in C
    #[test]
    fn none_signals_eof() {
        let mut rp = Box::pin(ResultProcessorWrapper::new(ResultRP::new_ok_none()));
        let h = ptr::from_mut(&mut rp.header);
        assert_eq!(
            unsafe {
                // we don't care about the exact search result value here
                #[allow(const_item_mutation)]
                (rp.header.next.unwrap())(h, ptr::from_mut(&mut SEARCH_RESULT_INIT))
            },
            ffi::RPStatus_RS_RESULT_EOF as i32
        );
    }

    /// Assert that `Ok(Some(())` in Rust translates to the `OK` in C
    #[test]
    fn ok_some_signals_ok() {
        let mut rp = Box::pin(ResultProcessorWrapper::new(ResultRP::new_ok_some()));
        let h = ptr::from_mut(&mut rp.header);
        assert_eq!(
            unsafe {
                // we don't care about the exact search result value here
                #[allow(const_item_mutation)]
                (rp.header.next.unwrap())(h, ptr::from_mut(&mut SEARCH_RESULT_INIT))
            },
            ffi::RPStatus_RS_RESULT_OK as i32
        );
    }

    /// Assert that C return codes translate to the correct Rust error types
    #[test]
    fn c_ret_code_to_error() {
        fn new_upstream(ret_code: c_int) -> Header {
            unsafe extern "C" fn result_processor_next(
                me: *mut Header,
                res: *mut ffi::SearchResult,
            ) -> c_int {
                unsafe { me.as_ref().unwrap().parent as c_int }
            }

            Header {
                // encode the ret code as the parent pointer so we dont have to define a new wrapper type
                parent: ret_code as *mut _,
                upstream: ptr::null_mut(),
                ty: ffi::ResultProcessorType_RP_MAX,
                gil_time: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                next: Some(result_processor_next),
                free: None,
                _unpin: PhantomPinned,
            }
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
            let mut rp = rp.as_mut().project();
            // Emulate what `QITR_PushRP` would be doing
            *rp.header.as_mut().project().upstream = ptr::from_mut(&mut upstream);

            let cx = rp.header.as_context();
            #[allow(const_item_mutation)] // we don't care about the exact search result value here
            let res = rp.result_processor.next(cx, &mut SEARCH_RESULT_INIT);

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
    fn search_result_passing() {
        fn new_upstream() -> Header {
            unsafe extern "C" fn result_processor_next(
                me: *mut Header,
                res: *mut ffi::SearchResult,
            ) -> c_int {
                unsafe { res.as_mut() }.unwrap().score = 42.0;

                RPStatus_RS_RESULT_OK as c_int
            }

            Header {
                parent: ptr::null_mut(),
                upstream: ptr::null_mut(),
                ty: ffi::ResultProcessorType_RP_MAX,
                gil_time: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                next: Some(result_processor_next),
                free: None,
                _unpin: PhantomPinned,
            }
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
        let mut rp = rp.as_mut().project();
        // Emulate what `QITR_PushRP` would be doing
        *rp.header.as_mut().project().upstream = ptr::from_mut(&mut upstream);

        let cx = rp.header.as_context();

        let mut res = SEARCH_RESULT_INIT;
        rp.result_processor.next(cx, &mut res).unwrap().unwrap();

        assert_eq!(res.score, 42.0);
    }
}
