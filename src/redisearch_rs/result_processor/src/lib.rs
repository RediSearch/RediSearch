/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::{c_int, timespec};
use pin_project_lite::pin_project;
use std::{
    marker::PhantomPinned,
    pin::Pin,
    ptr::{self, NonNull},
};

/// Errors that can be returned by [`ResultProcessor`]
#[derive(Debug)]
pub enum Error {
    /// Execution paused due to rate limiting (or manual pause from ext. thread??)
    Paused,
    /// Execution halted because of timeout
    TimedOut,
    /// Aborted because of error. The QueryState (parent->status) should have
    /// more information.
    Error,
}

/// This is the main trait that Rust result processors need to implement
pub trait ResultProcessor {
    /// The type of this result processor.
    const TYPE: ffi::ResultProcessorType;

    /// Pull the next [`ffi::SearchResult`] from this result processor into the provided `res` location.
    ///
    /// Should return `Ok(Some(()))` if a search result was successfully pulled from the processor
    /// and `Ok(None)` to indicate the end of search results has been reached.
    /// `Err(_)` cases should be returned to indicate exceptional error.
    fn next(&mut self, cx: Context, res: &mut ffi::SearchResult) -> Result<Option<()>, Error>;
}

/// This type allows result processors to access its context (the owning QueryIterator, upstream result processors, etc.)
pub struct Context<'a> {
    result_processor: Pin<&'a mut Header>,
}

impl Context<'_> {
    /// The previous result processor in the pipeline
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

/// The previous result processor in the pipeline
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
            ffi::RPStatus_RS_RESULT_PAUSED => Err(Error::Paused),
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
/// chain. Accidentally moving a processor will remove in a broken link and undefined behaviour.
///
/// This invariant is implicit in the C code where its uncommon to move the heap allocated objects (which would
/// mean memcopying them around); In Rust we need to explicitly manage this invariant however, as move semantics make
/// it easy to break accidentally. For details refer to the [`Pin`] documentation which explains the concept of "pinning"
/// a Rust value in memory and its implications.
///
/// This crate has to ensure that these pinning invariants are upheld internally, but when sending pointers through the FFI boundary
/// this is impossible to guarantee. Thankfully, as mentioned above its quite rare to move out of pointers, so its reasonably safe.
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
        ::std::mem::offset_of!(Header, ty) == ::std::mem::offset_of!(ffi::ResultProcessor, type_)
    );
    assert!(
        ::std::mem::offset_of!(Header, gil_time)
            == ::std::mem::offset_of!(ffi::ResultProcessor, GILTime)
    );
    assert!(
        ::std::mem::offset_of!(Header, next) == ::std::mem::offset_of!(ffi::ResultProcessor, Next)
    );
    assert!(
        ::std::mem::offset_of!(Header, free) == ::std::mem::offset_of!(ffi::ResultProcessor, Free)
    );
};

pin_project! {
    #[derive(Debug)]
    #[repr(C)]
    pub struct ResultProcessorWrapper<P> {
        #[pin]
        header: Header,
        result_processor: P,
    }
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
    /// The caller must ensure the pointer was previously created through [`Self::into_ptr`]. Furthermore,
    /// callers have to be careful to never call this method twice for the same pointer, otherwise a double-free
    /// or other memory corruptions will occur.
    /// The caller *must* also ensure that `ptr` continues to be treated as pinned.
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
            Err(Error::Paused) => ffi::RPStatus_RS_RESULT_PAUSED as c_int,
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
    #![allow(unused)]

    use super::*;
    use ffi::SearchResult;

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
}
