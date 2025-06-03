/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use std::{
    ffi::c_void,
    marker::PhantomPinned,
    mem::MaybeUninit,
    pin::Pin,
    ptr::{self, NonNull},
};

use libc::timespec;
use pin_project_lite::pin_project;

use crate::Error;

/// Stub type representing the search result that is passed through the result processor chain.
#[repr(C)]
#[derive(Default)]
pub struct SearchResult {
    // stub
    private: [u8; 0],
}

/// Known result processor types.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultProcessorType(pub i32);

impl ResultProcessorType {
    pub const INDEX: Self = Self(0);
    pub const LOADER: Self = Self(1);
    pub const SAFE_LOADER: Self = Self(2);
    pub const SCORER: Self = Self(3);
    pub const SORTER: Self = Self(4);
    pub const COUNTER: Self = Self(5);
    pub const PAGE_LIMITER: Self = Self(6);
    pub const HIGHLIGHTER: Self = Self(7);
    pub const GROUP: Self = Self(8);
    pub const PROJECTOR: Self = Self(9);
    pub const FILTER: Self = Self(10);
    pub const PROFILE: Self = Self(11);
    pub const NETWORK: Self = Self(12);
    pub const METRICS: Self = Self(13);
    pub const KEY_NAME_LOADER: Self = Self(14);
    pub const MAX_SCORE_NORMALIZER: Self = Self(15);
    pub const TIMEOUT: Self = Self(16); // DEBUG ONLY
    pub const CRASH: Self = Self(17); // DEBUG ONLY
    pub const MAX: Self = Self(18);
}

/// Status codes returned by [`ResultProcessorNext`].
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RPStatus(pub(crate) i32);

impl RPStatus {
    /// Result is filled with valid data
    pub const RS_RESULT_OK: Self = Self(0);
    /// Result is empty, and the last result has already been returned.
    pub const RS_RESULT_EOF: Self = Self(1);
    /// Execution paused due to rate limiting (or manual pause from ext. thread??)
    pub const RS_RESULT_PAUSED: Self = Self(2);
    /// Execution halted because of timeout
    pub const RS_RESULT_TIMEDOUT: Self = Self(3);
    /// Aborted because of error. The QueryState (parent->status) should have
    /// more information.
    pub const RS_RESULT_ERROR: Self = Self(4);
}

type ResultProcessorNext =
    unsafe extern "C" fn(*mut Header, res: *mut MaybeUninit<SearchResult>) -> RPStatus;
type ResultProcessorFree = unsafe extern "C" fn(*mut Header);

/// Properties of Result Processors that are accessed by C code through FFI. This type is named `Header` because it
/// must be the first member of the [`ResultProcessor`] struct in order to guarantee that we can cast a `*mut ResultProcessor<P>`
/// to a `*mut Header` (which is what the C code expects to receive).
///
/// # Pinning Safety Contract
///
/// Result processors intrusively linked, where one result processor has the pointer to the
/// previous (forming an intrusively singly-linked list). This places a big safety invariant on all code
/// that touches this data structure: Result processors must *never* be moved while part of a QueryIterator
/// chain. Accidentally moving a processor will remove in a broken link and undefined behaviour.
///
/// This invariant is implicit in the C code where its uncommon to move the heap allocated objects (which would
/// mean memcopying them around). Rust move semantics make it very easy to break this invariant however, which is why
/// all APIs will only ever return either immutable or pinned mutable references to the data structure.
///
/// This crate has to ensure that these pinning invariants are upheld internally, but when sending pointers through the FFI boundary
/// this is impossible to guarantee. Thankfully, as mentioned above its quite rare to move out of pointers, so its reasonably safe.
#[repr(C)]
#[derive(Debug)]
pub struct Header {
    /// Reference to the parent structure
    pub(super) parent: *mut c_void, // QueryIterator*

    /// Previous result processor in the chain
    pub(super) upstream: *mut Self,

    /// Type of result processor
    ty: ResultProcessorType,

    /// time measurements
    giltime: timespec,

    /// "VTable" function. Pulls SearchResults out of this result processor.
    pub(super) next: Option<ResultProcessorNext>,
    /// "VTable" function. Destroys and deallocates the result processor. This is only ever called by C code.
    free: Option<ResultProcessorFree>,

    // ResultProcessors *must* be !Unpin to ensure they are not moved about breaking the linked list.
    _unpin: PhantomPinned,
}

pin_project! {
    #[derive(Debug)]
    #[repr(C)]
    pub struct ResultProcessor<P> {
        header: Header,
        result_processor: P,
    }
}

impl<P> ResultProcessor<P>
where
    P: crate::ResultProcessor,
{
    /// Construct a new FFI-ResultProcessor from the provided [`crate::ResultProcessor`] implementer.
    pub fn new(result_processor: P) -> Self {
        Self {
            header: Header {
                parent: ptr::null_mut(),
                upstream: ptr::null_mut(),
                ty: P::TYPE,
                giltime: timespec {
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
        ptr: *mut Header,
        res: *mut MaybeUninit<SearchResult>,
    ) -> RPStatus {
        let ptr = NonNull::new(ptr).unwrap();
        debug_assert!(ptr.is_aligned());

        let mut res = NonNull::new(res).unwrap();
        debug_assert!(res.is_aligned());

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics on this type -
        // ensures that we can safely cast to `Self` here.
        let me = unsafe { ptr.cast::<Self>().as_mut() };
        // Safety: As this function is directly called C code, there are no hard guarantees. Moving out of pointers
        //  is luckily quite rare though, so we can be fairly confident that this is fine.
        let mut me = unsafe { Pin::new_unchecked(me) };
        let me = me.as_mut().project();

        let cx = crate::Context {
            // Safety: see the comment above
            result_processor: unsafe { Pin::new_unchecked(me.header) },
        };

        // Safety: We have done as much checking as we can at the start of the function (checking alignment & non-null-ness).
        let res = unsafe { res.as_mut() };

        match me.result_processor.next(cx, res) {
            Ok(Some(())) => RPStatus::RS_RESULT_OK,
            Ok(None) => RPStatus::RS_RESULT_EOF,
            Err(Error::Paused) => RPStatus::RS_RESULT_PAUSED,
            Err(Error::TimedOut) => RPStatus::RS_RESULT_TIMEDOUT,
            Err(Error::Error) => RPStatus::RS_RESULT_ERROR,
        }
    }

    unsafe extern "C" fn result_processor_free(ptr: *mut Header) {
        debug_assert!(ptr.is_aligned());

        // Safety: This function is called through the ResultProcessor "VTable" which - through the generics
        //  and constructor of this type - ensures that we can safely cast to `Self` here.
        //  For all other safety guarantees we have to trust the QueryIterator implementation to be correct.
        drop(unsafe { Self::from_ptr(ptr.cast::<Self>()) });
    }
}
