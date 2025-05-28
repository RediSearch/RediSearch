/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::c_void, marker::PhantomPinned, mem::MaybeUninit, pin::Pin, ptr::NonNull, time::Duration,
};

use pin_project_lite::pin_project;

#[repr(C)]
#[derive(Default)]
pub struct SearchResult {
    // stub
}

pub type FFIResultProcessorNext =
    unsafe extern "C" fn(*mut Header, res: NonNull<MaybeUninit<SearchResult>>) -> i32;

pub type FFIResultProcessorFree = unsafe extern "C" fn(*mut Header);

#[repr(C)]
pub struct Header {
    /// Reference to the parent structure
    /// TODO Check if Option needed
    pub(super) parent: Option<NonNull<c_void>>, // QueryIterator*

    /// Previous result processor in the chain
    pub(super) upstream: Option<NonNull<Self>>,

    /// Type of result processor
    ty: crate::ResultProcessorType,

    /// time measurements
    /// TODO find ffi type
    timespec: Duration,

    pub(super) next: FFIResultProcessorNext,
    free: FFIResultProcessorFree,

    // TODO check if we need to worry about <https://github.com/rust-lang/rust/issues/63818>
    _unpin: PhantomPinned,
}

pin_project! {
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
    pub fn new(result_processor: P) -> Self {
        // Must be kept in sync with the deallocation logic above
        // FIXME would be great if the Box::into_raw could also live here to mirror the Box::from_raw above...
        Self {
            header: Header {
                parent: None,
                upstream: None,
                ty: P::TYPE,
                timespec: Duration::ZERO,
                next: Self::result_processor_next,
                free: Self::result_processor_free,
                _unpin: PhantomPinned,
            },
            result_processor,
        }
    }

    /// # Safety
    ///
    /// The caller *must* continue to treat the pointer as pinned.
    pub unsafe fn into_ptr(me: Pin<Box<Self>>) -> *mut Self {
        // Safety: ensured by caller
        Box::into_raw(unsafe { Pin::into_inner_unchecked(me) })
    }

    /// # Safety
    ///
    /// The caller must ensure the pointer was previously allocated through `Box::pin` and converted
    /// into a pointer using `Box::into_raw`. Furthermore the pointer *must* be treated as pinned.
    pub unsafe fn from_ptr(ptr: *mut Header) -> Pin<Box<Self>> {
        // Safety: TODO
        let b = unsafe { Box::from_raw(ptr.cast()) };

        // Safety: ensured by caller
        unsafe { Pin::new_unchecked(b) }
    }

    unsafe extern "C" fn result_processor_next(
        ptr: *mut Header,
        mut out: NonNull<MaybeUninit<SearchResult>>,
    ) -> i32 {
        // Safety: TODO
        let mut me = unsafe { Self::from_ptr(ptr) };
        let me = me.as_mut().project();

        let cx = crate::Context { rp: me.header };

        match me.result_processor.next(cx) {
            Ok(Some(res)) => {
                // Safety: TODO
                unsafe { out.as_mut() }.write(res);
                0
            }
            Ok(None) => {
                // TODO convert into ret code
                1
            }
            Err(_) => {
                // TODO convert rust error into ret code
                1
            }
        }
    }

    unsafe extern "C" fn result_processor_free(ptr: *mut Header) {
        // Safety: TODO
        drop(unsafe { Self::from_ptr(ptr) });
    }
}
