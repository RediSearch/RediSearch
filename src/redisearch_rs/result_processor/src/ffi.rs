/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::timespec;
use pin_project_lite::pin_project;
use std::{ffi::c_void, marker::PhantomPinned, pin::Pin};

#[repr(C)]
#[derive(Default)]
pub struct SearchResult {
    // stub
    private: [u8; 0],
}

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

pub(crate) type FFIResultProcessorNext =
    unsafe extern "C" fn(*mut Header, res: *mut SearchResult) -> RPStatus;

pub(crate) type FFIResultProcessorFree = unsafe extern "C" fn(*mut Header);

#[repr(C)]
#[derive(Debug)]
pub struct Header {
    /// Reference to the parent structure
    pub(super) parent: *mut c_void, // QueryIterator*

    /// Previous result processor in the chain
    pub(super) upstream: *mut Self,

    /// Type of result processor
    ty: crate::ResultProcessorType,

    /// time measurements
    GILTime: timespec,

    pub(super) next: FFIResultProcessorNext,
    free: FFIResultProcessorFree,

    // TODO check if we need to worry about <https://github.com/rust-lang/rust/issues/63818>
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
    P: crate::ResultProcessor + std::fmt::Debug,
{
    pub fn new(result_processor: P) -> Self {
        eprintln!("ResultProcessor::new");

        // Must be kept in sync with the deallocation logic above
        // FIXME would be great if the Box::into_raw could also live here to mirror the Box::from_raw above...
        Self {
            header: Header {
                parent: 0xdeadbeef as *mut c_void,   // ptr::null_mut(),
                upstream: 0xbeefdead as *mut Header, // ptr::null_mut(),
                ty: P::TYPE,
                GILTime: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
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
        eprintln!("ResultProcessor::into_ptr me_addr={me:p} me={me:?}");

        // Safety: ensured by caller
        Box::into_raw(unsafe { Pin::into_inner_unchecked(me) })
    }

    /// # Safety
    ///
    /// The caller must ensure the pointer was previously allocated through `Box::pin` and converted
    /// into a pointer using `Box::into_raw`. Furthermore the pointer *must* be treated as pinned.
    pub unsafe fn from_ptr(ptr: *mut Header) -> Pin<Box<Self>> {
        eprintln!("ResultProcessor::from_ptr self_ptr={ptr:p}");

        // Safety: TODO
        let b = unsafe { Box::from_raw(ptr.cast()) };
        eprintln!("ResultProcessor::from_ptr self_ptr={ptr:p} self={b:?}");

        // Safety: ensured by caller
        unsafe { Pin::new_unchecked(b) }
    }

    unsafe extern "C" fn result_processor_next(
        ptr: *mut Header,
        out: *mut SearchResult,
    ) -> RPStatus {
        eprintln!("ResultProcessor::result_processor_next ptr={ptr:p} out={out:p}");

        assert!(!ptr.is_null());
        assert!(!out.is_null());

        // Safety: TODO
        let mut me = unsafe { Self::from_ptr(ptr) };
        let me = me.as_mut().project();

        let cx = crate::Context {
            rp: unsafe { Pin::new_unchecked(me.header) },
        };

        match me.result_processor.next(cx) {
            Ok(Some(res)) => {
                unsafe {
                    out.write(res);
                }

                RPStatus::RS_RESULT_OK
            }
            Ok(None) => RPStatus::RS_RESULT_EOF,
            Err(_) => RPStatus::RS_RESULT_ERROR,
        }
    }

    unsafe extern "C" fn result_processor_free(ptr: *mut Header) {
        eprintln!("ResultProcessor::result_processor_free");

        // Safety: TODO
        drop(unsafe { Self::from_ptr(ptr) });
    }
}
