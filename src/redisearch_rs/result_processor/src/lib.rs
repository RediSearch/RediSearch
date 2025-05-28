/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod counter;
pub mod ffi;

use std::{
    ffi::c_void,
    mem::MaybeUninit,
    pin::Pin,
    ptr::{self, NonNull},
};

#[derive(Debug)]
pub enum Error {
    Paused,
    TimedOut,
    Error,
}

#[derive(Debug)]
#[repr(i32)]
#[non_exhaustive]
pub enum ResultProcessorType {
    Index,
    Loader,
    SafeLoader,
    Scorer,
    Sorter,
    Counter,
    PageLimiter,
    Highlighter,
    Group,
    Projector,
    Filter,
    Profile,
    Network,
    Metrics,
    KeyNameLoader,
    MaxScoreNormalizer,
    Timeout, // DEBUG ONLY
    Crash,   // DEBUG ONLY
    Max,
}

/// This is the main trait that Rust result processors need to implement
pub trait ResultProcessor {
    const TYPE: ResultProcessorType;

    fn next(&mut self, cx: Context) -> Result<Option<ffi::SearchResult>, Error>;
}

/// This type allows result processors to access its context (the owning QueryIterator, upstream result processors, etc.)
pub struct Context<'a> {
    rp: Pin<&'a mut ffi::Header>,
}

impl Context<'_> {
    /// The QueryIterator that owns this result processor
    pub fn parent(&mut self) -> NonNull<c_void> {
        todo!()
    }

    /// The previous result processor in the pipeline
    pub fn upstream(&mut self) -> Option<Upstream<'_>> {
        let rp = unsafe { Pin::new_unchecked(self.rp.upstream.as_mut()?) };

        Some(Upstream { rp })
    }
}

/// The previous result processor in the pipeline
#[derive(Debug)]
pub struct Upstream<'a> {
    rp: Pin<&'a mut ffi::Header>,
}

impl Upstream<'_> {
    #[allow(clippy::should_implement_trait, reason = "yes thank you I know")]
    pub fn next(&mut self) -> Result<Option<ffi::SearchResult>, Error> {
        eprintln!("Upstream::next self_addr={:p} self={self:?}", self.rp);

        let next = self.rp.next;
        let mut res: MaybeUninit<ffi::SearchResult> = MaybeUninit::uninit();

        // Safety: TODO clarify safety constraints on `next`
        let ret = unsafe {
            next(
                ptr::from_mut(self.rp.as_mut().get_unchecked_mut()),
                res.as_mut_ptr(),
            )
        };

        match ret {
            ffi::RPStatus::RS_RESULT_OK => Ok(Some(unsafe { res.assume_init() })),
            ffi::RPStatus::RS_RESULT_EOF => Ok(None),
            ffi::RPStatus::RS_RESULT_PAUSED => Err(Error::Paused),
            ffi::RPStatus::RS_RESULT_TIMEDOUT => Err(Error::TimedOut),
            ffi::RPStatus::RS_RESULT_ERROR => Err(Error::Error),
            ffi::RPStatus(code) => {
                unimplemented!("result processor returned unknown error code {code}")
            }
        }
    }
}
