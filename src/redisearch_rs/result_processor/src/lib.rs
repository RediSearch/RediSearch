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
    mem::MaybeUninit,
    pin::Pin,
    ptr::{self, NonNull},
};

use crate::ffi::QueryIterator;

#[derive(Debug)]
pub enum Error {
    Eof,
    Paused,
    TimedOut,
    Error,
}

#[derive(Debug)]
#[repr(C)]
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

    fn next(&mut self, cx: Context, res: &mut MaybeUninit<ffi::SearchResult>) -> Result<(), Error>;
}

/// This type allows result processors to access its context (the owning QueryIterator, upstream result processors, etc.)
pub struct Context<'a> {
    rp: Pin<&'a mut ffi::Header>,
}

impl Context<'_> {
    /// The QueryIterator that owns this result processor
    pub fn parent(&mut self) -> NonNull<QueryIterator> {
        NonNull::new(self.rp.parent).unwrap()
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
    pub fn next(&mut self, res: &mut MaybeUninit<ffi::SearchResult>) -> Result<(), Error> {
        eprintln!(
            "Upstream::next self_addr={:p} self={self:?} self.parent={:?} self.parent.err={:?}",
            self.rp,
            unsafe { self.rp.parent.as_ref() },
            unsafe { self.rp.parent.as_ref().unwrap().err.as_ref() }
        );

        let next = self.rp.next;

        // Safety: TODO clarify safety constraints on `next`
        let ret = unsafe { next(ptr::from_mut(self.rp.as_mut().get_unchecked_mut()), res) };

        eprintln!("C rp retcode {ret:?}");
        match ret {
            ffi::RPStatus::RS_RESULT_OK => Ok(()),
            ffi::RPStatus::RS_RESULT_EOF => Err(Error::Eof),
            ffi::RPStatus::RS_RESULT_PAUSED => Err(Error::Paused),
            ffi::RPStatus::RS_RESULT_TIMEDOUT => Err(Error::TimedOut),
            ffi::RPStatus::RS_RESULT_ERROR => Err(Error::Error),
            ffi::RPStatus(code) => {
                unimplemented!("result processor returned unknown error code {code}")
            }
        }
    }
}
