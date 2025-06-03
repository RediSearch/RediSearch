/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

pub mod ffi;

use std::{
    mem::MaybeUninit,
    pin::Pin,
    ptr::{self},
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
    /// The type of this result processsor.
    const TYPE: ffi::ResultProcessorType;

    /// Pull the next [`SearchResult`] from this result processor into the provided `res` location.
    ///
    /// Should return `Ok(Some(()))` if a search result was successfully pulled from the processor
    /// and `Ok(None)` to indicate the end of search results has been reached.
    /// `Err(_)` cases should be returned to indicate expectional error.
    fn next(
        &mut self,
        cx: Context,
        res: &mut MaybeUninit<ffi::SearchResult>,
    ) -> Result<Option<()>, Error>;
}

/// This type allows result processors to access its context (the owning QueryIterator, upstream result processors, etc.)
pub struct Context<'a> {
    result_processor: Pin<&'a mut ffi::Header>,
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
    result_processor: Pin<&'a mut ffi::Header>,
}

impl Upstream<'_> {
    /// Pull the next [`SearchResult`] from this result processor into the provided `res` location.
    ///
    /// Returns `Ok(Some(()))` if a search result was successfully pulled from the processor
    /// and `Ok(None)` to indicate the end of search results has been reached.
    ///
    /// # Errors
    ///
    /// Returns `Err(_)` for expectional error cases.
    pub fn next(&mut self, res: &mut MaybeUninit<ffi::SearchResult>) -> Result<Option<()>, Error> {
        let next = self.result_processor.next.unwrap();

        // Safety: The `next` function is required to treat the `*mut Header` pointer as pinned.
        let result_processor = unsafe { self.result_processor.as_mut().get_unchecked_mut() };
        // Safety: At the end of the day we're calling to arbitrary code at this point... But provided
        // the QueryIterator and other result processors are implemented correctly, this should be safe.
        let ret_code = unsafe { next(ptr::from_mut(result_processor), res) };

        match ret_code {
            ffi::RPStatus::RS_RESULT_OK => Ok(Some(())),
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
