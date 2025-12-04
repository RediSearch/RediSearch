/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{Context, Error, ResultProcessor, ResultProcessorWrapper};
use search_result::SearchResult;
use std::{pin::Pin, ptr::NonNull};

/// Create a ResultProcessor from an `Iterator` for testing purposes
pub fn from_iter<I>(i: I) -> IterResultProcessor<I::IntoIter>
where
    I: IntoIterator<Item = SearchResult<'static>>,
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
    I: Iterator<Item = SearchResult<'static>>,
{
    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX + 1;

    fn next(&mut self, _cx: Context, out: &mut SearchResult) -> Result<Option<()>, Error> {
        if let Some(res) = self.iter.next() {
            *out = res;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
}

/// A result processor that returns the provided result.
pub struct ResultRP {
    res: Option<Result<Option<()>, Error>>,
}
impl ResultRP {
    pub fn new_err(error: Error) -> Self {
        Self {
            res: Some(Err(error)),
        }
    }
    pub fn new_ok_some() -> Self {
        Self {
            res: Some(Ok(Some(()))),
        }
    }
    pub fn new_ok_none() -> Self {
        Self {
            res: Some(Ok(None)),
        }
    }
}
impl ResultProcessor for ResultRP {
    const TYPE: ffi::ResultProcessorType = ffi::ResultProcessorType_RP_MAX;

    fn next(&mut self, _cx: Context, _res: &mut SearchResult) -> Result<Option<()>, Error> {
        self.res.take().unwrap()
    }
}

/// A mock result processor of a fixed type, that just calls it's upstream's `next`.
pub struct MockResultProcessor<const TYPE: ffi::ResultProcessorType>;

impl<const RP_TYPE: ffi::ResultProcessorType> MockResultProcessor<RP_TYPE> {
    pub fn new() -> Self {
        Self
    }
}

impl<const RP_TYPE: ffi::ResultProcessorType> ResultProcessor for MockResultProcessor<RP_TYPE> {
    const TYPE: ffi::ResultProcessorType = RP_TYPE;

    fn next(&mut self, mut cx: Context, res: &mut SearchResult) -> Result<Option<()>, Error> {
        let Some(mut upstream) = cx.upstream() else {
            return Ok(None);
        };

        upstream.next(res)
    }
}

/// A mock implementation of the "result processor chain" part of the `QueryIterator`
///
/// It acts as an owning collection of linked result processors.
pub struct Chain {
    result_processors: Vec<NonNull<crate::Header>>,
    query_processing_context: Pin<Box<ffi::QueryProcessingCtx>>,
}

impl Chain {
    pub fn new() -> Self {
        Self {
            result_processors: Vec::new(),
            query_processing_context: ffi::QueryProcessingCtx::new(),
        }
    }

    /// Append a new result processor at the end of the chain. It will have its `upstream`
    /// field set to the previous last result processor.
    pub fn append<P>(&mut self, result_processor: P)
    where
        P: ResultProcessor + 'static,
    {
        let mut result_processor = ResultProcessorWrapper::new(result_processor);
        result_processor.header.parent = &raw const *self.query_processing_context;

        if let Some(upstream) = self.result_processors.last() {
            result_processor.header.upstream = upstream.as_ptr();
        }

        // Safety: We treat this pointer as pinned and never hand out mutable references that would allow
        // moving out of the type.
        let header_ptr: NonNull<crate::Header> =
            unsafe { ResultProcessorWrapper::into_ptr(Box::pin(result_processor)).cast() };
        self.result_processors.push(header_ptr);

        // Safety: ResultProcessorWrapper's layout is compatible with ffi::ResultProcessor.
        let result_processor_ptr: *mut ffi::ResultProcessor = unsafe { header_ptr.cast().as_mut() };

        self.query_processing_context
            .append_raw(result_processor_ptr)
    }

    /// Append a new result processor at the end of the chain. It will have its `upstream`
    /// field set to the previous last result processor.
    ///
    /// # Safety
    ///
    /// The caller has to ensure that the given pointer dereferences to a valid result processor.
    pub unsafe fn push_raw(&mut self, mut result_processor: NonNull<crate::Header>) {
        if let Some(upstream) = self.result_processors.last() {
            unsafe {
                result_processor.as_mut().upstream = upstream.as_ptr();
            }
        }

        self.result_processors.push(result_processor);

        // Safety: ResultProcessorWrapper's layout is compatible with ffi::ResultProcessor.
        let result_processor_ptr: *mut ffi::ResultProcessor =
            unsafe { result_processor.cast().as_mut() };

        self.query_processing_context
            .append_raw(result_processor_ptr)
    }

    /// Return a raw `NonNull` ptr to the last result processor in the chain
    ///
    /// # Safety
    ///
    /// 1. The caller must treat the returned pointer as pinned
    pub unsafe fn last_raw(&mut self) -> &mut NonNull<crate::Header> {
        self.result_processors
            .last_mut()
            .expect("empty result processor chain")
    }

    /// Return a [`Context`] and mutable reference to the inner [`ResultProcessor`] implementation
    /// from the last result processor in the chain.
    ///
    /// The caller has to provide the expected type of the inner result processor through the `P` generic.
    ///
    /// # Panics
    ///
    /// Panics if the last result processors inner implementation is not of the expected type.
    pub fn last_as_context_and_inner<P>(&mut self) -> (Context<'_>, &mut P)
    where
        P: ResultProcessor + 'static,
    {
        // Safety: Context treats the pointer as pinned
        let ptr = unsafe { self.last_raw() };

        // Safety:
        // 1. ptr is non-null
        // 2. ptr is well-aligned, and valid either because we took it from a `Pin<Box<P>>` (`Self::push`)
        //    or because the caller promised it as part of an unsafe contract (`Self::push_raw`).
        unsafe {
            ResultProcessorWrapper::<P>::debug_assert_same_type(*ptr);
        }

        // Safety: The assert above ensures this is always of the right type
        let result_processor =
            unsafe { Pin::new_unchecked(ptr.cast::<ResultProcessorWrapper<P>>().as_mut()) };
        let result_processor = result_processor.project();

        let cx = Context::new(result_processor.header);
        let result_processor = result_processor.result_processor;

        (cx, result_processor)
    }
}

impl Drop for Chain {
    fn drop(&mut self) {
        for mut ptr in self.result_processors.drain(..) {
            unsafe { (ptr.as_mut().free.unwrap())(ptr.as_ptr()) }
        }
    }
}

/// Mock implementation of `DMD_Free` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn DMD_Free(_cmd: *const ffi::RSDocumentMetadata) {
    unreachable!()
}

/// Mock implementation of `RSValue_Decref` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn RSValue_Decref(_v: *mut ffi::RSValue) {
    unreachable!()
}

/// Mock implementation of `SEDestroy` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn SEDestroy(_scr_exp: *mut ffi::RSScoreExplain) {
    unreachable!()
}

/// Mock implementation of `RLookupRow_Wipe` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Reset(_row: *mut ffi::RLookupRow) {
    // unreachable!()
}

/// Mock implementation of `RLookupRow_Wipe` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Wipe(_row: *mut ffi::RLookupRow) {
    // unreachable!()
}
