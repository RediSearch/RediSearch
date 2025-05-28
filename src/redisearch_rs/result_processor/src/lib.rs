pub mod counter;
pub mod ffi;

use std::{ffi::c_void, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

pub enum Error {
    // stub
}

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
    rp: &'a mut ffi::Header,
}

impl Context<'_> {
    /// The QueryIterator that owns this result processor
    pub fn parent(&mut self) -> NonNull<c_void> {
        todo!()
    }

    /// The previous result processor in the pipeline
    pub fn upstream(&mut self) -> Option<Upstream<'_>> {
        let rp = self.rp.upstream?;

        Some(Upstream {
            rp,
            _m: PhantomData,
        })
    }
}

/// The previous result processor in the pipeline
pub struct Upstream<'a> {
    rp: NonNull<ffi::Header>,
    _m: PhantomData<&'a mut ffi::Header>,
}

impl Upstream<'_> {
    #[allow(clippy::should_implement_trait, reason = "yes thank you I know")]
    pub fn next(&mut self) -> Result<Option<ffi::SearchResult>, Error> {
        // Safety: TODO
        let next = unsafe { self.rp.as_mut() }.next;

        let mut res: MaybeUninit<ffi::SearchResult> = MaybeUninit::uninit();

        // Safety: TODO clarify safety constraints on `next`
        let ret = unsafe { next(self.rp.as_ptr(), NonNull::from(&mut res)) };

        const RS_RESULT_OK: i32 = 0;

        if ret == RS_RESULT_OK {
            // Safety: next returned `RS_RESULT_OK` and guarantees the ptr is "filled with valid data"
            Ok(Some(unsafe { res.assume_init() }))
        } else {
            todo!("map the return code to rust error")
        }
    }
}
