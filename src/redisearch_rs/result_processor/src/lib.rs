pub mod counter;

use std::{ffi::c_void, marker::PhantomPinned, mem::MaybeUninit, ptr::NonNull, time::Duration};

#[derive(Default)]
pub struct SearchResult {
    // stub
}

pub enum Error {
    // stub

    // Result is empty, and the last result has already been returned.
    //     EOF,
    //     // Execution paused due to rate limiting (or manual pause from ext. thread??)
    //     PAUSED,
    //     // Execution halted because of timeout
    //     TIMEDOUT,
    //     // Aborted because of error. The QueryState (parent->status) should have
    //     // more information.
    //     ERROR,
    //     // Not a return code per se, but a marker signifying the end of the 'public'
    //     // return codes. Implementations can use this for extensions.
    //     MAX,
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait ResultProcessor {
    fn next(&mut self) -> Result<Option<SearchResult>>;
}

// ===== Types to remove when all ResultProcessors are rewritten to Rust =====

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

pub type FFIResultProcessorNext =
    unsafe extern "C" fn(NonNull<Header>, res: NonNull<MaybeUninit<SearchResult>>) -> i32;

pub type FFIResultProcessorFree = unsafe extern "C" fn(NonNull<Header>);

#[repr(C)]
pub struct Header {
    /// Reference to the parent structure
    /// TODO Check if Option needed
    parent: Option<NonNull<c_void>>, // QueryIterator*

    /// Previous result processor in the chain
    upstream: Option<NonNull<Self>>,

    /// Type of result processor
    ty: ResultProcessorType,

    /// time measurements
    /// TODO find ffi type
    timespec: Duration,

    next: FFIResultProcessorNext,
    free: FFIResultProcessorFree,

    // TODO check if we need to worry about <https://github.com/rust-lang/rust/issues/63818>
    _unpin: PhantomPinned,
}

impl Header {
    pub const fn new(
        ty: ResultProcessorType,
        next: FFIResultProcessorNext,
        free: FFIResultProcessorFree,
    ) -> Self {
        Self {
            parent: None,
            upstream: None,
            ty,
            timespec: Duration::ZERO,
            next,
            free,
            _unpin: PhantomPinned,
        }
    }

    pub fn upstream(&mut self) -> Option<Upstream> {
        Some(Upstream {
            hdr: self.upstream?,
        })
    }
}

pub struct Upstream {
    hdr: NonNull<Header>,
}

impl ResultProcessor for Upstream {
    fn next(&mut self) -> Result<Option<SearchResult>> {
        let next = unsafe { self.hdr.as_mut() }.next;

        let mut res: MaybeUninit<SearchResult> = MaybeUninit::uninit();

        let ret = unsafe { next(NonNull::from(self.hdr), NonNull::from(&mut res)) };

        const RS_RESULT_OK: i32 = 0;

        if ret == RS_RESULT_OK {
            // Safety: next returned `RS_RESULT_OK` and guarantees the ptr is "filled with valid data"
            Ok(Some(unsafe { res.assume_init() }))
        } else {
            todo!("map the return code to rust error")
        }
    }
}
