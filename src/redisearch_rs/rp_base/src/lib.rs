#[repr(C)]
pub struct ResultProcessorHeader {
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

    next_ftor: ResultProcessorNext,
    free_ftor: ResultProcessorFree,

    // TODO check if we need to worry about <https://github.com/rust-lang/rust/issues/63818>
    _unpin: PhantomPinned,
}

// ===== POC =====

enum Error {
    // Result is empty, and the last result has already been returned.
    EOF,
    // Execution paused due to rate limiting (or manual pause from ext. thread??)
    PAUSED,
    // Execution halted because of timeout
    TIMEDOUT,
    // Aborted because of error. The QueryState (parent->status) should have
    // more information.
    ERROR,
    // Not a return code per se, but a marker signifying the end of the 'public'
    // return codes. Implementations can use this for extensions.
    MAX,
}

pub trait ResultProcessor {
    fn next(&mut self) -> Result<SearchResult, Error>;
}

pub struct RPCounter {
    count: u32,

    base: ResultProcssorHeader,
}
