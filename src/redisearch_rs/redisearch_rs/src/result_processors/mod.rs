use std::marker::PhantomPinned;

/// Possible types
#[repr(i32)]
#[non_exhaustive]
pub enum ResultProcessorType {
    RP_INDEX,
    RP_LOADER,
    RP_SAFE_LOADER,
    RP_SCORER,
    RP_SORTER,
    RP_COUNTER,
    RP_PAGER_LIMITER,
    RP_HIGHLIGHTER,
    RP_GROUP,
    RP_PROJECTOR,
    RP_FILTER,
    RP_PROFILE,
    RP_NETWORK,
    RP_METRICS,
    RP_KEY_NAME_LOADER,
    RP_MAX_SCORE_NORMALIZER,
    RP_TIMEOUT, // DEBUG ONLY
    RP_CRASH,   // DEBUG ONLY
    RP_MAX,
}

/// Possible return values from `ResultProcessorNext`
#[repr(i32)]
#[non_exhaustive]
pub enum RPStatus {
    /// Result is filled with valid data
    Ok = 0,
    /// Result is empty, and the last result has already been returned.
    Eof,
    /// Execution paused due to rate limiting (or manual pause from ext. thread??)
    Paused,
    /// Execution halted because of timeout
    Timedout,
    /// Aborted because of error. The QueryState (parent->status) should have
    /// more information.
    Error,
    /// Not a return code per se, but a marker signifying the end of the 'public'
    /// return codes. Implementations can use this for extensions.
    Max,
}

#[repr(C)]
pub struct SearchResult {}

pub type ResultProcessorNext = Option<
    unsafe extern "C" fn(
        this: *mut ResultProcessorHeader,
        search_result: *mut SearchResult,
    ) -> RPStatus,
>;

pub type ResultProcessorFree = Option<unsafe extern "C" fn(this: *mut ResultProcessorHeader)>;

extern "C" fn RPCounter_New() -> NonNull<ResultProcessorHeader> {
    todo!();
}
