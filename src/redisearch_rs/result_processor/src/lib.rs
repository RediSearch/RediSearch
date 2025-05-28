pub mod counter;
pub mod header;
pub mod upstream;

use upstream::Upstream;

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
    fn type_() -> ResultProcessorType;
    fn next(&mut self, upstream: Option<Upstream>) -> Result<Option<SearchResult>>;
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
