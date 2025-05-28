/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use core::fmt;
use libc::{c_char, timespec};
use pin_project_lite::pin_project;
use std::{ffi::c_void, marker::PhantomPinned, mem::MaybeUninit, pin::Pin};

#[repr(C)]
#[derive(Default)]
pub struct SearchResult {
    // stub
    private: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryErrorCode {
    Ok = 0,
    Egeneric,
    Esyntax,
    Eparseargs,
    Eaddargs,
    Eexpr,
    Ekeyword,
    Enoresults,
    Ebadattr,
    Einval,
    Ebuildplan,
    EconstructPipeline,
    Enoreducer,
    EreducerGeneric,
    Eaggplan,
    Ecursoralloc,
    Ereducerinit,
    Eqstring,
    Enopropkey,
    Enopropval,
    Enodoc,
    Enooption,
    Erediskeytype,
    Einvalpath,
    Eindexexists,
    Ebadoption,
    Ebadorderoption,
    Elimit,
    Enoindex,
    Edocexists,
    Edocnotadded,
    Edupfield,
    Egeoformat,
    Enodistribute,
    Eunsupptype,
    Enotnumeric,
    Etimedout,
    Enoparam,
    Edupparam,
    Ebadval,
    Enhybrid,
    Ehybridnexist,
    Eadhocwbatchsize,
    Eadhocwefruntime,
    Enrange,
    Emissing,
    Emissmatch,
    Eunknownindex,
    Edroppedbackground,
    Ealiasconflict,
    Indexbgoomfail,
}

impl QueryErrorCode {
    pub fn message(&self) -> &'static str {
        match self {
            QueryErrorCode::Ok => "OK",
            QueryErrorCode::Egeneric => "Generic error evaluating the query",
            QueryErrorCode::Esyntax => "Parsing/Syntax error for query string",
            QueryErrorCode::Eparseargs => "Error parsing query/aggregation arguments",
            QueryErrorCode::Eaddargs => "Error parsing document indexing arguments",
            QueryErrorCode::Eexpr => "Parsing/Evaluating dynamic expression failed",
            QueryErrorCode::Ekeyword => "Could not handle query keyword",
            QueryErrorCode::Enoresults => "Query matches no results",
            QueryErrorCode::Ebadattr => "Attribute not supported for term",
            QueryErrorCode::Einval => "Could not validate the query nodes (bad attribute?)",
            QueryErrorCode::Ebuildplan => "Could not build plan from query",
            QueryErrorCode::EconstructPipeline => "Could not construct query pipeline",
            QueryErrorCode::Enoreducer => "Missing reducer",
            QueryErrorCode::EreducerGeneric => "Generic reducer error",
            QueryErrorCode::Eaggplan => "Could not plan aggregation request",
            QueryErrorCode::Ecursoralloc => "Could not allocate a cursor",
            QueryErrorCode::Ereducerinit => "Could not initialize reducer",
            QueryErrorCode::Eqstring => "Bad query string",
            QueryErrorCode::Enopropkey => "Property does not exist in schema",
            QueryErrorCode::Enopropval => "Value was not found in result (not a hard error)",
            QueryErrorCode::Enodoc => "Document does not exist",
            QueryErrorCode::Enooption => "Invalid option",
            QueryErrorCode::Erediskeytype => "Invalid Redis key",
            QueryErrorCode::Einvalpath => "Invalid path",
            QueryErrorCode::Eindexexists => "Index already exists",
            QueryErrorCode::Ebadoption => "Option not supported for current mode",
            QueryErrorCode::Ebadorderoption => {
                "Path with undefined ordering does not support slop/inorder"
            }
            QueryErrorCode::Elimit => "Limit exceeded",
            QueryErrorCode::Enoindex => "Index not found",
            QueryErrorCode::Edocexists => "Document already exists",
            QueryErrorCode::Edocnotadded => "Document was not added because condition was unmet",
            QueryErrorCode::Edupfield => "Field was specified twice",
            QueryErrorCode::Egeoformat => "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"",
            QueryErrorCode::Enodistribute => "Could not distribute the operation",
            QueryErrorCode::Eunsupptype => "Unsupported index type",
            QueryErrorCode::Enotnumeric => "Could not convert value to a number",
            QueryErrorCode::Etimedout => "Timeout limit was reached",
            QueryErrorCode::Enoparam => "Parameter not found",
            QueryErrorCode::Edupparam => "Parameter was specified twice",
            QueryErrorCode::Ebadval => "Invalid value was given",
            QueryErrorCode::Enhybrid => "hybrid query attributes were sent for a non-hybrid query",
            QueryErrorCode::Ehybridnexist => "invalid hybrid policy was given",
            QueryErrorCode::Eadhocwbatchsize => "'batch size' is irrelevant for 'ADHOC_BF' policy",
            QueryErrorCode::Eadhocwefruntime => "'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy",
            QueryErrorCode::Enrange => "range query attributes were sent for a non-range query",
            QueryErrorCode::Emissing => {
                "'ismissing' requires field to be defined with 'INDEXMISSING'"
            }
            QueryErrorCode::Emissmatch => {
                "Index mismatch: Shard index is different than queried index"
            }
            QueryErrorCode::Eunknownindex => "Unknown index name",
            QueryErrorCode::Edroppedbackground => {
                "The index was dropped before the query could be executed"
            }
            QueryErrorCode::Ealiasconflict => "Alias conflicts with an existing index name",
            QueryErrorCode::Indexbgoomfail => "Index background scan did not complete due to OOM",
        }
    }
}

impl fmt::Display for QueryErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct QueryError {
    code: QueryErrorCode,
    // The error message which we can expose in the logs, does not contain user data
    message: *const c_char,
    // The formatted error message in its entirety, can be shown only to the user
    detail: *mut c_char,
    // warnings
    reached_max_prefix_expansions: bool,
}

#[repr(C)]
#[derive(Debug)]
pub struct QueryIterator {
    // First processor
    root_proc: *mut ResultProcessor<()>,

    // Last processor
    end_proc: *mut ResultProcessor<()>,

    // Concurrent search context for thread switching
    conc: *mut c_void,

    // Contains our spec
    sctx: *mut c_void,

    init_time: timespec, //used with clock_gettime(CLOCK_MONOTONIC, ...)
    giltime: timespec,   //milliseconds

    // the minimal score applicable for a result. It can be used to optimize the
    // scorers
    min_score: f64,

    // the total results found in the query, incremented by the root processors
    // and decremented by others who might disqualify results
    total_results: u32,

    // the number of results we requested to return at the current chunk.
    // This value is meant to be used by the RP to limit the number of results
    // returned by its upstream RP ONLY.
    // It should be restored after using it for local aggregation etc., as done in
    // the Safe-Loader, Sorter, and Pager.
    result_limit: u32,

    // Object which contains the error
    pub err: *mut QueryError,

    is_profile: bool,
    // TODO RSTimeoutPolicy timeoutPolicy;
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
    unsafe extern "C" fn(*mut Header, res: *mut MaybeUninit<SearchResult>) -> RPStatus;

pub(crate) type FFIResultProcessorFree = unsafe extern "C" fn(*mut Header);

#[repr(C)]
#[derive(Debug)]
pub struct Header {
    /// Reference to the parent structure
    pub(super) parent: *mut QueryIterator,

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
                parent: 0xdeadbeef as *mut QueryIterator, // ptr::null_mut(),
                upstream: 0xbeefdead as *mut Header,      // ptr::null_mut(),
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

    // /// # Safety
    // ///
    // /// The caller must ensure the pointer was previously allocated through `Box::pin` and converted
    // /// into a pointer using `Box::into_raw`. Furthermore the pointer *must* be treated as pinned.
    // pub unsafe fn from_ptr(ptr: *mut Header) -> Pin<Box<Self>> {
    //     eprintln!("ResultProcessor::from_ptr self_ptr={ptr:p}");

    //     // Safety: TODO
    //     let b = unsafe { Box::from_raw(ptr.cast()) };
    //     eprintln!("ResultProcessor::from_ptr self_ptr={ptr:p} self={b:?}");

    //     // Safety: ensured by caller
    //     unsafe { Pin::new_unchecked(b) }
    // }

    unsafe extern "C" fn result_processor_next(
        ptr: *mut Header,
        res: *mut MaybeUninit<SearchResult>,
    ) -> RPStatus {
        eprintln!("ResultProcessor::result_processor_next ptr={ptr:p} out={res:p}");

        assert!(!ptr.is_null());
        assert!(!res.is_null());

        // Safety: TODO
        let mut me = unsafe { Pin::new_unchecked(ptr.cast::<Self>().as_mut().unwrap()) };
        let me = me.as_mut().project();

        let cx = crate::Context {
            rp: unsafe { Pin::new_unchecked(me.header) },
        };
        let res = unsafe { res.as_mut().unwrap() };

        match me.result_processor.next(cx, res) {
            Ok(()) => RPStatus::RS_RESULT_OK,
            Err(crate::Error::Eof) => RPStatus::RS_RESULT_EOF,
            Err(crate::Error::Paused) => RPStatus::RS_RESULT_PAUSED,
            Err(crate::Error::TimedOut) => RPStatus::RS_RESULT_TIMEDOUT,
            Err(crate::Error::Error) => RPStatus::RS_RESULT_ERROR,
        }
    }

    unsafe extern "C" fn result_processor_free(ptr: *mut Header) {
        eprintln!("ResultProcessor::result_processor_free");
        // Safety: TODO
        drop(unsafe { Pin::new_unchecked(Box::from_raw(ptr.cast::<Self>())) });
    }
}
