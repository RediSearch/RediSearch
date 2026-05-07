/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Reduce warnings for generated code.
#![allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    improper_ctypes,
    dead_code,
    unnecessary_transmutes,
    unsafe_op_in_unsafe_fn,
    clippy::allow_attributes,
    clippy::ptr_offset_with_cast,
    clippy::upper_case_acronyms,
    clippy::useless_transmute,
    clippy::multiple_unsafe_ops_per_block,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_safety_doc,
    clippy::len_without_is_empty,
    clippy::approx_constant,
    clippy::missing_const_for_fn,
    clippy::disallowed_types,
    rustdoc::invalid_html_tags,
    rustdoc::broken_intra_doc_links
)]

use std::{cell::UnsafeCell, pin::Pin, ptr};

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// Access to the RediSearch Module context
pub mod context;

/// Use the Rust definitions directly
pub use document::DocumentType;
pub use query_node_type::QueryNodeType;
pub use query_term::{RSQueryTerm, RSTokenFlags};
pub use rqe_iterator_type::IteratorType;

#[repr(C)]
#[derive(Debug)]
pub struct QueryProcessingCtx {
    /// First processor in the chain.
    pub rootProc: UnsafeCell<*mut ResultProcessor>,
    /// Last processor in the chain.
    pub endProc: UnsafeCell<*mut ResultProcessor>,
    /// Used with `clock_gettime(CLOCK_MONOTONIC, ...)`.
    pub initTime: rs_wall_clock,
    /// Time accumulated in nanoseconds.
    pub queryGILTime: rs_wall_clock_ns_t,
    /// The minimal score applicable for a result. It can be used to optimize
    /// the scorers.
    pub minScore: f64,
    /// The total results found in the query, incremented by the root
    /// processors and decremented by others who might disqualify results.
    pub totalResults: u32,
    /// The number of results we requested to return at the current chunk.
    /// This value is meant to be used by the RP to limit the number of results
    /// returned by its upstream RP ONLY.
    /// It should be restored after using it for local aggregation etc., as done
    /// in the Safe-Loader, Sorter, and Pager.
    pub resultLimit: u32,
    /// Object which contains the error.
    pub err: *mut QueryError,
    /// Background indexing OOM warning.
    pub bgScanOOM: bool,
    pub isProfile: bool,
    pub timeoutPolicy: RSTimeoutPolicy,
    /// True iff any prefix of the pipeline's output is a valid (though possibly
    /// incomplete) answer to the query - i.e. the pipeline can yield partial
    /// results on early termination.
    /// Set post-construction on the coordinator AREQ. Used by the
    /// RETURN-STRICT timeout path to drain queued shard replies on the main
    /// thread after the background pipeline has aborted.
    pub canYieldPartialResults: bool,
}

impl QueryProcessingCtx {
    pub fn new() -> Pin<Box<Self>> {
        let ctx = Self {
            rootProc: UnsafeCell::new(ptr::null_mut()),
            endProc: UnsafeCell::new(ptr::null_mut()),
            initTime: timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            queryGILTime: 0,
            minScore: 0.0,
            totalResults: 0,
            resultLimit: 0,
            err: ptr::null_mut(),
            bgScanOOM: false,
            isProfile: false,
            timeoutPolicy: 0,
            canYieldPartialResults: false,
        };

        Box::pin(ctx)
    }

    pub fn append_raw(self: &mut Pin<Box<Self>>, result_processor_ptr: *mut ResultProcessor) {
        if unsafe { *self.rootProc.get() }.is_null() {
            unsafe { *self.rootProc.get() = result_processor_ptr };
        }

        unsafe { *self.endProc.get() = result_processor_ptr };
    }
}

/// Rust implementation of `t_fieldMask` from `redisearch.h`
pub type FieldMask = t_fieldMask;

#[cfg(target_pointer_width = "64")]
pub const RS_FIELDMASK_ALL: FieldMask = u128::MAX;

#[cfg(target_pointer_width = "32")]
pub const RS_FIELDMASK_ALL: FieldMask = u64::MAX;

pub const RS_INVALID_FIELD_INDEX: t_fieldIndex = 0xFFFF;
