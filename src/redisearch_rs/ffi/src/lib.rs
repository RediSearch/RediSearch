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
    unsafe_op_in_unsafe_fn,
    clippy::ptr_offset_with_cast,
    clippy::upper_case_acronyms,
    clippy::useless_transmute,
    clippy::multiple_unsafe_ops_per_block,
    clippy::undocumented_unsafe_blocks,
    clippy::missing_safety_doc,
    clippy::len_without_is_empty,
    clippy::approx_constant,
    rustdoc::invalid_html_tags,
    rustdoc::broken_intra_doc_links
)]

use std::cell::UnsafeCell;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[repr(C)]
#[derive(Debug)]
pub struct QueryProcessingCtx {
    pub rootProc: UnsafeCell<*mut ResultProcessor>,
    pub endProc: UnsafeCell<*mut ResultProcessor>,
    pub sctx: *mut RedisSearchCtx,
    pub initTime: rs_wall_clock,
    pub GILTime: rs_wall_clock_ns_t,
    pub minScore: f64,
    pub totalResults: u32,
    pub resultLimit: u32,
    pub err: *mut QueryError,
    pub isProfile: bool,
    pub timeoutPolicy: RSTimeoutPolicy,
}

/// Rust implementation of `t_fieldMask` from `redisearch.h`
pub type FieldMask = t_fieldMask;

#[cfg(target_pointer_width = "64")]
pub const RS_FIELDMASK_ALL: FieldMask = u128::MAX;

#[cfg(target_pointer_width = "32")]
pub const RS_FIELDMASK_ALL: FieldMask = u64::MAX;
