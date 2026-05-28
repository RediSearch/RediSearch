/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_char;

#[cfg(doc)]
use crate::RLookup;
use crate::RLookupKey;

/// Smart pointer handle for [`RLookupKey`] that can be
/// invalidated when the iterator that owns the key is freed.
#[cheadergen::config(export)]
#[repr(C)]
#[derive(Debug)]
pub struct RLookupKeyHandle<'a> {
    /// Pointer to the [`RLookupKey`] pointer field inside
    /// the owning iterator.
    pub key_ptr: *mut *mut RLookupKey<'a>,
    /// Whether the owning iterator is still alive. Set to `true` on
    /// creation and cleared to `false` when the iterator is freed.
    pub is_valid: bool,
}

/// A deferred binding between a metric name produced during query parsing
/// and the [`RLookupKey`] that will be resolved during
/// pipeline construction.
#[cheadergen::config(export)]
#[repr(C)]
#[derive(Debug)]
pub struct MetricRequest<'a> {
    /// The name of the metric field to register in the
    /// [`RLookup`] table (e.g. `"__vec_score"`).
    pub metric_name: *const c_char,
    /// Optional handle back to the iterator's
    /// [`RLookupKey`] slot. `NULL` when the iterator
    /// that requested this metric was not created (e.g. an early
    /// empty-result short-circuit).
    pub key_handle: *mut RLookupKeyHandle<'a>,
    /// When `true`, the metric is excluded from the query response
    /// (the corresponding [`RLookupKey`] is created
    /// with the `HIDDEN` flag).
    #[cheadergen(rename = "isInternal")]
    pub is_internal: bool,
}
