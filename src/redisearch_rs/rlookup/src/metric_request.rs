/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, ptr::NonNull};

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

/// The iterator side of an [`RLookupKeyHandle`]: the handle, if any, that the
/// owning iterator invalidates when it lets go of it.
///
/// Attaching a different handle or detaching the current one invalidates the
/// previous handle, and dropping the slot invalidates the current one, so a
/// handle never reads as valid after its iterator stopped tracking it.
///
/// `#[repr(transparent)]` so it occupies exactly the space of the
/// `Option<NonNull<_>>` it wraps.
///
/// # Invariants
///
/// The slot holds either [`None`], or a pointer to an initialized
/// [`RLookupKeyHandle`] that remains alive until the slot is dropped or the
/// handle is replaced.
#[repr(transparent)]
#[derive(Debug, Default)]
pub struct KeyHandleSlot<'a>(Option<NonNull<RLookupKeyHandle<'a>>>);

impl<'a> KeyHandleSlot<'a> {
    /// Create a slot with no handle attached.
    pub const fn new() -> Self {
        Self(None)
    }

    /// Attach `handle`, invalidating any different previously attached handle.
    ///
    /// Attaching the handle that is already attached is a no-op.
    ///
    /// # Safety
    ///
    /// 1. `handle` is [`None`], or points to an initialized [`RLookupKeyHandle`]
    ///    that remains alive until this slot is dropped or the handle is
    ///    replaced.
    /// 2. The `is_valid` field of the previously attached handle, if it differs
    ///    from `handle`, must be writable exclusively during this call.
    /// 3. The `is_valid` field of `handle` must be writable exclusively whenever
    ///    the slot invalidates it: when the slot is dropped or the handle is
    ///    replaced.
    pub unsafe fn set(&mut self, handle: Option<NonNull<RLookupKeyHandle<'a>>>) {
        if self.0 != handle {
            self.invalidate();
            self.0 = handle;
        }
    }

    /// Clear the attached handle's validity flag and detach it.
    const fn invalidate(&mut self) {
        if let Some(handle) = self.0.take() {
            // SAFETY: the type invariant keeps the handle initialized and alive
            // until now, and the setter's contract grants exclusive write access
            // to `is_valid`. Writing through the raw place creates no reference,
            // so other live pointers to the handle are not invalidated.
            unsafe { (*handle.as_ptr()).is_valid = false };
        }
    }
}

impl Drop for KeyHandleSlot<'_> {
    fn drop(&mut self) {
        self.invalidate();
    }
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
