/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CStr, ptr::NonNull};

use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags};

/// Smart pointer handle for [`RLookupKey`] that can be
/// invalidated when the iterator that owns the key is freed.
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
#[derive(Debug)]
pub struct MetricRequest<'a> {
    metric_name: &'a CStr,
    /// A leaked [`Box`], reclaimed when the request drops.
    ///
    /// Kept as a raw pointer because the iterator the handle is installed on
    /// writes through its own copy of it, which a live `Box` — asserting
    /// unique access to its pointee — does not allow.
    key_handle: Option<NonNull<RLookupKeyHandle<'a>>>,
    is_internal: bool,
}

impl<'a> MetricRequest<'a> {
    /// The name of the metric field to register in the [`RLookup`] table
    /// (e.g. `"__vec_score"`).
    pub const fn metric_name(&self) -> &'a CStr {
        self.metric_name
    }

    /// The handle back to the iterator's [`RLookupKey`] slot, or [`None`] when
    /// the iterator that requested this metric was not created (e.g. an early
    /// empty-result short-circuit).
    pub const fn key_handle(&self) -> Option<NonNull<RLookupKeyHandle<'a>>> {
        self.key_handle
    }

    /// Whether the metric is excluded from the query response, by creating its
    /// [`RLookupKey`] with [`RLookupKeyFlag::Hidden`].
    pub const fn is_internal(&self) -> bool {
        self.is_internal
    }
}

impl Drop for MetricRequest<'_> {
    fn drop(&mut self) {
        if let Some(handle) = self.key_handle {
            // SAFETY: `MetricRequests::bind` leaked the handle's `Box`, and
            // this request is its only owner.
            drop(unsafe { Box::from_raw(handle.as_ptr()) });
        }
    }
}

/// The metric requests of one query, in the order its nodes reserved them.
#[derive(Debug, Default)]
pub struct MetricRequests<'a> {
    requests: Vec<MetricRequest<'a>>,
}

/// Why [`MetricRequests::register_keys`] refused a metric.
#[derive(Debug, PartialEq, Eq)]
pub enum MetricKeyError<'a> {
    /// The metric is named after a field of the index schema.
    InSchema(&'a CStr),
    /// The lookup already has a key by the metric's name.
    Duplicate(&'a CStr),
}

impl<'a> MetricRequests<'a> {
    /// Reserve a request for `metric_name`, returning its index.
    ///
    /// The request starts unbound; [`bind`](Self::bind) gives it a key handle
    /// once the iterator that yields the metric exists.
    pub fn push(&mut self, metric_name: &'a CStr, is_internal: bool) -> usize {
        self.requests.push(MetricRequest {
            metric_name,
            key_handle: None,
            is_internal,
        });
        self.requests.len() - 1
    }

    /// Allocate the key handle of the request at `index`, pointing at
    /// `key_ptr`, and return it for the iterator that owns `key_ptr`.
    ///
    /// The handle is owned by the list, so its address stays fixed for as long
    /// as the iterator may write through it.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds, or if the request is already bound:
    /// replacing its handle would free one an iterator may still hold.
    ///
    /// # Safety
    ///
    /// 1. `key_ptr` must be [valid] for writes until the returned handle's
    ///    [`is_valid`](RLookupKeyHandle::is_valid) is cleared, because
    ///    [`register_keys`](Self::register_keys) writes the resolved key
    ///    through it whenever the flag is still set. For a slot inside an
    ///    iterator, that means installing the handle on that iterator before
    ///    it can be freed, since clearing the flag is the iterator's job.
    /// 2. The returned handle must not be used after the list is dropped, which
    ///    frees it. In particular, every iterator holding it must be freed
    ///    first, since an iterator clears the flag on its way out.
    /// 3. Every [`RLookup`] that [`register_keys`](Self::register_keys) is
    ///    later called with must outlive every iterator bound to the list,
    ///    since the iterator dereferences the key written into its slot.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn bind(
        &mut self,
        index: usize,
        key_ptr: *mut *mut RLookupKey<'a>,
    ) -> NonNull<RLookupKeyHandle<'a>> {
        debug_assert!(
            !key_ptr.is_null(),
            "a bound metric request must have a key slot"
        );
        let request = &mut self.requests[index];
        assert!(
            request.key_handle.is_none(),
            "a metric request is bound at most once"
        );
        let handle = NonNull::from(Box::leak(Box::new(RLookupKeyHandle {
            key_ptr,
            is_valid: true,
        })));
        request.key_handle = Some(handle);
        handle
    }

    /// The requests reserved so far, in reservation order.
    pub fn as_slice(&self) -> &[MetricRequest<'a>] {
        &self.requests
    }

    /// Register every metric as a key of `lookup`, and point each iterator that
    /// is still alive at its key.
    ///
    /// A request whose iterator was never built still gets its key: the query
    /// is valid, so later stages (a sorter, for one) must still be able to name
    /// the field.
    ///
    /// # Errors
    ///
    /// Stops at the first metric named after a field `is_schema_field` claims,
    /// or after a key `lookup` already has. The keys registered before it stay.
    pub fn register_keys(
        &self,
        lookup: &mut RLookup<'a>,
        mut is_schema_field: impl FnMut(&CStr) -> bool,
    ) -> Result<(), MetricKeyError<'a>> {
        for request in &self.requests {
            let name = request.metric_name;
            if is_schema_field(name) {
                return Err(MetricKeyError::InSchema(name));
            }
            let flags = if request.is_internal {
                RLookupKeyFlag::Hidden.into()
            } else {
                RLookupKeyFlags::empty()
            };
            let key = lookup
                .get_key_write_ptr(name, flags)
                .ok_or(MetricKeyError::Duplicate(name))?;

            let Some(handle) = request.key_handle else {
                continue;
            };
            // SAFETY: the handle is a live allocation owned by `request`.
            let handle = unsafe { handle.as_ref() };
            if handle.is_valid {
                // SAFETY: precondition (1) of `bind` keeps `key_ptr` valid for
                // writes while the flag is set.
                unsafe { *handle.key_ptr = key.as_ptr() };
            }
        }
        Ok(())
    }
}
