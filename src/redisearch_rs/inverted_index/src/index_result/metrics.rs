/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Yieldable metrics attached to query results.
//!
//! A [`MetricsVec`] holds a dynamic array of [`MetricEntry`] values, each
//! pairing a borrowed [`RLookupKey`] with an owned [`SharedRsValue`].
//!
//! Backed by [`ThinVec`] (pointer-sized, no-alloc on construction),
//! `MetricsVec` is `repr(transparent)` and can be embedded directly in
//! `repr(C)` structs like [`super::RSIndexResult`].

use ffi::RLookupKey;
use thin_vec::ThinVec;
use value::{RsValue, SharedRsValue};

/// A single metric: a borrowed key and an owned, reference-counted value.
#[repr(C)]
#[derive(Clone)]
pub struct MetricEntry<'a> {
    /// Borrowed reference to the lookup key that identifies this metric.
    /// `None` when the metric has no associated key.
    key: Option<&'a RLookupKey>,

    /// Owned, reference-counted value.  Cloning a [`MetricEntry`]
    /// increments the refcount; dropping it decrements.
    value: SharedRsValue,
}

impl<'a> MetricEntry<'a> {
    /// Creates a new metric entry.
    pub const fn new(key: Option<&'a RLookupKey>, value: SharedRsValue) -> Self {
        Self { key, value }
    }

    /// Returns the key reference, or `None` if the metric has no key.
    pub const fn key(&self) -> Option<&'a RLookupKey> {
        self.key
    }

    /// Returns a reference to the owned value.
    pub const fn value(&self) -> &SharedRsValue {
        &self.value
    }

    /// Returns the raw value pointer (for passing to C).
    pub const fn value_ptr(&self) -> *const RsValue {
        self.value.as_ptr()
    }

    /// Replaces the value, dropping (and decrementing the refcount of) the
    /// previous one.
    pub fn set_value(&mut self, new_value: SharedRsValue) {
        self.value = new_value;
    }
}

impl PartialEq for MetricEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(
            self.key.map_or(std::ptr::null(), |k| k as *const _),
            other.key.map_or(std::ptr::null(), |k| k as *const _),
        ) && SharedRsValue::ptr_eq(&self.value, &other.value)
    }
}

/// A dynamically-sized collection of [`MetricEntry`] values.
///
/// Backed by a [`ThinVec`] — a single-pointer vec that stores len/cap in
/// the allocation header. `MetricsVec::new()` does not allocate.
///
/// `repr(transparent)` over `ThinVec` means this type is pointer-sized
/// and can be embedded directly in `repr(C)` structs.
#[repr(transparent)]
#[derive(Clone, PartialEq)]
pub struct MetricsVec<'a> {
    inner: ThinVec<MetricEntry<'a>>,
}

/// A read-only, C-visible slice view over the entries of a [`MetricsVec`].
///
/// Returned by [`MetricsVec::as_metrics_slice`] for zero-copy iteration
/// from C. The pointed-to data is valid as long as the originating
/// [`MetricsVec`] is not mutated or dropped.
#[repr(C)]
pub struct MetricsSlice<'a> {
    /// Pointer to the first [`MetricEntry`].  May be dangling (but not null)
    /// when `len == 0`.
    pub data: *const MetricEntry<'a>,

    /// Number of entries.
    pub len: usize,
}

impl<'a> MetricsVec<'a> {
    /// Creates an empty metrics collection. Does not allocate.
    pub const fn new() -> Self {
        Self {
            inner: ThinVec::new(),
        }
    }

    /// Appends a metric entry.
    pub fn push(&mut self, key: Option<&'a RLookupKey>, value: SharedRsValue) {
        self.inner.push(MetricEntry::new(key, value));
    }

    /// Moves all entries from `other` into `self`, leaving `other` empty.
    ///
    /// Ownership of each [`SharedRsValue`] transfers to `self`; no
    /// reference counts are changed.
    pub fn concat(&mut self, other: &mut Self) {
        self.inner.append(&mut other.inner);
    }

    /// Drops all entries, decrementing the refcount of each value.
    pub fn reset(&mut self) {
        self.inner.clear();
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns a C-compatible slice view for zero-copy iteration.
    pub fn as_metrics_slice(&self) -> MetricsSlice<'a> {
        let slice = self.inner.as_slice();
        MetricsSlice {
            data: slice.as_ptr(),
            len: slice.len(),
        }
    }

    /// Returns a reference to the entry at `index`, or `None` if out of
    /// bounds.
    pub fn get(&self, index: usize) -> Option<&MetricEntry<'a>> {
        self.inner.as_slice().get(index)
    }

    /// Returns a mutable reference to the entry at `index`, or `None` if
    /// out of bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut MetricEntry<'a>> {
        self.inner.as_mut_slice().get_mut(index)
    }

    /// Returns an iterator over the entries.
    pub fn iter(&self) -> impl Iterator<Item = &MetricEntry<'a>> {
        self.inner.as_slice().iter()
    }

    /// Finds the first entry whose key matches `key` (pointer equality)
    /// and returns a mutable reference to it.
    pub fn find_by_key_mut(&mut self, key: &RLookupKey) -> Option<&mut MetricEntry<'a>> {
        self.inner
            .as_mut_slice()
            .iter_mut()
            .find(|e| e.key.is_some_and(|k| std::ptr::eq(k, key)))
    }
}

impl Default for MetricsVec<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetricsVec<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsVec")
            .field("len", &self.inner.len())
            .finish()
    }
}
