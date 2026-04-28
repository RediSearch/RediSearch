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
//! pairing a borrowed [`RLookupKey`] with a numeric value.
//!
//! Backed by [`ThinVec`] (pointer-sized, no-alloc on construction),
//! `MetricsVec` is `repr(transparent)` and can be embedded directly in
//! `repr(C)` structs like [`super::RSIndexResult`].
use ffi::RLookupKey;
use thin_vec::ThinVec;

/// A single metric: a borrowed key and a numeric value.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MetricEntry<'a> {
    /// Borrowed reference to the lookup key that identifies this metric.
    /// `None` when the metric has no associated key.
    key: Option<&'a RLookupKey>,

    /// The metric value (e.g. vector distance, score).
    value: f64,
}

impl<'a> MetricEntry<'a> {
    /// Creates a metric entry with an associated key.
    pub const fn with_key(key: &'a RLookupKey, value: f64) -> Self {
        Self {
            key: Some(key),
            value,
        }
    }

    /// Creates a metric entry without an associated key.
    pub const fn without_key(value: f64) -> Self {
        Self { key: None, value }
    }

    /// Returns the key reference, or `None` if the metric has no key.
    pub const fn key(&self) -> Option<&'a RLookupKey> {
        self.key
    }

    /// Returns the metric value.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Replaces the value.
    pub const fn set_value(&mut self, new_value: f64) {
        self.value = new_value;
    }
}

impl PartialEq for MetricEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(
            self.key.map_or(std::ptr::null(), |k| k as *const _),
            other.key.map_or(std::ptr::null(), |k| k as *const _),
        ) && self.value == other.value
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
#[derive(Clone, PartialEq, Debug)]
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

    /// Appends a metric entry with an associated key.
    pub fn push_with_key(&mut self, key: &'a RLookupKey, value: f64) {
        self.inner.push(MetricEntry::with_key(key, value));
    }

    /// Appends a metric entry without an associated key.
    pub fn push_without_key(&mut self, value: f64) {
        self.inner.push(MetricEntry::without_key(value));
    }

    /// Moves all entries from `other` into `self`, leaving `other` empty.
    pub fn concat(&mut self, other: &mut Self) {
        self.inner.append(&mut other.inner);
    }

    /// Drops all entries.
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
