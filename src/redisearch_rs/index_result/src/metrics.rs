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
use ref_mode::{Active, Ref, SharedPtr};
use std::ptr;
use thin_vec::ThinVec;

/// A single metric: a borrowed key and a numeric value.
///
/// The `R: Ref` parameter controls how the key reference is stored:
/// in [`Active<'a>`] mode it is a valid `&'a RLookupKey`, in
/// [`ref_mode::Suspended`] mode it is an inert raw pointer.
///
/// `key` is `Option<SharedPtr<R, RLookupKey>>` so "no key" is encoded as
/// `None`. `SharedPtr` wraps `NonNull` so the niche optimization keeps the C
/// ABI as a nullable `RLookupKey *`.
#[derive(Debug)]
#[repr(C)]
pub struct RawMetricEntry<R: Ref> {
    /// Borrowed reference to the lookup key that identifies this metric,
    /// or `None` when the metric has no associated key.
    pub key: Option<SharedPtr<R, RLookupKey>>,

    /// The metric value (e.g. vector distance, score).
    pub value: f64,
}

/// The [`Active`] instantiation of [`RawMetricEntry`].
#[cheadergen::config(export)]
pub type MetricEntry<'a> = RawMetricEntry<Active<'a>>;

impl<R: Ref> Copy for RawMetricEntry<R> {}

impl<R: Ref> Clone for RawMetricEntry<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a> MetricEntry<'a> {
    /// Creates a metric entry with an associated key.
    pub const fn with_key(key: &'a RLookupKey, value: f64) -> Self {
        Self {
            key: Some(SharedPtr::from_ref(key)),
            value,
        }
    }

    /// Creates a metric entry without an associated key.
    pub const fn without_key(value: f64) -> Self {
        Self { key: None, value }
    }

    /// Returns the key reference, or `None` if the metric has no key.
    pub const fn key(&self) -> Option<&'a RLookupKey> {
        match self.key {
            Some(k) => Some(k.get()),
            None => None,
        }
    }
}

impl<R: Ref> RawMetricEntry<R> {
    /// Returns the metric value.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Replaces the value.
    pub const fn set_value(&mut self, new_value: f64) {
        self.value = new_value;
    }
}

// Manual `PartialEq` (rather than derived) because `RLookupKey` is an opaque
// FFI type without a `PartialEq` impl, and we want identity-based key
// comparison anyway: two metric entries refer to "the same key" when their
// pointers match, regardless of the key's content. Restricted to the
// `Active` instantiation since [`Suspended`] pointers may be stale.
impl<'a> PartialEq for MetricEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        let Self { key, value } = self;
        let Self {
            key: o_key,
            value: o_value,
        } = other;
        let lhs = key.map_or(ptr::null(), |p| p.as_raw());
        let rhs = o_key.map_or(ptr::null(), |p| p.as_raw());
        ptr::eq(lhs, rhs) && value == o_value
    }
}

/// A dynamically-sized collection of [`MetricEntry`] values.
///
/// Backed by a [`ThinVec`] — a single-pointer vec that stores len/cap in
/// the allocation header. `MetricsVec::new()` does not allocate.
///
/// `repr(transparent)` over `ThinVec` means this type is pointer-sized
/// and can be embedded directly in `repr(C)` structs.
#[derive(Debug)]
#[repr(transparent)]
pub struct RawMetricsVec<R: Ref> {
    inner: ThinVec<RawMetricEntry<R>>,
}

/// The [`Active`] instantiation of [`RawMetricsVec`].
pub type MetricsVec<'a> = RawMetricsVec<Active<'a>>;

impl<R: Ref> Clone for RawMetricsVec<R> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        Self {
            inner: inner.clone(),
        }
    }
}

impl<'a> PartialEq for MetricsVec<'a> {
    fn eq(&self, other: &Self) -> bool {
        let Self { inner } = self;
        let Self { inner: o_inner } = other;
        inner.as_slice() == o_inner.as_slice()
    }
}

/// A read-only, C-visible slice view over the entries of a [`MetricsVec`].
///
/// Returned by [`MetricsVec::as_metrics_slice`] for zero-copy iteration
/// from C. The pointed-to data is valid as long as the originating
/// [`MetricsVec`] is not mutated or dropped.
#[repr(C)]
pub struct RawMetricsSlice<R: Ref> {
    /// Pointer to the first [`MetricEntry`].  May be dangling (but not null)
    /// when `len == 0`.
    pub data: *const RawMetricEntry<R>,

    /// Number of entries.
    pub len: usize,
}

/// The [`Active`] instantiation of [`RawMetricsSlice`].
pub type MetricsSlice<'a> = RawMetricsSlice<Active<'a>>;

impl<R: Ref> RawMetricsVec<R> {
    /// Creates an empty metrics collection. Does not allocate.
    pub const fn new() -> Self {
        Self {
            inner: ThinVec::new(),
        }
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
    pub fn as_metrics_slice(&self) -> RawMetricsSlice<R> {
        let slice = self.inner.as_slice();
        RawMetricsSlice {
            data: slice.as_ptr(),
            len: slice.len(),
        }
    }

    /// Returns a reference to the entry at `index`, or `None` if out of
    /// bounds.
    pub fn get(&self, index: usize) -> Option<&RawMetricEntry<R>> {
        self.inner.as_slice().get(index)
    }

    /// Returns a mutable reference to the entry at `index`, or `None` if
    /// out of bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut RawMetricEntry<R>> {
        self.inner.as_mut_slice().get_mut(index)
    }

    /// Returns an iterator over the entries.
    pub fn iter(&self) -> impl Iterator<Item = &RawMetricEntry<R>> {
        self.inner.as_slice().iter()
    }

    /// Finds the first entry whose key matches `key` (pointer equality)
    /// and returns a mutable reference to it.
    pub fn find_by_key_mut(&mut self, key: &RLookupKey) -> Option<&mut RawMetricEntry<R>> {
        let needle = key as *const RLookupKey;
        self.inner
            .as_mut_slice()
            .iter_mut()
            .find(|e| e.key.is_some_and(|p| ptr::eq(p.as_raw(), needle)))
    }
}

impl<'a> MetricsVec<'a> {
    /// Appends a metric entry with an associated key.
    pub fn push_with_key(&mut self, key: &'a RLookupKey, value: f64) {
        self.inner.push(MetricEntry::with_key(key, value));
    }

    /// Appends a metric entry without an associated key.
    pub fn push_without_key(&mut self, value: f64) {
        self.inner.push(MetricEntry::without_key(value));
    }
}

impl<R: Ref> Default for RawMetricsVec<R> {
    fn default() -> Self {
        Self::new()
    }
}
