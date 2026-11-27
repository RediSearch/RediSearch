/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Values that iterators yield alongside a hit, for the query pipeline to load.
//!
//! An iterator can attach a [`YieldedValue`] to the result it produces: a
//! [`YieldedData`] (a vector distance, a score, a loaded field value) paired with
//! the [`RLookupKey`] the pipeline writes it under. Aggregate iterators forward
//! their children's values, and the result processor that loads them writes each
//! one into the row through its key. The planner asks for a key per value up
//! front, see `rlookup::YieldRequest`.
//!
//! A result carries its values as a [`YieldedValues`].
//!
//! Backed by [`ThinVec`] (pointer-sized, no-alloc on construction),
//! `YieldedValues` is `repr(transparent)` and can be embedded directly in
//! `repr(C)` structs like [`super::RSIndexResult`].
use ffi::RLookupKey;
use std::{ptr, ptr::NonNull};
use thin_vec::ThinVec;

/// How a [`SharedString`] shares, releases and reads the value it holds,
/// supplied by the code that builds one.
///
/// This crate cannot do it itself: depending on the value library would put
/// its C references into every Rust test binary that holds a result, including
/// those that do not link the C library.
#[cheadergen::config(opaque)]
pub struct SharedStringVTable {
    /// Take one more reference to `value`.
    pub retain: unsafe fn(value: NonNull<ffi::RSValue>),
    /// Release one reference to `value`.
    pub release: unsafe fn(value: NonNull<ffi::RSValue>),
    /// The bytes of the string `value`, valid while a reference to it is held.
    pub bytes: unsafe fn(value: NonNull<ffi::RSValue>) -> NonNull<[u8]>,
}

/// A string `RSValue` held by reference count, which C reads as
/// [`value`](Self::as_ptr).
///
/// # Invariants
///
/// `value` is a string value, this handle owns one reference to it, and
/// `vtable` holds the operations of the library that built it.
#[repr(C)]
pub struct SharedString {
    value: NonNull<ffi::RSValue>,
    vtable: &'static SharedStringVTable,
}

impl SharedString {
    /// Take over one reference to the string `value`, shared through `vtable`.
    ///
    /// # Safety
    ///
    /// 1. `value` is a string value whose reference the caller hands over.
    /// 2. `vtable`'s operations are sound for `value`, from any thread.
    pub const unsafe fn from_raw(
        value: NonNull<ffi::RSValue>,
        vtable: &'static SharedStringVTable,
    ) -> Self {
        Self { value, vtable }
    }

    /// The value, borrowed for as long as `self` lives.
    pub const fn as_ptr(&self) -> NonNull<ffi::RSValue> {
        self.value
    }

    /// The operations `self` was built with.
    pub const fn vtable(&self) -> &'static SharedStringVTable {
        self.vtable
    }

    /// The string's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: by the type invariant `vtable` reads this value.
        let bytes = unsafe { (self.vtable.bytes)(self.value) };
        // SAFETY: the bytes stay valid while a reference is held, and `self`
        // holds one for as long as the borrow lasts.
        unsafe { bytes.as_ref() }
    }
}

impl Clone for SharedString {
    fn clone(&self) -> Self {
        // SAFETY: by the type invariant `vtable` shares this value.
        unsafe { (self.vtable.retain)(self.value) };
        // SAFETY: the reference just taken is handed over, with the same
        // operations as `self`'s.
        unsafe { Self::from_raw(self.value, self.vtable) }
    }
}

impl Drop for SharedString {
    fn drop(&mut self) {
        // SAFETY: by the type invariant this handle owns one reference, which
        // `vtable` releases.
        unsafe { (self.vtable.release)(self.value) };
    }
}

impl std::fmt::Debug for SharedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedString")
            .field(&String::from_utf8_lossy(self.as_bytes()))
            .finish()
    }
}

// SAFETY: `from_raw` requires the operations to be sound from any thread, and
// the value is only read through them.
unsafe impl Send for SharedString {}
// SAFETY: as above.
unsafe impl Sync for SharedString {}

/// The data of a [`YieldedValue`].
#[derive(Debug, Clone)]
#[repr(C, u8)]
#[cheadergen::config(export, prefix_with_name)]
pub enum YieldedData {
    /// A number: a vector distance, a score, or a value a NUMERIC clause
    /// loaded.
    Number(f64),
    /// The tag a TAG clause loaded, as a string `RSValue`.
    ///
    /// Shared rather than copied: the iterator reading one tag value builds it
    /// once, and each hit carries a clone, an atomic reference-count increment
    /// rather than a copy of the string.
    String(SharedString),
}

impl YieldedData {
    /// The number, or `None` for a string.
    pub const fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(number) => Some(*number),
            Self::String(_) => None,
        }
    }
}

impl From<f64> for YieldedData {
    fn from(number: f64) -> Self {
        Self::Number(number)
    }
}

// Strings compare by content: equal values read by different iterators are
// different allocations.
impl PartialEq for YieldedData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Number(lhs), Self::Number(rhs)) => lhs == rhs,
            (Self::String(lhs), Self::String(rhs)) => lhs.as_bytes() == rhs.as_bytes(),
            _ => false,
        }
    }
}

/// A single yielded value: a borrowed key and its [`YieldedData`].
#[derive(Debug, Clone)]
#[repr(C)]
#[cheadergen::config(export)]
pub struct YieldedValue<'query> {
    /// Borrowed reference to the lookup key the value is written under, or
    /// `None` when the value has no associated key.
    ///
    /// The key always lives under the `'query` lifetime: it belongs to the
    /// query pipeline (the `RLookupKey`), not to the inverted index, so it is
    /// never weakened by the `Active`/`Suspended` ref-mode transitions —
    /// unlike the index-backed pointers of a result, it stays a genuine
    /// `&'query` borrow across the whole suspend/resume cycle. That is why it
    /// is modelled as a plain reference rather than a ref-mode-parametrised
    /// `SharedPtr`.
    ///
    /// Stored as `Option<&'query RLookupKey>` so "no key" is `None`; the
    /// `NonNull` niche of `&T` keeps the C ABI as a nullable `RLookupKey *`.
    pub key: Option<&'query RLookupKey>,

    /// The value (e.g. vector distance, score, loaded tag).
    pub value: YieldedData,
}

impl<'a> YieldedValue<'a> {
    /// Creates a yielded value with an associated key.
    pub fn with_key(key: &'a RLookupKey, value: impl Into<YieldedData>) -> Self {
        Self {
            key: Some(key),
            value: value.into(),
        }
    }

    /// Creates a yielded value without an associated key.
    pub fn without_key(value: impl Into<YieldedData>) -> Self {
        Self {
            key: None,
            value: value.into(),
        }
    }

    /// Returns the key reference, or `None` if the value has no key.
    pub const fn key(&self) -> Option<&'a RLookupKey> {
        self.key
    }
}

impl YieldedValue<'_> {
    /// Returns the value.
    pub const fn value(&self) -> &YieldedData {
        &self.value
    }

    /// Replaces the value.
    pub fn set_value(&mut self, new_value: impl Into<YieldedData>) {
        self.value = new_value.into();
    }
}

// Manual `PartialEq` (rather than derived) because `RLookupKey` is an opaque
// FFI type without a `PartialEq` impl, and we want identity-based key
// comparison anyway: two yielded values refer to "the same key" when their
// pointers match, regardless of the key's content. Restricted to the
// `Active` instantiation since [`Suspended`] pointers may be stale.
impl<'a> PartialEq for YieldedValue<'a> {
    fn eq(&self, other: &Self) -> bool {
        let Self { key, value } = self;
        let Self {
            key: o_key,
            value: o_value,
        } = other;
        let lhs = key.map_or(ptr::null(), |k| k as *const RLookupKey);
        let rhs = o_key.map_or(ptr::null(), |k| k as *const RLookupKey);
        ptr::eq(lhs, rhs) && value == o_value
    }
}

/// A dynamically-sized collection of [`YieldedValue`]s.
///
/// Backed by a [`ThinVec`] — a single-pointer vec that stores len/cap in
/// the allocation header. `YieldedValues::new()` does not allocate.
///
/// `repr(transparent)` over `ThinVec` means this type is pointer-sized
/// and can be embedded directly in `repr(C)` structs.
#[derive(Debug)]
#[repr(transparent)]
#[cheadergen::config(export)]
pub struct YieldedValues<'query> {
    inner: ThinVec<YieldedValue<'query>>,
}

impl Clone for YieldedValues<'_> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        Self {
            inner: inner.clone(),
        }
    }
}

impl<'a> PartialEq for YieldedValues<'a> {
    fn eq(&self, other: &Self) -> bool {
        let Self { inner } = self;
        let Self { inner: o_inner } = other;
        inner.as_slice() == o_inner.as_slice()
    }
}

/// A read-only, C-visible slice view over the entries of a [`YieldedValues`].
///
/// Returned by [`YieldedValues::as_c_slice`] for zero-copy iteration
/// from C. The pointed-to data is valid as long as the originating
/// [`YieldedValues`] is not mutated or dropped.
#[repr(C)]
#[cheadergen::config(export)]
pub struct YieldedValuesSlice<'query> {
    /// Pointer to the first [`YieldedValue`].  May be dangling (but not null)
    /// when `len == 0`.
    pub data: *const YieldedValue<'query>,

    /// Number of entries.
    pub len: usize,
}

impl<'query> YieldedValues<'query> {
    /// Creates an empty collection. Does not allocate.
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
    pub fn as_c_slice(&self) -> YieldedValuesSlice<'query> {
        let slice = self.inner.as_slice();
        YieldedValuesSlice {
            data: slice.as_ptr(),
            len: slice.len(),
        }
    }

    /// Returns a reference to the entry at `index`, or `None` if out of
    /// bounds.
    pub fn get(&self, index: usize) -> Option<&YieldedValue<'query>> {
        self.inner.as_slice().get(index)
    }

    /// Returns a mutable reference to the entry at `index`, or `None` if
    /// out of bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut YieldedValue<'query>> {
        self.inner.as_mut_slice().get_mut(index)
    }

    /// Returns an iterator over the entries.
    pub fn iter(&self) -> impl Iterator<Item = &YieldedValue<'query>> {
        self.inner.as_slice().iter()
    }

    /// Finds the first entry whose key matches `key` (pointer equality)
    /// and returns a mutable reference to it.
    pub fn find_by_key_mut(&mut self, key: &RLookupKey) -> Option<&mut YieldedValue<'query>> {
        let needle = key as *const RLookupKey;
        self.inner
            .as_mut_slice()
            .iter_mut()
            .find(|e| e.key.is_some_and(|k| ptr::eq(k, needle)))
    }
}

impl<'a> YieldedValues<'a> {
    /// Appends a yielded value with an associated key.
    pub fn push_with_key(&mut self, key: &'a RLookupKey, value: impl Into<YieldedData>) {
        self.inner.push(YieldedValue::with_key(key, value));
    }

    /// Appends a yielded value without an associated key.
    pub fn push_without_key(&mut self, value: impl Into<YieldedData>) {
        self.inner.push(YieldedValue::without_key(value));
    }
}

impl Default for YieldedValues<'_> {
    fn default() -> Self {
        Self::new()
    }
}
