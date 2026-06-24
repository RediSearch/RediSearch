/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrappers around the C [`ffi::dict`] type.

use std::ffi::c_void;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use hidden_string::HiddenStringRef;
use inverted_index::opaque::InvertedIndex;

/// Describes a `Dict` type defined by a `ffi::dictType` with conversion
/// functions to and from keys and values.
///
/// # Safety
///
/// Implementers must ensure that:
/// - [`as_ptr`](Self::as_ptr) returns a pointer to a valid, live `ffi::dictType`
///   whose callbacks are consistent with `K` and `V`.
/// - [`key_from_ptr`](Self::key_from_ptr) and [`val_from_ptr`](Self::val_from_ptr)
///   correctly reconstruct the key/value from the raw pointer stored by the dict.
/// - [`key_into_ptr`](Self::key_into_ptr) and [`val_into_ptr`](Self::val_into_ptr)
///   produce pointers that are handled correctly by the `dictType`.
pub unsafe trait DictType {
    type K<'a>: Sized + Copy + 'a;
    type V<'a>: Sized + Copy + 'a;

    /// Return a pointer to the static C `dictType` for this configuration.
    fn as_ptr() -> *mut ffi::dictType;

    /// Reconstruct a key from the raw pointer stored in a dict entry.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer to a key consistent with `K`, valid for `'a`.
    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a>;

    /// Convert a key to the raw pointer to be passed to the dict.
    fn key_into_ptr<'a>(key: Self::K<'a>) -> *mut c_void;

    /// Reconstruct a value from the raw pointer stored in a dict entry.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer consistent with `V`, valid for `'a`.
    unsafe fn val_from_ptr<'a>(ptr: *mut c_void) -> Self::V<'a>;

    /// Convert a value to the raw pointer to be passed to the dict.
    fn val_into_ptr<'a>(val: Self::V<'a>) -> *mut c_void;
}

/// [`DictType`] marker for the spec's `missingFieldDict`.
pub struct MissingFieldDictType;

// SAFETY:
// - dictTypeHeapHiddenStrings is a valid static ffi::dictType whose callbacks
//   are compatible with HiddenStringRef keys and Option<&InvertedIndex> values.
// - keyDup copies the HiddenString so callers may free the original after insert.
// - valDestructor is null so callers retain ownership of InvertedIndex.
// - The key/value conversions round-trip through *mut HiddenString and
//   *mut InvertedIndex respectively, consistent with how the C dict stores them.
unsafe impl DictType for MissingFieldDictType {
    type K<'a> = HiddenStringRef<'a>;
    type V<'a> = Option<&'a InvertedIndex>;

    fn as_ptr() -> *mut ffi::dictType {
        std::ptr::addr_of_mut!(ffi::dictTypeHeapHiddenStrings)
    }

    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a> {
        // SAFETY: caller guarantees ptr is a valid *mut HiddenString for 'a.
        unsafe { HiddenStringRef::from_raw(ptr.cast::<ffi::HiddenString>()) }
    }

    fn key_into_ptr<'a>(key: Self::K<'a>) -> *mut c_void {
        key.as_ptr().cast()
    }

    unsafe fn val_from_ptr<'a>(ptr: *mut c_void) -> Self::V<'a> {
        // SAFETY: caller guarantees non-null ptr is a valid *mut InvertedIndex
        // for 'a; null represents None (no missing-doc index for this field).
        NonNull::new(ptr.cast::<InvertedIndex>()).map(|p| unsafe { p.as_ref() })
    }

    fn val_into_ptr<'a>(val: Self::V<'a>) -> *mut c_void {
        match val {
            None => std::ptr::null_mut(),
            Some(r) => std::ptr::from_ref(r).cast_mut().cast(),
        }
    }
}

/// A safe wrapper around a C [`ffi::dict`].
///
/// `DT` is a [`DictType`] marker that fixes the key type ([`DictType::K`]),
/// value type ([`DictType::V`]), pointer conversions, and the underlying C
/// `dictType` callbacks.
///
/// `'val` is the minimum lifetime of values stored in this dict. All values
/// passed to [`Dict::insert`] must be valid for at least `'val`, which the
/// compiler enforces. This prevents dangling references when values are read
/// back via [`Dict::iter`].
///
/// Obtained from a raw `*const`/`*mut ffi::dict` via [`Dict::from_raw`] /
/// [`Dict::from_raw_mut`]. For an owned dict that is released on drop, use
/// [`OwnedDict`].
#[repr(transparent)]
pub struct Dict<'val, DT: DictType> {
    inner: ffi::dict,
    _phantom: PhantomData<(DT, DT::V<'val>)>,
}

impl<'val, DT: DictType> Dict<'val, DT> {
    /// Borrow a [`Dict`] from a raw const pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null pointer to an `ffi::dict` created with
    /// the `ffi::dictType` described by `DT`, and must remain live for `'a`.
    /// `'val` must not outlive the lifetimes of any values stored in the dict.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::dict) -> &'a Self {
        // SAFETY: #[repr(transparent)] guarantees identical layout.
        // Validity and liveness are the caller's responsibility.
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Borrow a [`Dict`] mutably from a raw pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null pointer to an `ffi::dict` created with
    /// the `ffi::dictType` described by `DT`, must remain live for `'a`, must
    /// have no other aliasing references for the duration, and `'val` must not
    /// outlive the lifetimes of any values stored in the dict.
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::dict) -> &'a mut Self {
        // SAFETY: #[repr(transparent)] guarantees identical layout.
        // Validity, liveness, and exclusivity are the caller's responsibility.
        unsafe { ptr.cast::<Self>().as_mut().unwrap() }
    }

    /// Return a raw const pointer to the underlying [`ffi::dict`].
    pub const fn as_ptr(&self) -> *const ffi::dict {
        &raw const self.inner
    }

    /// Return a raw mutable pointer to the underlying [`ffi::dict`].
    pub const fn as_mut_ptr(&mut self) -> *mut ffi::dict {
        &raw mut self.inner
    }

    /// Insert a typed key–value pair into this dict.
    ///
    /// Returns `true` when the entry was added, `false` when the key already
    /// existed (the existing entry is not updated).
    ///
    /// The key lifetime is unconstrained because the underlying `dictType` is
    /// required (by the [`DictType`] safety contract) to copy keys on insertion.
    /// The value must be valid for `'val` — the dict's value lifetime — so that
    /// values read back via [`Dict::iter`] are never dangling.
    pub fn insert(&mut self, key: DT::K<'_>, val: DT::V<'val>) -> bool {
        // SAFETY: self points to a valid dict; key is copied by the dictType's
        // keyDup; val lifetime is enforced to be 'val by the type signature.
        unsafe {
            ffi::RS_dictAdd(
                self.as_mut_ptr(),
                DT::key_into_ptr(key),
                DT::val_into_ptr(val),
            ) == 0
        }
    }

    /// Iterate over all entries in this dict.
    ///
    /// Each entry is yielded as a [`DictEntry`] whose [`DictEntry::key`] and
    /// [`DictEntry::val`] return the typed key and value directly.
    ///
    /// The iterator holds a C-side iterator object and releases it on drop,
    /// so it is safe to abandon iteration early.
    pub fn iter(&self) -> DictIterator<'_, DT> {
        // SAFETY: `self.inner` is a valid dict (invariant upheld by construction).
        // RS_dictGetIterator does not modify the dict; the cast from *const to
        // *mut is sound because the C function only reads the dict to initialise
        // the iterator's internal fingerprint.
        unsafe { DictIterator::new(self.as_ptr().cast_mut()) }
    }
}

/// An owned [`Dict`] that calls `RS_dictRelease` on drop.
///
/// `'val` is the minimum lifetime of values stored in this dict; see
/// [`Dict`] for details.
///
/// Obtained via [`OwnedDict::create`]. Derefs to [`Dict<'val, DT>`] so all
/// [`Dict`] methods — including [`iter`](Dict::iter) and
/// [`insert`](Dict::insert) — are available on `&OwnedDict` and
/// `&mut OwnedDict` respectively.
pub struct OwnedDict<'val, DT: DictType> {
    ptr: NonNull<ffi::dict>,
    _phantom: PhantomData<(DT, DT::V<'val>)>,
}

impl<'val, DT: DictType> OwnedDict<'val, DT> {
    /// Allocate a new C dict for the configuration described by `DT`.
    pub fn create() -> Self {
        // SAFETY: DT::as_ptr() is guaranteed by the unsafe DictType impl to
        // return a valid, compatible ffi::dictType*.
        let ptr = unsafe { ffi::RS_dictCreate(DT::as_ptr(), std::ptr::null_mut()) };
        Self {
            ptr: NonNull::new(ptr).expect("RS_dictCreate returned null"),
            _phantom: PhantomData,
        }
    }

    /// Return the raw dict pointer for use in FFI calls that take `*mut ffi::dict`.
    pub const fn as_ptr(&self) -> *mut ffi::dict {
        self.ptr.as_ptr()
    }
}

impl<'val, DT: DictType> Deref for OwnedDict<'val, DT> {
    type Target = Dict<'val, DT>;

    fn deref(&self) -> &Dict<'val, DT> {
        // SAFETY: self.ptr is a valid, non-null dict alive for the lifetime of
        // this OwnedDict. 'val matches the OwnedDict's own 'val invariant.
        unsafe { Dict::from_raw(self.ptr.as_ptr()) }
    }
}

impl<'val, DT: DictType> DerefMut for OwnedDict<'val, DT> {
    fn deref_mut(&mut self) -> &mut Dict<'val, DT> {
        // SAFETY: self.ptr is a valid, non-null dict alive for the lifetime of
        // this OwnedDict, and we hold exclusive access via &mut self.
        // 'val matches the OwnedDict's own 'val invariant.
        unsafe { Dict::from_raw_mut(self.ptr.as_ptr()) }
    }
}

impl<'val, DT: DictType> Drop for OwnedDict<'val, DT> {
    fn drop(&mut self) {
        // SAFETY: self.ptr was obtained from RS_dictCreate and has not been
        // released yet.
        unsafe { ffi::RS_dictRelease(self.ptr.as_ptr()) };
    }
}

/// An iterator over the entries of a [`Dict`].
///
/// `'a` is the lifetime of the underlying dict data.  Wraps
/// [`ffi::RS_dictGetIterator`] / [`ffi::RS_dictNext`] /
/// [`ffi::RS_dictReleaseIterator`] and releases the underlying C iterator on
/// drop.
pub struct DictIterator<'a, DT: DictType> {
    iter: *mut ffi::dictIterator,
    _phantom: PhantomData<(&'a (), DT)>,
}

impl<'a, DT: DictType> DictIterator<'a, DT> {
    /// Create an iterator directly from a raw `dict*`.
    ///
    /// # Safety
    ///
    /// `dict` must be a valid, non-null `dict*` consistent with the key and
    /// value types of `DT`, and must remain live for `'a`.
    pub unsafe fn new(dict: *mut ffi::dict) -> Self {
        // SAFETY: caller guarantees dict is valid and non-null.
        let iter = unsafe { ffi::RS_dictGetIterator(dict) };
        Self {
            iter,
            _phantom: PhantomData,
        }
    }
}

impl<'a, DT: DictType> Iterator for DictIterator<'a, DT> {
    type Item = DictEntry<'a, DT>;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: self.iter is a valid iterator obtained during construction.
        let entry = unsafe { ffi::RS_dictNext(self.iter) };
        if entry.is_null() {
            None
        } else {
            // SAFETY: RS_dictNext returns a valid, non-null dictEntry* consistent
            // with DT, and it remains valid for 'a (the dict borrow lifetime).
            Some(unsafe { DictEntry::new(entry) })
        }
    }
}

impl<DT: DictType> Drop for DictIterator<'_, DT> {
    fn drop(&mut self) {
        // SAFETY: self.iter is a valid iterator obtained during construction.
        unsafe { ffi::RS_dictReleaseIterator(self.iter) };
    }
}

/// A single entry in a [`Dict`], yielded by [`DictIterator`].
///
/// `'a` is the lifetime of the underlying dict data.
pub struct DictEntry<'a, DT: DictType> {
    entry: *mut ffi::dictEntry,
    _phantom: PhantomData<(&'a (), DT)>,
}

impl<'a, DT: DictType> DictEntry<'a, DT> {
    /// Wrap a raw `dictEntry*` as a [`DictEntry`].
    ///
    /// # Safety
    ///
    /// `entry` must be a valid, non-null pointer to a `dictEntry` whose key
    /// and value are consistent with `DT`, and must remain valid for `'a`.
    const unsafe fn new(entry: *mut ffi::dictEntry) -> Self {
        Self {
            entry,
            _phantom: PhantomData,
        }
    }

    /// Return the key of this entry.
    pub fn key(&self) -> DT::K<'a> {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let ptr = unsafe { (*self.entry).key };
        // SAFETY: ptr is the key of a valid dictEntry, consistent with DT::K.
        unsafe { DT::key_from_ptr(ptr) }
    }

    /// Return the value of this entry.
    pub fn val(&self) -> DT::V<'a> {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let v = unsafe { &(*self.entry).v };
        // SAFETY: v.val is the value pointer of a valid dictEntry, consistent with DT::V.
        let ptr = unsafe { v.val };
        // SAFETY: ptr is the value of a valid dictEntry, consistent with DT::V.
        unsafe { DT::val_from_ptr(ptr) }
    }
}
