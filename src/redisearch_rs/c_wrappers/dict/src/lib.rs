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

/// Bidirectional conversion between a Rust type and the raw `*mut c_void`
/// pointer stored in a dict entry.
///
/// The lifetime `'a` ties the output of [`from_dict_ptr`](Self::from_dict_ptr)
/// to the lifetime of the underlying dict data, enabling borrowed types like
/// [`HiddenStringRef<'a>`] to be returned directly from
/// [`DictEntry::key`] and [`DictEntry::val`].
///
/// The same type is used for both reading (via [`Dict::iter`]) and writing
/// (via [`Dict::insert`]).
///
/// # Safety
///
/// Implementors must ensure that the `*mut c_void` passed to `from_dict_ptr`
/// genuinely points to a value of the type that `Self` represents. Whether
/// `into_dict_ptr` is safe to call depends on the [`ffi::dictType`] the dict
/// was created with — the dictType's callbacks determine how the pointer is
/// used after insertion.
pub unsafe trait DictValue<'a>: Sized + Copy + 'a {
    /// Convert a raw dict entry pointer to `Self`.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a valid value of the type that `Self` represents,
    /// and must remain valid for at least `'a`.
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self;

    /// Convert `self` into a raw dict entry pointer.
    fn into_dict_ptr(self) -> *mut c_void;
}

// SAFETY: pointer casts are always layout-compatible; the caller is responsible
// for ensuring the pointer actually addresses a T.
unsafe impl<'a, T: 'a> DictValue<'a> for *mut T {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        ptr.cast()
    }
    fn into_dict_ptr(self) -> *mut c_void {
        self.cast()
    }
}

// SAFETY: same as *mut T; const-ness is a Rust concept, not a C one.
unsafe impl<'a, T: 'a> DictValue<'a> for *const T {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        ptr.cast()
    }
    fn into_dict_ptr(self) -> *mut c_void {
        self.cast_mut().cast()
    }
}

// SAFETY: The dict stores *mut HiddenString pointers as *mut c_void.
// Casting back and wrapping in HiddenStringRef is sound when the caller
// guarantees the pointer is a valid, non-null HiddenString that lives for 'a.
unsafe impl<'a> DictValue<'a> for HiddenStringRef<'a> {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        // SAFETY: caller guarantees ptr is a valid *mut HiddenString for 'a.
        unsafe { HiddenStringRef::from_raw(ptr.cast::<ffi::HiddenString>()) }
    }
    fn into_dict_ptr(self) -> *mut c_void {
        self.as_ptr().cast()
    }
}

// SAFETY: null pointer → None (absence of a value); non-null *mut c_void →
// Some(&T). The caller must ensure non-null pointers address a valid T for 'a.
unsafe impl<'a, T: 'a> DictValue<'a> for Option<&'a T> {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        // SAFETY: caller guarantees non-null ptr is a valid *const T for 'a.
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr.cast::<T>() })
        }
    }
    fn into_dict_ptr(self) -> *mut c_void {
        match self {
            None => std::ptr::null_mut(),
            Some(r) => (r as *const T).cast_mut().cast(),
        }
    }
}

/// Describes the key type, value type, and underlying C [`ffi::dictType`] for
/// a particular dict configuration.
///
/// Implemented by marker types (e.g. `MissingFieldDictType`) that bind a
/// specific `ffi::dictType` to typed `K` and `V` parameters. This allows
/// [`OwnedDict<DT>`] to call [`create`](OwnedDict::create) without a caller-
/// supplied `dictType` pointer, and ensures that the dict's hash/compare/dup/
/// destructor callbacks are always consistent with the key and value types.
///
/// # Safety
///
/// Implementors must ensure that [`as_ptr`](Self::as_ptr) returns a pointer to
/// a valid, live `ffi::dictType` whose callbacks are consistent with `K` and
/// `V`: e.g. if `K::into_dict_ptr` returns a pointer to a temporary, the
/// dictType's `keyDup` must copy it before the caller is allowed to free the
/// original.
pub unsafe trait DictType {
    type K<'a>: DictValue<'a>;
    type V<'a>: DictValue<'a>;

    /// Return a pointer to the static C `dictType` for this configuration.
    fn as_ptr() -> *mut ffi::dictType;
}

/// [`DictType`] marker for the spec's `missingFieldDict`.
///
/// Keys are [`HiddenStringRef`] (field names); values are
/// `Option<&InvertedIndex>` (the per-field missing-doc inverted index, or
/// `None` when the field has no such index). The underlying C dictType is
/// `dictTypeHeapHiddenStrings`, which copies keys on insertion via `keyDup`
/// and does not free values on removal.
pub struct MissingFieldDictType;

// SAFETY: dictTypeHeapHiddenStrings is a valid static ffi::dictType whose
// callbacks are compatible with HiddenStringRef keys and Option<&InvertedIndex>
// values: keyDup copies the HiddenString so callers may free the original after
// insert; valDestructor is null so callers retain ownership of InvertedIndex.
unsafe impl DictType for MissingFieldDictType {
    type K<'a> = HiddenStringRef<'a>;
    type V<'a> = Option<&'a InvertedIndex>;

    fn as_ptr() -> *mut ffi::dictType {
        unsafe { std::ptr::addr_of_mut!(ffi::dictTypeHeapHiddenStrings) }
    }
}

/// A safe wrapper around a C [`ffi::dict`].
///
/// `DT` is a [`DictType`] marker that fixes the key type ([`DictType::K`]),
/// value type ([`DictType::V`]), and the underlying C `dictType` callbacks.
///
/// Obtained from a raw `*const`/`*mut ffi::dict` via [`Dict::from_raw`] /
/// [`Dict::from_raw_mut`]. For an owned dict that is released on drop, use
/// [`OwnedDict`].
#[repr(transparent)]
pub struct Dict<DT: DictType> {
    inner: ffi::dict,
    _phantom: PhantomData<DT>,
}

impl<DT: DictType> Dict<DT> {
    /// Borrow a [`Dict`] from a raw const pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null pointer to an `ffi::dict` created with
    /// the `ffi::dictType` described by `DT`, and must remain live for `'a`.
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
    /// the `ffi::dictType` described by `DT`, must remain live for `'a`, and
    /// must have no other aliasing references for the duration.
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
    pub fn insert<'b>(&mut self, key: DT::K<'b>, val: DT::V<'b>) -> bool {
        // SAFETY: self points to a valid dict; key and val lifetimes are managed
        // by the dictType's dup/destructor callbacks.
        unsafe { ffi::RS_dictAdd(self.as_mut_ptr(), key.into_dict_ptr(), val.into_dict_ptr()) == 0 }
    }

    /// Iterate over all entries in this dict.
    ///
    /// Each entry is yielded as a [`DictEntry`] whose [`DictEntry::key`] and
    /// [`DictEntry::val`] return the typed key and value directly — no manual
    /// casts or wrapper constructions required.
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
/// Obtained via [`OwnedDict::create`]. Derefs to [`Dict<DT>`] so all
/// [`Dict`] methods — including [`iter`](Dict::iter) and
/// [`insert`](Dict::insert) — are available on `&OwnedDict` and
/// `&mut OwnedDict` respectively.
pub struct OwnedDict<DT: DictType> {
    ptr: NonNull<ffi::dict>,
    _phantom: PhantomData<DT>,
}

impl<DT: DictType> OwnedDict<DT> {
    /// Allocate a new C dict for the configuration described by `DT`.
    ///
    /// The dictType pointer and the guarantee that its callbacks are compatible
    /// with `DT::K` and `DT::V` are encoded in the `unsafe impl DictType`.
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
    pub fn as_ptr(&self) -> *mut ffi::dict {
        self.ptr.as_ptr()
    }
}

impl<DT: DictType> Deref for OwnedDict<DT> {
    type Target = Dict<DT>;

    fn deref(&self) -> &Dict<DT> {
        // SAFETY: self.ptr is a valid, non-null dict alive for the lifetime of
        // this OwnedDict.
        unsafe { Dict::from_raw(self.ptr.as_ptr()) }
    }
}

impl<DT: DictType> DerefMut for OwnedDict<DT> {
    fn deref_mut(&mut self) -> &mut Dict<DT> {
        // SAFETY: self.ptr is a valid, non-null dict alive for the lifetime of
        // this OwnedDict, and we hold exclusive access via &mut self.
        unsafe { Dict::from_raw_mut(self.ptr.as_ptr()) }
    }
}

impl<DT: DictType> Drop for OwnedDict<DT> {
    fn drop(&mut self) {
        // SAFETY: self.ptr was obtained from RS_dictCreate and has not been
        // released yet.
        unsafe { ffi::RS_dictRelease(self.ptr.as_ptr()) };
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
    const fn new(entry: *mut ffi::dictEntry) -> Self {
        Self {
            entry,
            _phantom: PhantomData,
        }
    }

    /// Return the key of this entry.
    pub fn key(&self) -> DT::K<'a> {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let ptr = unsafe { (*self.entry).key };
        unsafe { <DT::K<'a> as DictValue<'a>>::from_dict_ptr(ptr) }
    }

    /// Return the value of this entry.
    pub fn val(&self) -> DT::V<'a> {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let v = unsafe { &(*self.entry).v };
        let ptr = unsafe { v.val };
        unsafe { <DT::V<'a> as DictValue<'a>>::from_dict_ptr(ptr) }
    }
}

/// An iterator over the entries of a C [`ffi::dict`].
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
    /// Prefer [`Dict::iter`] when a [`Dict`] reference is available.
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
            Some(DictEntry::new(entry))
        }
    }
}

impl<DT: DictType> Drop for DictIterator<'_, DT> {
    fn drop(&mut self) {
        // SAFETY: self.iter is a valid iterator obtained during construction.
        unsafe { ffi::RS_dictReleaseIterator(self.iter) };
    }
}
