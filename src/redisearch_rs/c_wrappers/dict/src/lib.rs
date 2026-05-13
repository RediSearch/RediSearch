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

use hidden_string::HiddenStringRef;

/// Conversion from the raw `*mut c_void` pointer stored in a dict entry.
///
/// The lifetime `'a` ties the returned value to the lifetime of the dict's
/// underlying data, enabling types like [`HiddenStringRef<'a>`] that borrow
/// from the dict to be returned directly from [`DictEntry::key`] and
/// [`DictEntry::val`].
///
/// Blanket implementations are provided for `*mut T` and `*const T`; both
/// perform a pointer cast and are valid for any lifetime.  [`HiddenStringRef`]
/// is implemented as a zero-cost wrapper construction.
///
/// # Safety
///
/// Implementors must ensure that the `*mut c_void` passed to
/// [`from_dict_ptr`](Self::from_dict_ptr) genuinely points to a value of
/// the type that `Self` represents.
pub unsafe trait FromDictPtr<'a>: Sized + Copy + 'a {
    /// Convert a raw dict entry pointer to `Self`.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a valid value of the type that `Self` represents,
    /// and must remain valid for at least `'a`.
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self;
}

// SAFETY: A pointer cast from *mut c_void to *mut T is always layout-compatible.
// The caller is responsible for ensuring the pointer actually addresses a T.
unsafe impl<'a, T: 'a> FromDictPtr<'a> for *mut T {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        ptr.cast()
    }
}

// SAFETY: A pointer cast from *mut c_void to *const T is always layout-compatible.
// The caller is responsible for ensuring the pointer actually addresses a T.
unsafe impl<'a, T: 'a> FromDictPtr<'a> for *const T {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        ptr.cast()
    }
}

// SAFETY: The dict stores *const HiddenString pointers as *mut c_void.
// Casting back and wrapping in HiddenStringRef is sound when the caller
// guarantees the pointer is a valid, non-null HiddenString that lives for 'a.
unsafe impl<'a> FromDictPtr<'a> for HiddenStringRef<'a> {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        // SAFETY: caller guarantees ptr is a valid *const HiddenString for 'a.
        unsafe { HiddenStringRef::from_raw(ptr.cast::<ffi::HiddenString>()) }
    }
}

// SAFETY: null pointer → None; non-null *mut c_void → Some(&T).
// The caller must ensure non-null pointers address a valid T that lives for 'a.
unsafe impl<'a, T: 'a> FromDictPtr<'a> for Option<&'a T> {
    unsafe fn from_dict_ptr(ptr: *mut c_void) -> Self {
        // SAFETY: caller guarantees non-null ptr is a valid *const T for 'a.
        if ptr.is_null() { None } else { Some(unsafe { &*ptr.cast::<T>() }) }
    }
}


/// A safe wrapper around a C [`ffi::dict`].
///
/// `K` and `V` are the types returned by [`DictEntry::key`] and
/// [`DictEntry::val`]; both default to `*mut c_void` for untyped access.
/// Specifying concrete types eliminates manual casts at call sites and — for
/// wrapper types such as [`HiddenStringRef`] — the manual construction call:
///
/// ```text
/// // typed key, untyped value
/// let d: &Dict<HiddenStringRef<'_>, *mut c_void> = spec.missing_field_dict();
/// for entry in d.iter() {
///     let name: HiddenStringRef<'_> = entry.key(); // no cast, no from_raw
///     let idx:  *mut c_void         = entry.val();
/// }
/// ```
///
/// Obtained from a raw `*const`/`*mut ffi::dict` via [`Dict::from_raw`] or
/// [`Dict::from_raw_mut`].
#[repr(transparent)]
pub struct Dict<K = *mut c_void, V = *mut c_void> {
    inner: ffi::dict,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Dict<K, V> {
    /// Borrow a [`Dict`] from a raw const pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null pointer to an `ffi::dict` that is
    /// properly initialised and remains live for the returned lifetime `'a`.
    /// The caller must also ensure that the dict's keys and values are
    /// actually of types `K` and `V`.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::dict) -> &'a Self {
        // SAFETY: #[repr(transparent)] guarantees identical layout for any K, V.
        // Validity and liveness are the caller's responsibility.
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Borrow a [`Dict`] mutably from a raw pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null pointer to an `ffi::dict` that is
    /// properly initialised and remains live for the returned lifetime `'a`,
    /// with no other aliasing references for the duration.  The caller must
    /// also ensure that the dict's keys and values are actually of types `K`
    /// and `V`.
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::dict) -> &'a mut Self {
        // SAFETY: #[repr(transparent)] guarantees identical layout for any K, V.
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

    /// Iterate over all entries in this dict.
    ///
    /// Each entry is yielded as a [`DictEntry`] whose [`DictEntry::key`] and
    /// [`DictEntry::val`] return the typed key and value directly — no manual
    /// casts or wrapper constructions required.
    ///
    /// The iterator holds a C-side iterator object and releases it on drop,
    /// so it is safe to abandon iteration early.
    pub fn iter<'a>(&'a self) -> DictIterator<'a, K, V>
    where
        K: FromDictPtr<'a>,
        V: FromDictPtr<'a>,
    {
        // SAFETY: `self.inner` is a valid dict (invariant upheld by construction).
        // RS_dictGetIterator does not modify the dict; the cast from *const to
        // *mut is sound because the C function only reads the dict to initialise
        // the iterator's internal fingerprint.
        unsafe { DictIterator::new(self.as_ptr().cast_mut()) }
    }
}

/// A single entry in a [`Dict`], yielded by [`DictIterator`].
///
/// `'a` is the lifetime of the underlying dict data; `K` and `V` are the
/// types returned by [`key`](Self::key) and [`val`](Self::val).
pub struct DictEntry<'a, K = *mut c_void, V = *mut c_void> {
    entry: *mut ffi::dictEntry,
    _phantom: PhantomData<(&'a (), K, V)>,
}

impl<'a, K, V> DictEntry<'a, K, V> {
    const fn new(entry: *mut ffi::dictEntry) -> Self {
        Self { entry, _phantom: PhantomData }
    }
}

impl<'a, K: FromDictPtr<'a>, V: FromDictPtr<'a>> DictEntry<'a, K, V> {
    /// Return the key of this entry.
    ///
    /// Corresponds to the C macro `dictGetKey(entry)` (`entry->key`),
    /// converted to `K` via [`FromDictPtr`].
    pub fn key(&self) -> K {
        // SAFETY: self.entry is a valid non-null dictEntry* (invariant of DictEntry).
        let ptr = unsafe { (*self.entry).key };
        // SAFETY: the caller selected K to match the actual key type stored in this dict.
        unsafe { K::from_dict_ptr(ptr) }
    }

    /// Return the value of this entry.
    ///
    /// Corresponds to the C macro `dictGetVal(entry)` (`entry->v.val`),
    /// converted to `V` via [`FromDictPtr`].
    pub fn val(&self) -> V {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let v = unsafe { &(*self.entry).v };
        // SAFETY: the dict value union variant is a pointer-sized void pointer.
        let ptr = unsafe { v.val };
        // SAFETY: the caller selected V to match the actual value type stored in this dict.
        unsafe { V::from_dict_ptr(ptr) }
    }
}

/// An iterator over the entries of a C [`ffi::dict`].
///
/// `'a` is the lifetime of the underlying dict data.  Wraps
/// [`ffi::RS_dictGetIterator`] / [`ffi::RS_dictNext`] /
/// [`ffi::RS_dictReleaseIterator`] and releases the underlying C iterator on
/// drop.
pub struct DictIterator<'a, K = *mut c_void, V = *mut c_void> {
    iter: *mut ffi::dictIterator,
    _phantom: PhantomData<(&'a (), K, V)>,
}

impl<'a, K, V> DictIterator<'a, K, V> {
    /// Create an iterator directly from a raw `dict*`.
    ///
    /// Prefer [`Dict::iter`] when a [`Dict`] reference is available.
    ///
    /// # Safety
    ///
    /// `dict` must be a valid, non-null `dict*` whose keys and values are
    /// of types `K` and `V`, and that remains live for `'a`.
    pub unsafe fn new(dict: *mut ffi::dict) -> Self {
        // SAFETY: caller guarantees dict is valid and non-null.
        let iter = unsafe { ffi::RS_dictGetIterator(dict) };
        Self { iter, _phantom: PhantomData }
    }
}

impl<'a, K: FromDictPtr<'a>, V: FromDictPtr<'a>> Iterator for DictIterator<'a, K, V> {
    type Item = DictEntry<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: self.iter is a valid iterator obtained during construction
        // and not yet released.
        let entry = unsafe { ffi::RS_dictNext(self.iter) };
        if entry.is_null() { None } else { Some(DictEntry::new(entry)) }
    }
}

impl<K, V> Drop for DictIterator<'_, K, V> {
    fn drop(&mut self) {
        // SAFETY: self.iter is a valid iterator obtained during construction.
        unsafe { ffi::RS_dictReleaseIterator(self.iter) };
    }
}
