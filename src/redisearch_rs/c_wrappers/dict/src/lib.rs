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
/// functions for keys and values.
///
/// Two separate value types model the ownership split between insertion and
/// iteration:
///
/// - [`InsertV`](Self::InsertV) — what the caller passes on insertion. Must
///   match the ownership contract of the dict's `valDestructor`: for owning
///   dicts (e.g. `InvIndFreeCb`) this should be an owned type such as
///   `Box<V>`; for non-owning dicts it can be a raw pointer or reference.
/// - [`RefV`](Self::RefV) — what is yielded during iteration. Always a
///   borrow from the dict for the dict-borrow lifetime `'a`.
///
/// # Safety
///
/// Implementers must ensure that:
/// - [`as_ptr`](Self::as_ptr) returns a pointer to a valid, live
///   `ffi::dictType` whose callbacks are consistent with `K`, `InsertV`, and
///   `RefV`.
/// - [`key_from_ptr`](Self::key_from_ptr) correctly reconstructs a key from
///   the raw pointer stored by the dict.
/// - [`key_into_ptr`](Self::key_into_ptr) produces a pointer the `dictType`
///   can manage (e.g. copy via `keyDup`).
/// - [`insert_val_into_ptr`](Self::insert_val_into_ptr) converts the owned
///   insert value to the raw pointer stored by the dict. Ownership semantics
///   must match the `valDestructor` in the `dictType`.
/// - [`iter_val_from_ptr`](Self::iter_val_from_ptr) correctly borrows the
///   value stored at `ptr` for lifetime `'a`.
pub unsafe trait DictType {
    /// Key type used for insertion and returned during iteration. `'a` is the
    /// dict-borrow lifetime, tying iterated keys to the underlying dict storage.
    type K<'a>;
    /// Owned value type for insertion. When `valDestructor` is set, this
    /// should be an owned type (e.g. `Box<V>`) whose memory the dict will
    /// free on removal.
    type InsertV;
    /// Borrowed value type yielded during iteration. `'a` is the dict-borrow
    /// lifetime.
    type RefV<'a>;
    /// Mutably borrowed value type yielded by [`Dict::fetch_mut`]. `'a` is the
    /// dict-borrow lifetime.
    type MutV<'a>;

    /// Return a pointer to the static C `dictType` for this configuration.
    fn as_ptr() -> *mut ffi::dictType;

    /// Convert a key to the raw pointer to be passed to the dict.
    fn key_into_ptr<'a>(key: Self::K<'a>) -> *mut c_void;

    /// Reconstruct a key from the raw pointer stored in a dict entry.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer to a key consistent with `K`, valid for `'a`.
    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a>;

    /// Convert an owned insert value to the raw pointer to be stored by the dict.
    fn insert_val_into_ptr(val: Self::InsertV) -> *mut c_void;

    /// Reconstruct a borrowed value from the raw pointer stored in a dict entry.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer to a value consistent with `RefV`, valid for `'a`.
    unsafe fn iter_val_from_ptr<'a>(ptr: *mut c_void) -> Self::RefV<'a>;

    /// Reconstruct a mutably borrowed value from the raw pointer stored in a dict entry.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer to a value consistent with `MutV`, valid for `'a`,
    /// with no other live references to it.
    unsafe fn mut_val_from_ptr<'a>(ptr: *mut c_void) -> Self::MutV<'a>;
}

/// [`DictType`] marker for the spec's `missingFieldDict`.
pub struct MissingFieldDictType;

// SAFETY:
// - missingFieldDictType is a valid static ffi::dictType whose callbacks are
//   compatible with HiddenStringRef keys and Box<InvertedIndex> values.
// - keyDup copies the HiddenString so callers may free the original after insert.
// - valDestructor is InvIndFreeCb → InvertedIndex_Free, which calls
//   drop(Box::from_raw(ptr)); insert_val_into_ptr calls Box::into_raw,
//   transferring ownership to the dict.
// - iter_val_from_ptr borrows the stored pointer as Option<&InvertedIndex>.
unsafe impl DictType for MissingFieldDictType {
    type K<'a> = HiddenStringRef<'a>;
    type InsertV = Box<InvertedIndex>;
    type RefV<'a> = &'a InvertedIndex;
    type MutV<'a> = &'a mut InvertedIndex;

    fn as_ptr() -> *mut ffi::dictType {
        std::ptr::addr_of_mut!(ffi::missingFieldDictType)
    }

    fn key_into_ptr<'a>(key: Self::K<'a>) -> *mut c_void {
        key.as_ptr().cast()
    }

    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a> {
        // SAFETY: caller guarantees ptr is a valid *mut HiddenString for 'a.
        unsafe { HiddenStringRef::from_raw(ptr.cast::<ffi::HiddenString>()) }
    }

    fn insert_val_into_ptr(val: Self::InsertV) -> *mut c_void {
        Box::into_raw(val).cast()
    }

    unsafe fn iter_val_from_ptr<'a>(ptr: *mut c_void) -> Self::RefV<'a> {
        // SAFETY: caller guarantees ptr is a valid, non-null *mut InvertedIndex for 'a.
        unsafe { &*ptr.cast::<InvertedIndex>() }
    }

    unsafe fn mut_val_from_ptr<'a>(ptr: *mut c_void) -> Self::MutV<'a> {
        // SAFETY: caller guarantees ptr is a valid, non-null *mut InvertedIndex for 'a,
        // with no other live references to it.
        unsafe { &mut *ptr.cast::<InvertedIndex>() }
    }
}

/// A safe wrapper around a C [`ffi::dict`].
///
/// `DT` is a [`DictType`] marker that fixes the key type ([`DictType::K`]),
/// insert value type ([`DictType::InsertV`]), iteration value type
/// ([`DictType::RefV`]), and the underlying C `dictType` callbacks.
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

    /// Insert a key–value pair into this dict.
    ///
    /// Returns `Ok(())` when the entry was added. Returns `Err(val)` when the
    /// key already existed — ownership of `val` is returned to the caller.
    ///
    /// The key is only borrowed for the duration of this call; `RS_dictAddRaw`
    /// copies it via `keyDup` if a new entry is allocated.
    pub fn try_insert(&mut self, key: DT::K<'_>, val: DT::InsertV) -> Result<(), DT::InsertV> {
        // SAFETY: self points to a valid dict; key is valid for the lookup;
        // RS_dictAddRaw copies the key via keyDup if it allocates a new entry.
        // Passing null for the `existing` out-param: the C function checks for
        // null before writing, and we only need the return value here.
        let new_entry = unsafe {
            ffi::RS_dictAddRaw(
                self.as_mut_ptr(),
                DT::key_into_ptr(key),
                std::ptr::null_mut(),
            )
        };
        if new_entry.is_null() {
            // Key already existed; val was never consumed.
            return Err(val);
        }
        // New entry allocated; set the value. Ownership passes to the dict.
        // SAFETY: new_entry is valid and non-null (returned by RS_dictAddRaw above).
        let entry = unsafe { &mut *new_entry };
        entry.v.val = DT::insert_val_into_ptr(val);
        Ok(())
    }

    /// Fetch a mutable reference to the value stored under `key`, if present.
    ///
    /// The key is only borrowed for the duration of this call.
    pub fn fetch_mut(&mut self, key: DT::K<'_>) -> Option<DT::MutV<'_>> {
        // SAFETY: self points to a valid dict; key is valid for the lookup.
        let val_ptr = unsafe { ffi::RS_dictFetchValue(self.as_mut_ptr(), DT::key_into_ptr(key)) };
        if val_ptr.is_null() {
            return None;
        }
        // SAFETY: val_ptr is a non-null value pointer owned by this dict, consistent
        // with DT::MutV. We hold `&mut self`, so there are no other live references
        // to it.
        Some(unsafe { DT::mut_val_from_ptr(val_ptr) })
    }

    /// Remove the entry for `key` from this dict, if present.
    ///
    /// If the dict's `valDestructor` is set, it is invoked to free the removed value.
    /// The key is only borrowed for the duration of this call.
    pub fn remove(&mut self, key: DT::K<'_>) {
        // SAFETY: self points to a valid dict; key is valid for the lookup.
        unsafe { ffi::RS_dictDelete(self.as_mut_ptr(), DT::key_into_ptr(key)) };
    }

    /// Iterate over all entries in this dict.
    ///
    /// Each entry is yielded as a [`DictEntryRef`] whose [`DictEntryRef::key`] and
    /// [`DictEntryRef::val`] return the typed key and value directly.
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
/// [`try_insert`](Dict::try_insert) — are available on `&OwnedDict` and
/// `&mut OwnedDict` respectively.
///
/// For a non-owning wrapper around an existing C dict pointer, use [`Dict`]
/// directly via [`Dict::from_raw`] or [`Dict::from_raw_mut`].
pub struct OwnedDict<DT: DictType> {
    ptr: NonNull<ffi::dict>,
    _phantom: PhantomData<DT>,
}

impl<DT: DictType> OwnedDict<DT> {
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

    /// Return a raw mutable pointer to the underlying [`ffi::dict`].
    pub const fn as_mut_ptr(&self) -> *mut ffi::dict {
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

/// An iterator over the entries of a [`Dict`].
///
/// `'dict` is the lifetime of the underlying dict data.
///
/// Wraps [`ffi::RS_dictGetIterator`] / [`ffi::RS_dictNext`] /
/// [`ffi::RS_dictReleaseIterator`] and releases the underlying C iterator on
/// drop.
pub struct DictIterator<'dict, DT: DictType> {
    iter: *mut ffi::dictIterator,
    _phantom: PhantomData<(&'dict (), DT)>,
}

impl<'dict, DT: DictType> DictIterator<'dict, DT> {
    /// Create an iterator directly from a raw `dict*`.
    ///
    /// # Safety
    ///
    /// - `dict` must be a valid, non-null `dict*` consistent with the key and
    ///   value types of `DT`, and must remain live for `'dict`.
    unsafe fn new(dict: *mut ffi::dict) -> Self {
        // SAFETY: caller guarantees dict is valid and non-null.
        let iter = unsafe { ffi::RS_dictGetIterator(dict) };
        Self {
            iter,
            _phantom: PhantomData,
        }
    }
}

impl<'dict, DT: DictType> Iterator for DictIterator<'dict, DT> {
    type Item = DictEntryRef<'dict, DT>;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: self.iter is a valid iterator obtained during construction.
        let entry = unsafe { ffi::RS_dictNext(self.iter) };
        if entry.is_null() {
            None
        } else {
            // SAFETY: RS_dictNext returns a valid, non-null dictEntry* consistent
            // with DT, and it remains valid for 'dict (the dict borrow lifetime).
            Some(unsafe { DictEntryRef::new(entry) })
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
/// `'dict` is the lifetime of the underlying dict data.
pub struct DictEntryRef<'dict, DT: DictType> {
    entry: *mut ffi::dictEntry,
    _phantom: PhantomData<(&'dict (), DT)>,
}

impl<'dict, DT: DictType> DictEntryRef<'dict, DT> {
    /// Wrap a raw `dictEntry*` as a [`DictEntryRef`].
    ///
    /// # Safety
    ///
    /// - `entry` must be a valid, non-null pointer to a `dictEntry` whose key
    ///   and value are consistent with `DT`, and must remain valid for `'dict`.
    const unsafe fn new(entry: *mut ffi::dictEntry) -> Self {
        Self {
            entry,
            _phantom: PhantomData,
        }
    }

    /// Return the key of this entry.
    pub fn key(&self) -> DT::K<'dict> {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let ptr = unsafe { (*self.entry).key };
        // SAFETY: ptr is the key of a valid dictEntry, consistent with DT::K,
        // and valid for 'dict (the dict-borrow lifetime). No mutable aliasing:
        // DictEntryRef<'dict> is only produced by DictIterator<'dict>, which is
        // created via &Dict — a shared borrow that excludes any &mut Dict.
        unsafe { DT::key_from_ptr(ptr) }
    }

    /// Return the value of this entry as a borrow from the dict.
    pub fn val(&self) -> DT::RefV<'dict> {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let entry = unsafe { &*self.entry };
        // SAFETY: v.val is the pointer-sized value field of the dictEntry union.
        let ptr = unsafe { entry.v.val };
        // SAFETY: ptr is the value of a valid dictEntry, consistent with
        // DT::RefV, and valid for 'dict (the dict-borrow lifetime). No mutable
        // aliasing: same argument as key() above.
        unsafe { DT::iter_val_from_ptr(ptr) }
    }
}
