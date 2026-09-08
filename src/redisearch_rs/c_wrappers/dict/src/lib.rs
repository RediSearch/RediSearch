/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrappers around the C [`ffi::dict`] type.

use std::cell::UnsafeCell;
use std::ffi::{c_char, c_int, c_void};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::slice;

use hidden_string::HiddenString;
use inverted_index::opaque::InvertedIndex;

/// Describes a `Dict` type defined by a `ffi::dictType` with conversion
/// functions for keys and values.
///
/// # Safety
///
/// Implementers must ensure that:
/// - [`as_ptr`](Self::as_ptr) returns a pointer to a valid, live
///   `ffi::dictType` whose callbacks are consistent with `K` and `V`.
/// - [`with_key_ptr`](Self::with_key_ptr) produces a pointer the `dictType`
///   can manage (e.g. copy via `keyDup`), valid for the whole closure call.
/// - [`key_from_ptr`](Self::key_from_ptr) correctly reconstructs a key from
///   the raw pointer stored by the dict.
/// - the `dictType`'s `valDestructor` frees a value as a `Box<V>` — the dict
///   takes ownership on insert and drops it on removal.
pub unsafe trait DictType {
    /// Key type used for insertion and returned during iteration. `'a` is the
    /// dict-borrow lifetime, tying iterated keys to the underlying dict storage.
    type K<'a>;
    /// The stored value. Owned by the dict as a `Box<V>` and freed by the
    /// `dictType`'s `valDestructor`.
    type V;

    /// Return a pointer to the static C `dictType` for this configuration.
    fn as_ptr() -> *mut ffi::dictType;

    /// Hand `key` to `f` as the raw pointer the dict's callbacks expect.
    fn with_key_ptr<R>(key: Self::K<'_>, f: impl FnOnce(*mut c_void) -> R) -> R;

    /// Reconstruct a key from the raw pointer stored in a dict entry.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer to a key consistent with `K`, valid for `'a`.
    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a>;
}

/// [`DictType`] marker for the spec's `missingFieldDict`.
pub struct MissingFieldDictType;

// SAFETY:
// - missingFieldDictType is a valid static ffi::dictType whose callbacks are
//   compatible with HiddenString keys and Box<InvertedIndex> values.
// - keyDup copies the HiddenString so callers may free the original after insert.
// - valDestructor is InvIndFreeCb → InvertedIndex_Free, which calls
//   drop(Box::from_raw(ptr)), matching the `Box<V>` ownership the trait assumes.
unsafe impl DictType for MissingFieldDictType {
    type K<'a> = &'a HiddenString;
    type V = InvertedIndex;

    fn as_ptr() -> *mut ffi::dictType {
        &raw mut ffi::missingFieldDictType
    }

    fn with_key_ptr<R>(key: Self::K<'_>, f: impl FnOnce(*mut c_void) -> R) -> R {
        f(key.as_ptr().cast_mut().cast())
    }

    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a> {
        // SAFETY: caller guarantees ptr is a valid *mut HiddenString for 'a.
        unsafe { HiddenString::from_raw(ptr.cast::<ffi::HiddenString>()) }
    }
}

/// [`DictType`] marker for the spec's `keysDict`.
pub struct KeysDictType;

// SAFETY:
// - `invIdxDictType` is the static `ffi::dictType` this marker describes, and
//   its callbacks are compatible with `CharBuf`-wrapped byte keys and
//   `Box<InvertedIndex>` values.
// - `with_key_ptr` passes a `CharBuf` that lives for the whole call, and
//   `CharBuf_KeyDup` copies both the struct and the bytes on insert, so the
//   dict never retains a borrow of the caller's slice.
// - `key_from_ptr` reads back the `CharBuf` the dict stored.
// - valDestructor is `InvIndFreeCb` → `InvertedIndex_Free`, which drops a
//   `Box`, matching the `Box<V>` ownership the trait assumes.
unsafe impl DictType for KeysDictType {
    type K<'a> = &'a [u8];
    type V = InvertedIndex;

    fn as_ptr() -> *mut ffi::dictType {
        &raw mut ffi::invIdxDictType
    }

    fn with_key_ptr<R>(key: Self::K<'_>, f: impl FnOnce(*mut c_void) -> R) -> R {
        // `CharBuf_HashFunction` forwards the length to a C function that accepts an `int`.
        assert!(
            key.len() <= std::ffi::c_int::MAX as usize,
            "dictionary key length must fit in c_int"
        );
        let mut key = ffi::CharBuf {
            buf: key.as_ptr().cast_mut().cast::<c_char>(),
            len: key.len(),
        };
        f((&raw mut key).cast())
    }

    unsafe fn key_from_ptr<'a>(ptr: *mut c_void) -> Self::K<'a> {
        // SAFETY: caller guarantees ptr is a valid *mut CharBuf for 'a.
        let key = unsafe { &*ptr.cast::<ffi::CharBuf>() };
        // `CharBuf_KeyDup` stores `rm_malloc(0)` for an empty key, which may be
        // null — not a pointer `slice::from_raw_parts` accepts.
        if key.len == 0 {
            return &[];
        }
        // SAFETY: a stored `CharBuf` describes `len` initialised bytes owned by
        // the dict, live for 'a.
        unsafe { slice::from_raw_parts(key.buf.cast::<u8>(), key.len) }
    }
}

/// A safe wrapper around a C [`ffi::dict`].
///
/// `DT` is a [`DictType`] marker that fixes the key type ([`DictType::K`]),
/// stored value type ([`DictType::V`]), and the underlying C `dictType`
/// callbacks.
///
/// Obtained from a raw `*const`/`*mut ffi::dict` via [`Dict::from_raw`] /
/// [`Dict::from_raw_mut`]. For an owned dict that is released on drop, use
/// [`OwnedDict`].
///
/// # Interior mutability
///
/// The dict sits in an [`UnsafeCell`] because a *lookup* can write to it: when
/// the dict is mid-resize, `RS_dictFind` performs an incremental rehash step,
/// moving a bucket and updating the dict's bookkeeping. A `&Dict` therefore
/// cannot promise the bytes stay untouched. The cell makes that write legal
/// under the aliasing model; keeping it race-free is [`Dict::from_raw`]'s
/// clause 2.
#[repr(transparent)]
pub struct Dict<DT: DictType> {
    inner: UnsafeCell<ffi::dict>,
    _phantom: PhantomData<DT>,
}

impl<DT: DictType> Dict<DT> {
    /// Borrow a [`Dict`] from a raw const pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to an `ffi::dict` created with
    ///    the `ffi::dictType` described by `DT`, and must remain live for `'a`.
    /// 2. Rehashing must be excluded (paused (`pauserehash > 0`) or not in progress
    ///    (`rehashidx == -1`)) whenever another thread may access the dict during `'a`.
    pub unsafe fn from_raw<'a>(ptr: *const ffi::dict) -> &'a Self {
        // SAFETY: the caller guarantees `ptr` points to a valid `ffi::dict`.
        debug_assert!(std::ptr::eq(unsafe { (*ptr).type_ }, DT::as_ptr()));
        // SAFETY: #[repr(transparent)] guarantees identical layout.
        // Validity and liveness are the caller's responsibility.
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Borrow a [`Dict`] mutably from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to an `ffi::dict` created with
    ///    the `ffi::dictType` described by `DT`, must remain live for `'a`, and the
    ///    caller must have exclusive access to it.
    pub unsafe fn from_raw_mut<'a>(ptr: *mut ffi::dict) -> &'a mut Self {
        // SAFETY: the caller guarantees `ptr` points to a valid `ffi::dict`.
        debug_assert!(std::ptr::eq(unsafe { (*ptr).type_ }, DT::as_ptr()));
        // SAFETY: #[repr(transparent)] guarantees identical layout.
        // Validity, liveness, and exclusivity are the caller's responsibility.
        unsafe { ptr.cast::<Self>().as_mut().unwrap() }
    }

    /// Return a raw const pointer to the underlying [`ffi::dict`].
    pub const fn as_ptr(&self) -> *const ffi::dict {
        self.inner.get()
    }

    /// Return a raw mutable pointer to the underlying [`ffi::dict`].
    pub const fn as_mut_ptr(&mut self) -> *mut ffi::dict {
        self.inner.get()
    }

    /// Insert a key–value pair into this dict.
    ///
    /// Returns `Ok(())` when the entry was added. Returns `Err(val)` when the
    /// key already existed — ownership of `val` is returned to the caller.
    ///
    /// The key is only borrowed for the duration of this call; `RS_dictAddRaw`
    /// copies it via `keyDup` if a new entry is allocated.
    pub fn try_insert(&mut self, key: DT::K<'_>, val: Box<DT::V>) -> Result<(), Box<DT::V>> {
        // SAFETY: self points to a valid dict; key is valid for the duration of the lookup;
        // RS_dictAddRaw copies the key via keyDup if it allocates a new entry.
        // Passing null for the `existing` out-param: the C function checks for
        // null before writing, and we only need the return value here.
        let new_entry = DT::with_key_ptr(key, |key| unsafe {
            ffi::RS_dictAddRaw(self.as_mut_ptr(), key, std::ptr::null_mut())
        });
        if new_entry.is_null() {
            // Key already existed; val was never consumed.
            return Err(val);
        }
        // New entry allocated; set the value. Ownership passes to the dict.
        // SAFETY: new_entry is valid and non-null (returned by RS_dictAddRaw above).
        let entry = unsafe { &mut *new_entry };
        entry.v.val = Box::into_raw(val).cast();
        Ok(())
    }

    /// Fetch a shared reference to the value stored under `key`, if present.
    ///
    /// The key is only borrowed for the duration of this call.
    ///
    /// Takes `&self`: the lookup may rehash-step the dict, which the
    /// [`UnsafeCell`] permits and [`Dict::from_raw`]'s clause 2 keeps race-free.
    pub fn fetch(&self, key: DT::K<'_>) -> Option<&DT::V> {
        // SAFETY: self points to a valid dict; key is valid for the duration of the lookup.
        let val_ptr = DT::with_key_ptr(key, |key| unsafe {
            ffi::RS_dictFetchValue(self.as_ptr().cast_mut(), key)
        });
        if val_ptr.is_null() {
            return None;
        }
        // SAFETY: val_ptr is a non-null value pointer owned by this dict, consistent
        // with &DT::V. We hold `&self`, which excludes any `&mut` to the value.
        Some(unsafe { &*val_ptr.cast::<DT::V>() })
    }

    /// Fetch a mutable reference to the value stored under `key`, if present.
    ///
    /// The key is only borrowed for the duration of this call.
    pub fn fetch_mut(&mut self, key: DT::K<'_>) -> Option<&mut DT::V> {
        // SAFETY: self points to a valid dict; key is valid for the duration of the lookup.
        let val_ptr = DT::with_key_ptr(key, |key| unsafe {
            ffi::RS_dictFetchValue(self.as_mut_ptr(), key)
        });
        if val_ptr.is_null() {
            return None;
        }
        // SAFETY: val_ptr is a non-null value pointer owned by this dict, consistent
        // with &mut DT::V. We hold `&mut self`, so there are no other live references
        // to it.
        Some(unsafe { &mut *val_ptr.cast::<DT::V>() })
    }

    /// Remove the entry for `key` from this dict, returning whether an entry was
    /// found and removed.
    ///
    /// If the dict's `valDestructor` is set, it is invoked to free the removed value.
    /// The key is only borrowed for the duration of this call.
    pub fn remove(&mut self, key: DT::K<'_>) -> bool {
        // SAFETY: self points to a valid dict; key is valid for the duration of the lookup.
        let status = DT::with_key_ptr(key, |key| unsafe {
            ffi::RS_dictDelete(self.as_mut_ptr(), key)
        });
        status == ffi::DICT_OK as c_int
    }

    /// Iterate over all entries in this dict.
    ///
    /// Each entry is yielded as a [`DictEntryRef`] whose [`DictEntryRef::key`] and
    /// [`DictEntryRef::val`] return the typed key and value directly.
    ///
    /// The iterator holds a C-side iterator object and releases it on drop,
    /// so it is safe to abandon iteration early.
    ///
    /// Uses C's *safe* iterator, which pauses rehashing from the first step
    /// until release. Without that, a [`fetch`](Dict::fetch) alongside a live
    /// iterator could relocate an entry out from under it: both take `&self`, so
    /// neither the borrow checker nor a `&mut` receiver keeps them apart.
    pub fn iter(&self) -> DictIterator<'_, DT> {
        // SAFETY: self points to a valid dict, live for the iterator's borrow.
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
        // SAFETY: `self.ptr` is a valid dict (1) and is, by definition of this
        // `OwnedDict` wrapper not being `Sync`, not shared on other threads (2).
        unsafe { Dict::from_raw(self.ptr.as_ptr()) }
    }
}

impl<DT: DictType> DerefMut for OwnedDict<DT> {
    fn deref_mut(&mut self) -> &mut Dict<DT> {
        // SAFETY: `self.ptr` is a valid dict (1) and the only existing reference (2).
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
/// Wraps [`ffi::RS_dictGetSafeIterator`] / [`ffi::RS_dictNext`] /
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
        let iter = unsafe { ffi::RS_dictGetSafeIterator(dict) };
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
    pub fn val(&self) -> &'dict DT::V {
        // SAFETY: self.entry is a valid non-null dictEntry*.
        let entry = unsafe { &*self.entry };
        // SAFETY: v.val is the pointer-sized value field of the dictEntry union.
        let ptr = unsafe { entry.v.val };
        // SAFETY: ptr is the value of a valid dictEntry, consistent with
        // &DT::V, and valid for 'dict (the dict-borrow lifetime). No mutable
        // aliasing: same argument as key() above.
        unsafe { &*ptr.cast::<DT::V>() }
    }
}
