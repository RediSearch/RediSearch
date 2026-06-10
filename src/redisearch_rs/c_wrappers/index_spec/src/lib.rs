/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::IndexSpec`].

use std::{ffi::c_char, ptr::NonNull, slice};

use field_spec::FieldSpec;
use inverted_index::opaque::InvertedIndex;
use schema_rule::SchemaRule;

/// A safe wrapper around an `ffi::IndexSpec`.
#[repr(transparent)]
pub struct IndexSpec(ffi::IndexSpec);

impl IndexSpec {
    /// Create a safe `IndexSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid], non-null pointer to an `ffi::IndexSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::IndexSpec) -> &'a Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Create a mutable `IndexSpec` wrapper from a non-null pointer.
    ///
    /// # Safety
    /// 1. `ptr` must be a [valid], non-null pointer to an `ffi::IndexSpec` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::IndexSpec) -> &'a mut Self {
        // Safety: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_mut().unwrap() }
    }

    /// Get the underlying schema rule.
    pub const fn rule(&self) -> &SchemaRule {
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { SchemaRule::from_raw(self.0.rule) }
    }

    /// Get the underlying field specs as a slice of `FieldSpec`s.
    pub fn field_specs(&self) -> &[FieldSpec] {
        debug_assert!(!self.0.fields.is_null(), "fields must not be null");
        let data = self.0.fields.cast::<FieldSpec>();
        let len = self.0.numFields.into();
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { slice::from_raw_parts(data, len) }
    }

    /// Acquire the write lock for this `IndexSpec`. This is required before performing any
    /// modifications to the index spec, such as applying deltas from compaction.
    pub fn write(&mut self) -> IndexSpecWriteGuard<'_> {
        IndexSpecWriteGuard::new(&mut self.0)
    }
}

/// A guard that holds the IndexSpec write lock and releases it on drop.
///
/// This guard implements the RAII pattern: the lock is acquired when the guard
/// is created and released when the guard is dropped, ensuring the lock is always
/// released even on early returns or panics.
///
/// # Invariants
///
/// The pointer passed to [`IndexSpecWriteGuard::new`] must be a valid `IndexSpec*`
/// that remains valid for the lifetime of this guard.
pub struct IndexSpecWriteGuard<'lock>(&'lock mut ffi::IndexSpec);

impl<'lock> IndexSpecWriteGuard<'lock> {
    /// Creates a new guard, acquiring the write lock.
    ///
    /// Returns `None` if the pointer is null.
    fn new(index_spec: &'lock mut ffi::IndexSpec) -> Self {
        // Safety: (1.) due to creation with `IndexSpec::from_raw`, and caller must ensure proper synchronization when mutating.
        unsafe { ffi::IndexSpec_AcquireWriteLock(index_spec) };
        Self(index_spec)
    }

    /// Decrements the document count for a term.
    pub fn decrement_trie_term_count(&mut self, term: &[u8], decr: u64) -> bool {
        // SAFETY: We hold the write lock (enforced by Self::new),
        // and the C side handles non-null-terminated strings with length.
        unsafe {
            ffi::IndexSpec_DecrementTrieTermCount(
                self.0,
                term.as_ptr() as *const c_char,
                term.len(),
                decr as usize,
            )
        }
    }

    /// Decrements the num terms counter by the given amount.
    pub fn decrement_num_terms(&mut self, decr: u64) {
        // SAFETY: We hold the write lock (enforced by Self::new).
        unsafe {
            ffi::IndexSpec_DecrementNumTerms(self.0, decr);
        }
    }

    /// Creates a guard from an already-locked IndexSpec without acquiring the lock.
    ///
    /// Returns `ManuallyDrop<IndexSpecWriteGuard>` to prevent the guard from releasing
    /// the lock on drop. This is intended for test code where no real lock is acquired
    /// and the test code maintains responsibility for the spec lifetime.
    ///
    /// # Safety
    ///
    /// - `index_spec` must be a valid pointer to an IndexSpec
    /// - In test contexts, no lock is actually held (tests don't use real locks)
    /// - Test code is responsible for ensuring exclusive access
    ///
    /// # Example
    ///
    /// ```ignore
    /// // In test code where no real lock is needed:
    /// let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut *spec_ptr) };
    /// // guard is ManuallyDrop, won't release lock on drop
    /// guard.decrement_num_terms(5);
    /// ```
    pub const unsafe fn from_locked_mut(
        index_spec: &'lock mut ffi::IndexSpec,
    ) -> std::mem::ManuallyDrop<Self> {
        std::mem::ManuallyDrop::new(Self(index_spec))
    }

    /// Get a mutable reference to the underlying schema rule.
    pub const fn rule_mut(&mut self) -> &mut SchemaRule {
        // SAFETY: We hold the write lock, so we have exclusive access.
        unsafe { SchemaRule::from_raw_mut(self.0.rule) }
    }

    /// Sets the disk index spec pointer.
    pub const fn set_disk_spec(&mut self, disk_spec: *mut ffi::RedisSearchDiskIndexSpec) {
        self.0.diskSpec = disk_spec;
    }

    /// Sets the existing documents inverted index pointer.
    pub const fn set_existing_docs_ptr(&mut self, existing_docs: *mut ffi::InvertedIndex) {
        self.0.existingDocs = existing_docs;
    }

    /// Sets whether documents expiration should be monitored.
    pub const fn set_monitor_document_expiration(&mut self, monitor: bool) {
        self.0.monitorDocumentExpiration = monitor;
    }

    /// Sets whether field expiration should be monitored.
    pub const fn set_monitor_field_expiration(&mut self, monitor: bool) {
        self.0.monitorFieldExpiration = monitor;
    }

    /// Returns the document table.
    pub const fn doc_table(&self) -> ffi::DocTable {
        self.0.docs
    }
}

impl Drop for IndexSpecWriteGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: We acquired the lock in new(), so we must release it.
        unsafe { ffi::IndexSpec_ReleaseWriteLock(self.0) };
    }
}

/// A guard that holds the IndexSpec read lock and releases it on drop.
///
/// This guard implements the RAII pattern for read locks: the lock is acquired
/// when the guard is created and released when dropped.
///
/// # Read vs Write Lock
///
/// This is a **read lock** (shared), allowing multiple concurrent readers.
/// For exclusive write access, use [`IndexSpecWriteGuard`] (write lock).
///
/// # Invariants
///
/// The pointer passed to `from_locked` must be a valid `IndexSpec*` that remains
/// valid for the lifetime of this guard.
pub struct IndexSpecReadGuard<'lock>(&'lock ffi::IndexSpec);

impl<'lock> IndexSpecReadGuard<'lock> {
    /// Creates a guard from an already-locked IndexSpec without acquiring the lock.
    ///
    /// Returns `ManuallyDrop<IndexSpecReadGuard>` to prevent the guard from releasing
    /// the lock on drop. This is intended for FFI boundaries where C code has already
    /// acquired the read lock and maintains ownership of the lock lifecycle.
    ///
    /// # Safety
    ///
    /// - `index_spec` must be a valid pointer to an IndexSpec
    /// - Caller must have already acquired the read lock
    /// - C code is responsible for releasing the lock
    ///
    /// # Example
    ///
    /// ```ignore
    /// // At FFI boundary where C holds the lock:
    /// let mut guard = unsafe { IndexSpecReadGuard::from_locked(&*spec_ptr) };
    /// // guard is ManuallyDrop, won't release lock on drop
    /// iterator.revalidate(&mut *guard)?;
    /// ```
    pub const unsafe fn from_locked(
        index_spec: &'lock ffi::IndexSpec,
    ) -> std::mem::ManuallyDrop<Self> {
        std::mem::ManuallyDrop::new(Self(index_spec))
    }

    /// Check whether the document with the given id exists in this spec's document table.
    pub fn doc_exists(&self, id: ffi::t_docId) -> bool {
        // SAFETY: docs is a valid DocTable for a properly initialised IndexSpec.
        unsafe { ffi::DocTable_Exists(&self.0.docs, id) }
    }

    /// Return the inverted index tracking all existing (live) document IDs, if present.
    ///
    /// This index is only populated when the spec's schema rule has
    /// [`index_all`](ffi::SchemaRule::index_all) set.
    pub fn existing_docs(&self) -> Option<&InvertedIndex> {
        NonNull::new(self.0.existingDocs).map(|existing_docs| {
            // SAFETY: existingDocs is a valid, non-null pointer to an opaque InvertedIndex.
            unsafe { existing_docs.cast::<InvertedIndex>().as_ref() }
        })
    }

    /// Returns whether the keys dictionary is available.
    ///
    /// The keys dictionary maps TEXT terms to their inverted indexes.
    pub const fn has_keys_dict(&self) -> bool {
        !self.0.keysDict.is_null()
    }

    /// Returns a pointer to the keys dictionary.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if keys_dict is null. Use `has_keys_dict()` to check first.
    pub fn keys_dict(&self) -> *mut ffi::dict {
        debug_assert!(!self.0.keysDict.is_null(), "keys_dict is null");
        self.0.keysDict
    }

    /// Returns a pointer to the existing documents inverted index.
    ///
    /// May be null if all documents have been garbage collected.
    pub const fn existing_docs_ptr(&self) -> *mut ffi::InvertedIndex {
        self.0.existingDocs
    }

    /// Returns a pointer to the missing field dictionary.
    ///
    /// This dictionary maps field names to their missing-value inverted indexes.
    pub const fn missing_field_dict(&self) -> *mut ffi::dict {
        self.0.missingFieldDict
    }

    /// Returns a pointer to the fields array.
    pub const fn fields_ptr(&self) -> *mut ffi::FieldSpec {
        self.0.fields
    }

    /// Returns a const raw pointer to the underlying `ffi::IndexSpec`.
    pub const fn as_ptr(&self) -> *const ffi::IndexSpec {
        self.0 as *const ffi::IndexSpec
    }

    /// Returns a mutable raw pointer to the underlying `ffi::IndexSpec`.
    ///
    /// # Safety Note
    ///
    /// Even though this returns `*mut`, the guard only holds a **read lock** (shared).
    /// The mutable pointer is needed for FFI calls that take `*mut` but don't actually
    /// mutate the spec (C convention). Do not perform unsynchronized mutations through
    /// this pointer.
    pub const fn as_mut_ptr(&self) -> *mut ffi::IndexSpec {
        self.0 as *const ffi::IndexSpec as *mut ffi::IndexSpec
    }

    /// Returns the document table.
    pub const fn doc_table(&self) -> ffi::DocTable {
        self.0.docs
    }

    /// Returns the number of strong references to this index spec.
    pub const fn own_ref(&self) -> ffi::StrongRef {
        self.0.own_ref
    }
}

impl Drop for IndexSpecReadGuard<'_> {
    fn drop(&mut self) {
        // This Drop implementation should never run because:
        // 1. from_locked() (the only way to create this guard) returns ManuallyDrop
        // 2. C code is responsible for releasing the read lock via RedisSearchCtx_UnlockSpec
        //
        // If this runs, it's a bug - the guard was created incorrectly or manually unwrapped.
        panic!(
            "IndexSpecReadGuard::drop() should never run. \
             This guard must be wrapped in ManuallyDrop at FFI boundaries."
        );
    }
}
