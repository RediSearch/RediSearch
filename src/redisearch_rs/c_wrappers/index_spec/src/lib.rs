/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::IndexSpec`].

use std::{ffi::c_char, slice};

use field_spec::FieldSpec;
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

    /// Get a mutable reference to the underlying schema rule.
    pub const fn rule_mut(&mut self) -> &mut SchemaRule {
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { SchemaRule::from_raw_mut(self.0.rule) }
    }

    /// Get the underlying field specs as a slice of `FieldSpec`s.
    pub fn field_specs(&self) -> &[FieldSpec] {
        debug_assert!(!self.0.fields.is_null(), "fields must not be null");
        let data = self.0.fields.cast::<FieldSpec>();
        let len = self
            .0
            .numFields
            .try_into()
            .expect("numFields must fit into usize");
        // Safety: (1.) due to creation with `IndexSpec::from_raw`
        unsafe { slice::from_raw_parts(data, len) }
    }

    /// Acquire the write lock for this `IndexSpec`. This is required before performing any
    /// modifications to the index spec, such as applying deltas from compaction.
    pub fn lock(&mut self) -> IndexSpecLockGuard<'_> {
        IndexSpecLockGuard::new(&mut self.0)
    }

    /// Checks if the keys dictionary is available.
    ///
    /// The keys dictionary maps TEXT terms to their inverted indexes.
    /// It may be null in test scenarios where a full spec is not set up.
    ///
    /// # Concurrency
    ///
    /// Caller must hold at least a read lock on the IndexSpec.
    pub const fn has_keys_dict(&self) -> bool {
        !self.0.keysDict.is_null()
    }

    /// Returns a pointer to the existing documents inverted index.
    ///
    /// Used by wildcard queries to match all documents.
    /// May be null if all documents have been garbage collected.
    ///
    /// # Concurrency
    ///
    /// Caller must hold at least a read lock on the IndexSpec.
    pub const fn existing_docs(&self) -> *mut ffi::InvertedIndex {
        self.0.existingDocs
    }

    /// Returns the document table.
    ///
    /// # Concurrency
    ///
    /// Caller must hold at least a read lock on the IndexSpec.
    pub const fn doc_table(&self) -> ffi::DocTable {
        self.0.docs
    }

    /// Returns a const raw pointer to the underlying `ffi::IndexSpec`.
    ///
    /// # Safety
    ///
    /// The returned pointer is valid as long as the wrapper reference is valid.
    /// Do not dereference the pointer after the wrapper goes out of scope.
    pub const fn as_raw_ptr(&self) -> *const ffi::IndexSpec {
        &self.0 as *const ffi::IndexSpec
    }

    /// Returns a mutable raw pointer to the underlying `ffi::IndexSpec`.
    ///
    /// # Safety
    ///
    /// The returned pointer is valid as long as the wrapper reference is valid.
    /// Do not dereference the pointer after the wrapper goes out of scope.
    pub const fn as_mut_raw_ptr(&mut self) -> *mut ffi::IndexSpec {
        &mut self.0 as *mut ffi::IndexSpec
    }

    /// Returns a mutable reference to the underlying `ffi::IndexSpec`.
    ///
    /// This is useful for test code that needs direct mutable access to fields
    /// that don't have dedicated accessor methods.
    pub const fn as_ffi_mut(&mut self) -> &mut ffi::IndexSpec {
        &mut self.0
    }

    /// Returns a pointer to the missing field dictionary.
    ///
    /// This dictionary maps field names to their missing-value inverted indexes.
    ///
    /// # Concurrency
    ///
    /// Caller must hold at least a read lock on the IndexSpec.
    pub const fn missing_field_dict(&self) -> *mut ffi::dict {
        self.0.missingFieldDict
    }

    /// Returns a pointer to the fields array.
    ///
    /// # Concurrency
    ///
    /// Caller must hold at least a read lock on the IndexSpec.
    pub const fn fields_ptr(&self) -> *mut ffi::FieldSpec {
        self.0.fields
    }

    /// Returns the number of strong references to this index spec
    ///
    /// # Concurrency
    ///
    /// Caller must hold at least a read lock on the IndexSpec.
    pub const fn own_ref(&self) -> ffi::StrongRef {
        self.0.own_ref
    }

    /// Sets the disk index spec pointer.
    pub const fn set_disk_spec(&mut self, disk_spec: *mut ffi::RedisSearchDiskIndexSpec) {
        self.0.diskSpec = disk_spec;
    }

    /// Sets the existing documents inverted index pointer.
    pub const fn set_existing_docs(&mut self, existing_docs: *mut ffi::InvertedIndex) {
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
}

/// A guard that holds the IndexSpec write lock and releases it on drop.
///
/// This guard implements the RAII pattern: the lock is acquired when the guard
/// is created and released when the guard is dropped, ensuring the lock is always
/// released even on early returns or panics.
///
/// # Invariants
///
/// The pointer passed to [`IndexSpecLockGuard::new`] must be a valid `IndexSpec*`
/// that remains valid for the lifetime of this guard.
pub struct IndexSpecLockGuard<'lock>(&'lock mut ffi::IndexSpec);

impl<'lock> IndexSpecLockGuard<'lock> {
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
}

impl Drop for IndexSpecLockGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: We acquired the lock in new(), so we must release it.
        unsafe { ffi::IndexSpec_ReleaseWriteLock(self.0) };
    }
}
