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
