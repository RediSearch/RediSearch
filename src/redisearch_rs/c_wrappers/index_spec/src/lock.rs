/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The spec's read and write locks, and the guards that give access under them.

use std::{
    ffi::{c_char, c_void},
    marker::PhantomData,
    ops::Deref,
    ptr,
    ptr::NonNull,
    slice,
    sync::atomic::{AtomicUsize, Ordering},
};

use c_trie::{SuffixTrie, TermsTrie};
use dict::{Dict, KeysDictType, MissingFieldDictType};
use field_spec::FieldSpec;
use inverted_index::opaque::InvertedIndex;
use schema_rule::SchemaRule;

use crate::{IndexSpec, IndexSpecStrongRef};

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
    pub(crate) fn new(index_spec: &'lock mut ffi::IndexSpec) -> Self {
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

    /// Returns a mutable reference to the spec's document table.
    pub const fn doc_table_mut(&mut self) -> &mut ffi::DocTable {
        &mut self.0.docs
    }

    /// Return a mutable reference to the `existingDocs` inverted index, if present.
    pub fn existing_docs_mut(&mut self) -> Option<&mut InvertedIndex> {
        if self.0.existingDocs.is_null() {
            return None;
        }
        // SAFETY: We hold the write lock and the pointer is non-null. existingDocs
        // is a valid Box<InvertedIndex> allocated by NewInvertedIndex_Ex.
        Some(unsafe { &mut *self.0.existingDocs.cast::<InvertedIndex>() })
    }

    /// Free the `existingDocs` inverted index and set the field to null.
    ///
    /// No-op if `existingDocs` is already null.
    pub fn clear_existing_docs(&mut self) {
        if !self.0.existingDocs.is_null() {
            // SAFETY: existingDocs was allocated as a Box<InvertedIndex> by
            // NewInvertedIndex_Ex. We hold the write lock and are the sole owner.
            unsafe { drop(Box::from_raw(self.0.existingDocs.cast::<InvertedIndex>())) };
            self.0.existingDocs = ptr::null_mut();
        }
    }

    /// Returns the spec's fields as a mutable slice.
    pub fn field_specs_mut(&mut self) -> &mut [FieldSpec] {
        let len = self.0.numFields as usize;
        if len == 0 {
            return &mut [];
        }
        debug_assert!(!self.0.fields.is_null(), "fields must not be null");
        // SAFETY: `fields` is a valid array of `numFields` ffi::FieldSpec elements that lives
        // at least as long as this guard, and we hold the write lock so exclusive mutable
        // access is sound. FieldSpec is #[repr(transparent)] over ffi::FieldSpec.
        unsafe { slice::from_raw_parts_mut(self.0.fields.cast::<FieldSpec>(), len) }
    }

    /// Returns the terms trie.
    pub fn terms_mut(&mut self) -> &mut TermsTrie {
        debug_assert!(!self.0.terms.is_null(), "terms trie must not be null");
        // SAFETY: `terms` is a valid, non-null `Trie` for a properly initialised IndexSpec,
        // and it lives as long as the spec, which outlives this guard's borrow. We hold the
        // write lock, which every other reader and writer of the trie — Rust or C — must
        // acquire, so the exclusive borrow is not aliased.
        unsafe { TermsTrie::from_raw_mut(self.0.terms) }
    }

    /// Returns the suffix trie.
    ///
    /// `None` when no field in the schema was declared `WITHSUFFIXTRIE`: the trie is
    /// only allocated once some field opts in.
    pub const fn suffix_mut(&mut self) -> Option<&mut SuffixTrie> {
        if self.0.suffix.is_null() {
            return None;
        }
        // SAFETY: `suffix` is non-null (checked above) and, once allocated, stays valid
        // for the life of the spec, which outlives this guard's borrow. We hold the write
        // lock, which every other reader and writer of the trie must acquire, so the
        // exclusive borrow is not aliased.
        Some(unsafe { SuffixTrie::from_raw_mut(self.0.suffix) })
    }

    /// Returns the keys dictionary, mapping each TEXT term to its inverted index.
    pub fn keys_dict_mut(&mut self) -> &mut Dict<KeysDictType> {
        debug_assert!(!self.0.keysDict.is_null(), "keys_dict is null");
        // SAFETY: `keysDict` is a valid non-null dict* created with `invIdxDictType`
        // (`KeysDictType::as_ptr()`), and lives as long as the spec (1.). We hold the
        // write lock, so there are no other live references and no concurrent C
        // access (2.).
        unsafe { Dict::from_raw_mut(self.0.keysDict) }
    }

    /// Return the spec's `missing.indexes` as a typed [`Dict`].
    pub fn missing_field_dict_mut(&mut self) -> &mut Dict<MissingFieldDictType> {
        debug_assert!(
            !self.0.missing.indexes.is_null(),
            "missing.indexes must not be null"
        );
        // SAFETY: `missing.indexes` is a valid non-null dict* created with
        // `dictTypeHeapHiddenStrings` (`MissingFieldDictType::as_ptr()`), and lives
        // as long as the spec (1.). We hold the write lock, so there are no other
        // live references and no concurrent C access (2.).
        unsafe { Dict::from_raw_mut(self.0.missing.indexes) }
    }

    /// Apply a signed delta to the spec's `totalInvertedIndexBlocks` counter.
    ///
    /// Uses a relaxed atomic add, matching `IndexStats_BlockCountAdd` in C.
    /// Negative deltas work via two's-complement wrapping of the `usize` field.
    /// No-op when `delta` is zero.
    pub fn add_block_count(&mut self, delta: i64) {
        if delta != 0 {
            // SAFETY: totalInvertedIndexBlocks is documented in spec.h to require
            // atomic access (relaxed writes, relaxed reads). We hold the write lock
            // so we are the only writer; concurrent readers use atomic loads.
            let atomic =
                unsafe { AtomicUsize::from_ptr(&raw mut self.0.stats.totalInvertedIndexBlocks) };

            // `as usize` reinterprets the two's-complement bits of a negative `delta`,
            // and `fetch_add` wraps on overflow, so a negative `delta` is effectively
            // subtracted.
            atomic.fetch_add(delta as usize, Ordering::Relaxed);
        }
    }

    /// Update the spec-level statistics after applying a GC delta.
    pub const fn update_gc_stats(
        &mut self,
        entries_removed: usize,
        bytes_freed: usize,
        bytes_allocated: usize,
        terms_size_removed: usize,
        terms_removed: usize,
    ) {
        self.0.stats.numRecords -= entries_removed;
        self.0.stats.invertedSize += bytes_allocated;
        self.0.stats.invertedSize -= bytes_freed;
        self.0.stats.termsSize -= terms_size_removed;
        self.0.stats.scoring.numTerms -= terms_removed;
    }
}

impl Deref for IndexSpecWriteGuard<'_> {
    type Target = IndexSpec;

    fn deref(&self) -> &IndexSpec {
        // SAFETY: `IndexSpec` is transparent over `ffi::IndexSpec`, and the guard's
        // borrow keeps the spec valid for the returned shared borrow.
        unsafe { IndexSpec::from_raw(self.0) }
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
    /// 1. `index_spec` must be a valid pointer to an IndexSpec
    /// 2. Caller must have already acquired the read lock
    /// 3. C code is responsible for releasing the lock
    /// 4. The keys dict must be paused for incremental rehashing.
    /// 5. The missing docs dict must be in a fully hashed state.
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
    pub fn doc_exists(&self, id: rqe_core::DocId) -> bool {
        // SAFETY: docs is a valid DocTable for a properly initialised IndexSpec.
        unsafe { ffi::DocTable_Exists(&self.0.docs, id) }
    }

    /// Snapshot which documents exist, as a bitmap indexed by document id, or [`None`] when
    /// the table's maximum document id makes the bitmap too large to allocate.
    ///
    /// Within the table's current maximum document id, bit `id` agrees with
    /// [`doc_exists`](Self::doc_exists) until the table next changes.
    pub fn live_doc_bitmap(&self) -> Option<Box<[u64]>> {
        // SAFETY: docs is a valid DocTable for a properly initialised IndexSpec.
        let words = unsafe { ffi::DocTable_LiveBitmapWords(&self.0.docs) };
        if words == 0 {
            return None;
        }
        let mut live = vec![0; words].into_boxed_slice();
        // SAFETY: docs is a valid DocTable as above, and `live` holds `words` writable words.
        unsafe { ffi::DocTable_FillLiveBitmap(&self.0.docs, live.as_mut_ptr(), words) };
        Some(live)
    }

    /// Call `f` once with the id of every document [`doc_exists`](Self::doc_exists)
    /// reports, in no particular order.
    pub fn for_each_live_doc<F: FnMut(rqe_core::DocId)>(&self, mut f: F) {
        unsafe extern "C" fn call<F: FnMut(rqe_core::DocId)>(ctx: *mut c_void, id: ffi::t_docId) {
            // SAFETY: `ctx` is the `&mut F` passed below, which outlives the walk.
            let f = unsafe { &mut *ctx.cast::<F>() };
            f(id);
        }
        // SAFETY: docs is a valid DocTable as above, and `ctx` points to `f`, which
        // `call::<F>` reads back as an `F`.
        unsafe {
            ffi::DocTable_ForEachLiveId(&self.0.docs, Some(call::<F>), (&raw mut f).cast());
        }
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

    /// Return the spec's `missing.indexes` as a typed [`Dict`].
    pub fn missing_field_dict(&self) -> &Dict<MissingFieldDictType> {
        debug_assert!(
            !self.0.missing.indexes.is_null(),
            "missing.indexes must not be null"
        );
        // SAFETY: `missing.indexes` is a valid non-null dict* created with
        // `dictTypeHeapHiddenStrings` (`MissingFieldDictType::as_ptr()`), and
        // lives as long as the spec (1.). `missing.indexes` is always in a
        // fully hashed state (2.).
        unsafe { Dict::from_raw(self.0.missing.indexes) }
    }

    /// Returns the terms trie.
    pub fn terms(&self) -> &TermsTrie {
        debug_assert!(!self.0.terms.is_null(), "terms trie must not be null");
        // SAFETY: `terms` is a valid, non-null `Trie` for a properly initialised IndexSpec,
        // and it lives as long as the spec, which outlives this guard's borrow. The guard
        // holds the spec's read lock, which every writer of the terms trie must not hold
        // concurrently, so the trie is not mutated for that borrow either.
        unsafe { TermsTrie::from_raw(self.0.terms) }
    }

    /// Returns the keys dictionary, mapping each TEXT term to its inverted index.
    pub fn keys_dict(&self) -> &Dict<KeysDictType> {
        debug_assert!(!self.0.keysDict.is_null(), "keys_dict is null");
        // SAFETY: `keysDict` is a valid non-null dict* created with `invIdxDictType`
        // (`KeysDictType::as_ptr()`), and lives as long as the spec (1.).
        // `IndexSpecReadGuard::from_locked` point 4 ensures it does not
        // incrementally rehash (2.).
        unsafe { Dict::from_raw(self.0.keysDict) }
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
    pub const fn missing_field_dict_ptr(&self) -> *mut ffi::dict {
        self.0.missing.indexes
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

impl Deref for IndexSpecReadGuard<'_> {
    type Target = IndexSpec;

    fn deref(&self) -> &IndexSpec {
        // SAFETY: `IndexSpec` is `#[repr(transparent)]` over `ffi::IndexSpec`, so a
        // `&ffi::IndexSpec` can be reinterpreted as a `&IndexSpec`. The reference lives
        // at least as long as this guard.
        unsafe { IndexSpec::from_raw(self.0) }
    }
}

impl Drop for IndexSpecReadGuard<'_> {
    fn drop(&mut self) {
        // This Drop implementation should never run because:
        // 1. from_locked() (the only way to create this guard) returns ManuallyDrop
        // 2. C code is responsible for releasing the read lock via IndexSpec_Unlock
        //
        // If this runs, it's a bug - the guard was created incorrectly or manually unwrapped.
        panic!(
            "IndexSpecReadGuard::drop() should never run. \
             This guard must be wrapped in ManuallyDrop at FFI boundaries."
        );
    }
}

/// The read lock of an [`IndexSpec`], taken by [`IndexSpecStrongRef::read`] and
/// released when this value is dropped.
///
/// Only a raw pointer is kept, so no reference to the spec is live when
/// [`Drop`] unlocks it through that pointer; [`guard`](Self::guard) hands out
/// the read-locked accessors, borrowed from this lock.
///
/// The lock borrows the strong reference rather than holding one of its own, so
/// it works on a spec invalidated since its promotion. A lock that is forgotten
/// rather than dropped stays taken, after its spec is freed too.
pub struct IndexSpecReadLock<'spec> {
    pub(crate) spec: NonNull<ffi::IndexSpec>,
    pub(crate) _strong_ref: PhantomData<&'spec IndexSpecStrongRef>,
}

impl IndexSpecReadLock<'_> {
    /// The read-locked accessors, for as long as this lock is borrowed.
    pub const fn guard(&self) -> std::mem::ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: the strong reference this lock borrows keeps the spec alive and
        // initialized for `'_`.
        let spec = unsafe { self.spec.as_ref() };
        // SAFETY: (1) `spec` is valid for `'_`, (2) the read lock is held for `'_`
        // and (3) released by `IndexSpecReadLock::drop`, which `'_` ends before,
        // (4) `IndexSpec_LockRead` paused keys-dict rehashing, and (5) the indexer
        // finishes every missing-docs dict rehash before releasing its write lock.
        unsafe { IndexSpecReadGuard::from_locked(spec) }
    }
}

impl Drop for IndexSpecReadLock<'_> {
    fn drop(&mut self) {
        // SAFETY: `IndexSpecStrongRef::read` took this read lock on this thread,
        // and the borrow of the strong reference keeps the spec alive until here.
        unsafe { ffi::IndexSpec_Unlock(self.spec.as_ptr()) };
    }
}
