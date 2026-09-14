/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around a search-on-disk index handle.
//!
//! Search-on-disk stores an index in a [`ffi::RedisSearchDiskIndexSpec`] owned
//! by the disk backend rather than in the in-memory structures. This crate
//! exposes a thin, safe API over that handle so the rest of the Rust codebase
//! can query it without scattering raw FFI calls and null checks across call
//! sites.

use std::ptr::NonNull;

use inverted_index::NumericFilter;
use query_term::RSQueryTerm;
use rqe_core::{DocId, FieldIndex, FieldMask};
use rqe_iterators::{RQEIteratorPrintable, SEARCH_ENTERPRISE_ITERATORS};

/// A handle to a search-on-disk index ([`ffi::RedisSearchDiskIndexSpec`]).
///
/// Holds a non-null, valid pointer to the disk index spec for the lifetime of
/// the handle, so its accessor methods are safe to call.
pub struct SearchDiskHandle(NonNull<ffi::RedisSearchDiskIndexSpec>);

impl SearchDiskHandle {
    /// Wrap a raw `disk_spec` pointer, yielding [`None`] when it is null.
    ///
    /// A null `disk_spec` means the index is not backed by search-on-disk, so
    /// there is no handle to wrap.
    ///
    /// # Safety
    ///
    /// When `disk_spec` is non-null it must point to a [valid]
    /// [`ffi::RedisSearchDiskIndexSpec`] that remains valid for the lifetime of
    /// the returned [`SearchDiskHandle`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn new(disk_spec: *mut ffi::RedisSearchDiskIndexSpec) -> Option<Self> {
        NonNull::new(disk_spec).map(Self)
    }

    /// The highest document ID currently assigned in the disk index, or `0`
    /// when the index is empty.
    pub fn max_doc_id(&self) -> DocId {
        // SAFETY: by the invariant established in `new`, the wrapped pointer is
        // a valid `RedisSearchDiskIndexSpec`, which is exactly what
        // `SearchDisk_GetMaxDocId` requires.
        unsafe { ffi::SearchDisk_GetMaxDocId(self.0.as_ptr()) }
    }

    /// Build a numeric filter iterator backed by this on-disk index.
    ///
    /// Consumes the handle and delegates to the registered enterprise numeric
    /// iterator, which yields one entry per document whose numeric value matches
    /// `filter`.
    ///
    /// # Safety
    ///
    /// 1. The wrapped disk index spec must remain valid for `'index`.
    /// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized (always the case
    ///    when the spec is backed by a disk index).
    /// 3. `snapshot` must be a
    ///    [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle for
    ///    this disk spec that remains valid for `'index`.
    /// 4. There must be no other live reference to the wrapped spec for `'index`.
    pub unsafe fn new_numeric_iterator<'index>(
        self,
        filter: &NumericFilter,
        field_index: FieldIndex,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        // SAFETY: precondition (2) — a disk-backed spec always has the
        // enterprise iterators registered.
        let api = SEARCH_ENTERPRISE_ITERATORS
            .get()
            .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");

        // SAFETY: `new`'s invariant guarantees `self.0` points to a valid
        // `RedisSearchDiskIndexSpec`; preconditions (1)/(4) uphold the `'index`
        // lifetime and exclusive access of the returned reference.
        let disk_spec = unsafe { &mut *self.0.as_ptr() };

        api.new_numeric_on_disk(disk_spec, filter, field_index, snapshot)
    }

    /// Build a geo-radius filter iterator backed by this on-disk index.
    ///
    /// Consumes the handle and delegates to the registered enterprise geo
    /// iterator. Geo fields are stored geohash-encoded in the numeric column
    /// family, so `gf` is decomposed into numeric range filters that are run
    /// through the same disk machinery as [`new_numeric_iterator`], unioned,
    /// and post-filtered by true great-circle distance.
    ///
    /// [`new_numeric_iterator`]: SearchDiskHandle::new_numeric_iterator
    ///
    /// # Safety
    ///
    /// 1. The wrapped disk index spec must remain valid for `'index`.
    /// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized (always the case
    ///    when the spec is backed by a disk index).
    /// 3. `snapshot` must be a
    ///    [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle for
    ///    this disk spec that remains valid for `'index`.
    /// 4. There must be no other live reference to the wrapped spec for `'index`.
    pub unsafe fn new_geo_iterator<'index>(
        self,
        gf: &'index mut ffi::GeoFilter,
        field_index: ffi::t_fieldIndex,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        // SAFETY: precondition (2) — a disk-backed spec always has the
        // enterprise iterators registered.
        let api = SEARCH_ENTERPRISE_ITERATORS
            .get()
            .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");

        // SAFETY: `new`'s invariant guarantees `self.0` points to a valid
        // `RedisSearchDiskIndexSpec`; preconditions (1)/(4) uphold the `'index`
        // lifetime and exclusive access of the returned reference.
        let disk_spec = unsafe { &mut *self.0.as_ptr() };

        api.new_geo_on_disk(disk_spec, gf, field_index, snapshot)
    }

    /// Build a term iterator backed by this on-disk index.
    ///
    /// Consumes the handle and delegates to the registered enterprise term
    /// iterator, which yields one entry per document containing `query_term`
    /// filtered by `field_mask`. The `needs_offsets` flag selects the
    /// offset-carrying or offset-free variant (the latter is cheaper and used by
    /// scorers that don't need term positions).
    ///
    /// `query_term` carries its own scoring metadata (`idf`/`bm25_idf`), which
    /// the caller is expected to populate before the call.
    ///
    /// # Safety
    ///
    /// 1. The wrapped disk index spec must remain valid for `'index`.
    /// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized (always the case
    ///    when the spec is backed by a disk index).
    /// 3. `snapshot` must be a
    ///    [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle for
    ///    this disk spec that remains valid for `'index`.
    /// 4. There must be no other live reference to the wrapped spec for `'index`.
    pub unsafe fn new_term_iterator<'index>(
        self,
        query_term: Box<RSQueryTerm>,
        field_mask: FieldMask,
        weight: f64,
        needs_offsets: bool,
        snapshot: NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<Box<dyn RQEIteratorPrintable<'index> + 'index>, Box<dyn std::error::Error>> {
        // SAFETY: precondition (2) — a disk-backed spec always has the
        // enterprise iterators registered.
        let api = SEARCH_ENTERPRISE_ITERATORS
            .get()
            .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");

        // SAFETY: `new`'s invariant guarantees `self.0` points to a valid
        // `RedisSearchDiskIndexSpec`; preconditions (1)/(4) uphold the `'index`
        // lifetime and exclusive access of the returned reference.
        let disk_spec = unsafe { &mut *self.0.as_ptr() };

        if needs_offsets {
            api.new_term_on_disk_with_offsets(disk_spec, query_term, field_mask, weight, snapshot)
        } else {
            api.new_term_on_disk_without_offsets(
                disk_spec, query_term, field_mask, weight, snapshot,
            )
        }
    }
}
