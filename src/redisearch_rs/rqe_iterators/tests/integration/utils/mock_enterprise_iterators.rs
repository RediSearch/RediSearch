/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A mock implementation of [`SearchEnterpriseIterators`] for use in integration tests.
//!
//! The real disk iterator is part of the closed-source enterprise codebase and is not
//! available in this repository. This mock stands in for it, allowing the open-source
//! test suite to exercise code paths that depend on [`SEARCH_ENTERPRISE_ITERATORS`]
//! without requiring the actual enterprise implementation.

use std::cell::Cell;

use inverted_index::NumericFilter;
use rqe_core::{DocId, FieldIndex};
use rqe_iterators::{
    QueryError, SEARCH_ENTERPRISE_ITERATORS, SearchEnterpriseIterators, TypeErasedRQEIterator,
    wildcard::Wildcard,
};

use super::{Mock, MockRevalidateResult};

/// The `top_id` used by the wildcard returned from
/// [`MockEnterpriseIterators::new_wildcard_on_disk`].
///
/// Tests that exercise the disk-wildcard path can call `num_estimated()` on the
/// resulting iterator and compare against this sentinel to confirm the disk path
/// was taken.
pub(crate) const MOCK_DISK_WILDCARD_TOP_ID: DocId = 53596;

/// The documents of the [`Mock`] that [`MockEnterpriseIterators::new_wildcard_on_disk`]
/// returns after [`next_disk_wildcard_revalidates_with`].
pub(crate) const MOCK_DISK_WILDCARD_DOC_IDS: [DocId; 3] = [10, 20, 30];

thread_local! {
    /// Set by [`next_disk_wildcard_revalidates_with`], consumed by the next
    /// [`MockEnterpriseIterators::new_wildcard_on_disk`] on the same thread.
    static NEXT_DISK_WILDCARD_REVALIDATE: Cell<Option<MockRevalidateResult>> =
        const { Cell::new(None) };
}

/// Make the next [`MockEnterpriseIterators::new_wildcard_on_disk`] call on this
/// thread return a [`Mock`] over [`MOCK_DISK_WILDCARD_DOC_IDS`] whose
/// revalidation reports `result`, instead of the default [`Wildcard`].
///
/// This lets tests drive the disk wrapper through every outcome a backend can
/// report, while still going through the production factory path.
pub(crate) fn next_disk_wildcard_revalidates_with(result: MockRevalidateResult) {
    NEXT_DISK_WILDCARD_REVALIDATE.set(Some(result));
}

/// Minimal [`SearchEnterpriseIterators`] stub for tests that exercise the
/// disk-index code paths.
///
/// `new_wildcard_on_disk` returns a [`Wildcard`] with [`MOCK_DISK_WILDCARD_TOP_ID`]
/// as its `top_id`, so callers which delegates `num_estimated` to their inner wildcard
/// can observe through it that the disk path was taken. A test can swap in a
/// configured [`Mock`] for one call with [`next_disk_wildcard_revalidates_with`].
pub(crate) struct MockEnterpriseIterators;

impl SearchEnterpriseIterators for MockEnterpriseIterators {
    fn new_wildcard_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        weight: f64,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
        _status: Option<&mut QueryError>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        if let Some(result) = NEXT_DISK_WILDCARD_REVALIDATE.take() {
            let mock = Mock::new(MOCK_DISK_WILDCARD_DOC_IDS);
            mock.data().set_revalidate_result(result);
            return Ok(TypeErasedRQEIterator::new(Box::new(mock)));
        }
        Ok(TypeErasedRQEIterator::new(Box::new(Wildcard::new(
            MOCK_DISK_WILDCARD_TOP_ID,
            weight,
        ))))
    }

    fn new_term_on_disk_with_offsets<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _query_term: Box<query_term::RSQueryTerm>,
        _field_mask: inverted_index::FieldMask,
        _weight: f64,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        unimplemented!(
            "MockEnterpriseIterators::new_term_on_disk_with_offsets not used in these tests"
        )
    }

    fn new_term_on_disk_without_offsets<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _query_term: Box<query_term::RSQueryTerm>,
        _field_mask: inverted_index::FieldMask,
        _weight: f64,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        unimplemented!(
            "MockEnterpriseIterators::new_term_on_disk_without_offsets not used in these tests"
        )
    }

    fn new_tag_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _token: &ffi::RSToken,
        _field_index: FieldIndex,
        _weight: f64,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        unimplemented!("MockEnterpriseIterators::new_tag_on_disk not used in these tests")
    }

    fn new_missing_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _field_index: FieldIndex,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        unimplemented!("MockEnterpriseIterators::new_missing_on_disk not used in these tests")
    }

    fn new_numeric_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _filter: &NumericFilter,
        _field_index: FieldIndex,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        unimplemented!("MockEnterpriseIterators::new_numeric_on_disk not used in these tests")
    }

    fn new_geo_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _gf: &'index mut ffi::GeoFilter,
        _field_index: ffi::t_fieldIndex,
        _snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    ) -> Result<TypeErasedRQEIterator<'index>, Box<dyn std::error::Error>> {
        unimplemented!("MockEnterpriseIterators::new_geo_on_disk not used in these tests")
    }
}

/// Initialize [`SEARCH_ENTERPRISE_ITERATORS`] with [`MockEnterpriseIterators`]
/// if it has not been set yet.
///
/// Safe to call from multiple tests in the same binary: subsequent calls are
/// no-ops (the `OnceLock` keeps the first value).
pub(crate) fn init_enterprise_iterators() {
    SEARCH_ENTERPRISE_ITERATORS.get_or_init(|| Box::new(MockEnterpriseIterators));
}
