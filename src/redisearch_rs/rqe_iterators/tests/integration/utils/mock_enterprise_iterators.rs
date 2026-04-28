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

use rqe_iterators::{
    RQEIterator, SEARCH_ENTERPRISE_ITERATORS, SearchEnterpriseIterators, wildcard::Wildcard,
};

/// The `top_id` used by the wildcard returned from
/// [`MockEnterpriseIterators::new_wildcard_on_disk`].
///
/// Tests that exercise the disk-wildcard path can call `num_estimated()` on the
/// resulting iterator and compare against this sentinel to confirm the disk path
/// was taken.
pub(crate) const MOCK_DISK_WILDCARD_TOP_ID: ffi::t_docId = 53596;

/// Minimal [`SearchEnterpriseIterators`] stub for tests that exercise the
/// disk-index code paths.
///
/// `new_wildcard_on_disk` returns a [`Wildcard`] with [`MOCK_DISK_WILDCARD_TOP_ID`]
/// as its `top_id`, so callers which delegates `num_estimated` to their inner wildcard
/// can observe through it that the disk path was taken.
pub(crate) struct MockEnterpriseIterators;

impl SearchEnterpriseIterators for MockEnterpriseIterators {
    fn new_wildcard_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>> {
        Ok(Box::new(Wildcard::new(MOCK_DISK_WILDCARD_TOP_ID, weight)))
    }

    fn new_term_on_disk_with_offsets<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _query_term: Box<query_term::RSQueryTerm>,
        _field_mask: inverted_index::t_fieldMask,
        _weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!(
            "MockEnterpriseIterators::new_term_on_disk_with_offsets not used in these tests"
        )
    }

    fn new_term_on_disk_without_offsets<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _query_term: Box<query_term::RSQueryTerm>,
        _field_mask: inverted_index::t_fieldMask,
        _weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!(
            "MockEnterpriseIterators::new_term_on_disk_without_offsets not used in these tests"
        )
    }

    fn new_tag_on_disk<'index>(
        &self,
        _index: &'index mut ffi::RedisSearchDiskIndexSpec,
        _token: &ffi::RSToken,
        _field_index: ffi::t_fieldIndex,
        _weight: f64,
    ) -> Result<Box<dyn RQEIterator<'index> + 'index>, Box<dyn std::error::Error>> {
        unimplemented!("MockEnterpriseIterators::new_tag_on_disk not used in these tests")
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
