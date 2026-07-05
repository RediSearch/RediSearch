/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Deferred result production for lazily-populated iterators.
//!
//! Some iterators (e.g. a vector range query) are cheap to build but expensive to
//! populate, and the population step must run *outside* the spec lock so writes can
//! proceed concurrently (see MOD-16437). A [`Producer`] is a one-shot closure that
//! yields the results, deferring the expensive computation until the first
//! `read`/`skip_to` of the owning iterator — by which point the caller has released
//! the lock.

use rqe_core::DocId;

use crate::{RQEIteratorError, utils::OwnedSlice};

/// The results yielded by a [`Producer`] when it is first run.
pub struct ProducedResults {
    /// The matching document IDs, sorted iff the owning iterator is sorted by id.
    pub ids: OwnedSlice<DocId>,
    /// The per-id metric (e.g. vector distance) values, parallel to `ids`.
    /// `None` when the owning iterator does not yield a metric.
    pub metrics: Option<OwnedSlice<f64>>,
}

/// A closure that produces an iterator's results on first access.
///
/// Returns [`RQEIteratorError::TimedOut`] if the underlying computation timed out before
/// producing results.
///
/// # Lifetime
///
/// The owning iterator calls this exactly once (on the first `read`/`skip_to`) but **keeps it
/// alive for its whole lifetime**, dropping it only when the iterator itself is dropped. This
/// matters when the producer owns state that the underlying query engine may still reference
/// after the call returns — e.g. a deferred vector range query whose `VecSimQueryParams`
/// (and its timeout context) must outlive the query, mirroring how the KNN hybrid iterator
/// keeps its `runtimeParams`/`timeoutCtx` for its whole lifetime. Hence [`FnMut`] (retained)
/// rather than [`FnOnce`] (consumed and dropped on first call).
pub type Producer<'a> = Box<dyn FnMut() -> Result<ProducedResults, RQEIteratorError> + 'a>;
