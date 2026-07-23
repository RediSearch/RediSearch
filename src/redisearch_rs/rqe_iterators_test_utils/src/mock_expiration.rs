/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A mock [`ExpirationChecker`] for testing expiration handling without TTL tables.

use std::collections::HashSet;

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterators::ExpirationChecker;

/// A mock expiration checker that marks a fixed set of doc ids as expired.
///
/// This lets a consumer of [`ExpirationChecker`] exercise its own skip-on-expired
/// logic without standing up a real TTL table and search context.
#[derive(Debug, Clone, Default)]
pub struct MockExpirationChecker {
    expired_docs: HashSet<DocId>,
}

impl MockExpirationChecker {
    /// Create a checker that reports the given doc ids as expired.
    pub const fn new(expired_docs: HashSet<DocId>) -> Self {
        Self { expired_docs }
    }

    /// Mark an additional doc id as expired.
    pub fn mark_expired(&mut self, doc_id: DocId) {
        self.expired_docs.insert(doc_id);
    }
}

impl ExpirationChecker for MockExpirationChecker {
    fn has_expiration(&self) -> bool {
        !self.expired_docs.is_empty()
    }

    fn is_expired(&self, result: &RSIndexResult) -> bool {
        self.expired_docs.contains(&result.doc_id)
    }
}
