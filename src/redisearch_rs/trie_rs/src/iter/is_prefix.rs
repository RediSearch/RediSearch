/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use memchr::arch::all::is_prefix;

use super::filter::{FilterOutcome, TraversalFilter};

/// Return all trie entries whose key is a prefix of [`Self::target`].
pub struct IsPrefixFilter<'a> {
    target: &'a [u8],
}

impl<'a> IsPrefixFilter<'a> {
    pub fn new(target: &'a [u8]) -> Self {
        Self { target }
    }
}

impl TraversalFilter for IsPrefixFilter<'_> {
    fn filter(&self, key: &[u8], label_offset: usize) -> FilterOutcome {
        debug_assert!(self.target.len() >= label_offset);

        // We only visit a node if all its precedessors were prefixes of `self.target`.
        // We can thus check exclusively the key portion that belongs to this label.
        let is_prefix = is_prefix(&self.target[label_offset..], &key[label_offset..]);
        FilterOutcome {
            yield_current: is_prefix,
            visit_descendants: is_prefix && key.len() < self.target.len(),
        }
    }
}
