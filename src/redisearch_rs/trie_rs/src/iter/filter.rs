/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Utilities to control which nodes are visited during iteration.
use memchr::arch::all::is_prefix;
use wildcard::{MatchOutcome, WildcardPattern};

#[derive(Clone, Copy)]
/// The outcome of [`TraversalFilter::filter`].
pub struct FilterOutcome {
    /// If `false`, the key and value associated with the current
    /// node won't be yielded by the iterator.
    pub yield_current: bool,
    /// If `false`, the entire subtree rooted in the current node
    /// will be skipped.
    pub visit_descendants: bool,
}

/// A mechanism to control which nodes are visited during iteration.
pub trait TraversalFilter {
    /// Determine whether the current node should be yielded
    /// and whether its descendants should be visited.
    ///
    /// The filter takes as an input the key associated with
    /// the current node—i.e. the concatenation of the
    /// labels associated with every node between the
    /// root of the trie and the current one.
    fn filter(&self, key: &[u8]) -> FilterOutcome;
}

/// Implement the trait for all closures that match the expected signature.
impl<F> TraversalFilter for F
where
    F: Fn(&[u8]) -> FilterOutcome,
{
    fn filter(&self, key: &[u8]) -> FilterOutcome {
        self(key)
    }
}

/// The simplest filter: visit all nodes, no exceptions.
pub struct VisitAll;

impl TraversalFilter for VisitAll {
    fn filter(&self, _key: &[u8]) -> FilterOutcome {
        FilterOutcome {
            yield_current: true,
            visit_descendants: true,
        }
    }
}

/// Returns all trie entries that match the given wildcard pattern.
pub struct WildcardFilter<'a>(WildcardPattern<'a>);

impl<'a> From<WildcardPattern<'a>> for WildcardFilter<'a> {
    fn from(value: WildcardPattern<'a>) -> Self {
        Self(value)
    }
}

impl TraversalFilter for WildcardFilter<'_> {
    fn filter(&self, key: &[u8]) -> FilterOutcome {
        match self.0.matches(key) {
            MatchOutcome::Match => FilterOutcome {
                yield_current: true,
                // If the pattern matches inputs of a given length,
                // and the current key is a match, it follows that
                // it won't match any of its descendants, since they'll be
                // at least one character longer.
                visit_descendants: self.0.expected_length().is_none(),
            },
            MatchOutcome::PartialMatch => FilterOutcome {
                yield_current: false,
                visit_descendants: true,
            },
            MatchOutcome::NoMatch => FilterOutcome {
                yield_current: false,
                visit_descendants: false,
            },
        }
    }
}

/// Return all trie entries whose key is a prefix of `self.0`.
pub struct IsPrefixFilter<'a> {
    haystack: &'a [u8],
}

impl<'a> IsPrefixFilter<'a> {
    pub fn new(needle: &'a [u8]) -> Self {
        Self { haystack: needle }
    }
}
impl TraversalFilter for IsPrefixFilter<'_> {
    fn filter(&self, key: &[u8]) -> FilterOutcome {
        let is_prefix = is_prefix(self.haystack, key);
        FilterOutcome {
            yield_current: is_prefix,
            visit_descendants: is_prefix && key.len() < self.haystack.len(),
        }
    }
}
