/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::trie_map::node::Node;

use super::{
    Iter, LendingIter,
    filter::{FilterOutcome, TraversalFilter},
};
use rqe_wildcard::{MatchOutcome, WildcardPattern};

/// Per-key filter-based wildcard iterator. Public-named because it
/// appears as the inner value of
/// [`WildcardIter::Filter`](super::automaton::WildcardIter::Filter),
/// but it can only be obtained through that dispatcher — the direct
/// constructor on [`TrieMap`](crate::TrieMap) is crate-private.
pub struct WildcardFilterIter<'tm, 'p, Data>(Iter<'tm, Data, WildcardFilter<'p>>);

impl<'tm, 'p, Data> WildcardFilterIter<'tm, 'p, Data> {
    pub(crate) fn new(root: Option<&'tm Node<Data>>, pattern: WildcardPattern<'p>) -> Self {
        let iter = match root {
            Some(root) => {
                // If the first portion of the pattern is a literal, we can jumping directly
                // to the subtree of the trie containing the terms under that prefix
                // (if there are any).
                if let Some(rqe_wildcard::Token::Literal(lit)) = pattern.tokens().first() {
                    match root.find_root_for_prefix(lit) {
                        Some((subroot, subroot_prefix)) => Iter::new(Some(subroot), subroot_prefix),
                        None => Iter::empty(),
                    }
                } else {
                    Iter::new(Some(root), vec![])
                }
            }
            None => Iter::empty(),
        }
        .traversal_filter(WildcardFilter(pattern));
        Self(iter)
    }

    /// Advance to the next matching entry; returns a reference to its data.
    ///
    /// Mirrors [`super::automaton::AutomatonIter::advance`] so that the
    /// auto-dispatching
    /// [`super::automaton::WildcardIter`] can plug a filter-based
    /// fallback into its `advance` / `key` interface without special-casing.
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        self.0.advance()
    }

    /// The current key — the concatenation of labels from the trie root to
    /// the node yielded by the most recent [`Self::advance`] call.
    pub(crate) fn key(&self) -> &[u8] {
        self.0.key()
    }
}

impl<'tm, 'p, Data> Iterator for WildcardFilterIter<'tm, 'p, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'tm, 'p, Data> From<WildcardFilterIter<'tm, 'p, Data>>
    for LendingIter<'tm, Data, WildcardFilter<'p>>
{
    fn from(iter: WildcardFilterIter<'tm, 'p, Data>) -> Self {
        iter.0.into()
    }
}

/// A [`TraversalFilter`] that keeps only keys matching the given [`WildcardPattern`].
pub struct WildcardFilter<'p>(WildcardPattern<'p>);

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
