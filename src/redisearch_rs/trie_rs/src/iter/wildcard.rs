/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::node::Node;

use super::{
    Iter, LendingIter,
    filter::{FilterOutcome, TraversalFilter},
};
use rqe_wildcard::{MatchOutcome, WildcardPattern};

/// An iterator over all entries that match the given wildcard pattern.
///
/// It can be instantiated by calling [`TrieMap::wildcard_iter`](crate::TrieMap::wildcard_iter).
pub struct WildcardIter<'tm, 'p, Data>(Iter<'tm, Data, WildcardFilter<'p>>);

impl<'tm, 'p, Data> WildcardIter<'tm, 'p, Data> {
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
}

impl<'tm, 'p, Data> Iterator for WildcardIter<'tm, 'p, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'tm, 'p, Data> From<WildcardIter<'tm, 'p, Data>> for LendingIter<'tm, Data, WildcardFilter<'p>> {
    fn from(iter: WildcardIter<'tm, 'p, Data>) -> Self {
        iter.0.into()
    }
}

/// Returns all trie entries that match the given wildcard pattern.
pub struct WildcardFilter<'a>(WildcardPattern<'a>);

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
