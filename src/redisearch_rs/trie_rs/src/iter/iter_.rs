/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::filter::{TraversalFilter, VisitAll};
use crate::node::Node;

/// Iterates over the entries of a [`TrieMap`](crate::TrieMap) in lexicographical order.
///
/// Invoke [`TrieMap::iter`](crate::TrieMap::iter) or [`TrieMap::prefixed_iter`](crate::TrieMap::prefixed_iter)
/// to create an instance of this iterator.
pub struct Iter<'tm, Data, F> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<(&'tm Node<Data>, bool)>,
    /// Determine if the current node should be yielded and
    /// if its children should be visited.
    filter: F,
    /// Concatenation of the labels of current node and its ancestors,
    /// i.e. the key of the current node.
    key: Vec<u8>,
}

impl<'a, Data, F> Iter<'a, Data, F> {
    /// Change the traversal filter used by this iterator.
    pub fn traversal_filter<F1>(self, f: F1) -> Iter<'a, Data, F1>
    where
        F1: TraversalFilter,
    {
        let Self { stack, key, .. } = self;
        Iter {
            stack,
            filter: f,
            key,
        }
    }
}

impl<'tm, Data> Iter<'tm, Data, VisitAll> {
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn new(root: Option<&'tm Node<Data>>, prefix: Vec<u8>) -> Self {
        Self::filtered(root, prefix, VisitAll)
    }

    /// Creates a new empty iterator, that yields no entries.
    pub(crate) fn empty() -> Self {
        Self::filtered(None, vec![], VisitAll)
    }
}

impl<'tm, Data, F> Iter<'tm, Data, F>
where
    F: TraversalFilter,
{
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn filtered(
        root: Option<&'tm Node<Data>>,
        prefix: Vec<u8>,
        visit_descendants: F,
    ) -> Self {
        Self {
            stack: root.into_iter().map(|node| (node, false)).collect(),
            key: prefix,
            filter: visit_descendants,
        }
    }

    /// The current key, obtained by concatenating the labels of the nodes
    /// between the root and the current node.
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    /// Advance this iterator to the next node, and set the
    /// key to the one matching that node's entry
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let (node, was_visited) = self.stack.pop()?;

            if !was_visited {
                self.stack.push((node, true));
                self.key.extend(node.label());

                let filter_outcome = self.filter.filter(&self.key);

                if filter_outcome.visit_descendants {
                    for child in node.children().iter().rev() {
                        self.stack.push((child, false));
                    }
                }

                if filter_outcome.yield_current {
                    if let Some(data) = node.data() {
                        return Some(data);
                    }
                }
            } else {
                self.key
                    .truncate(self.key.len() - node.label_len() as usize);
            }
        }
    }
}

impl<'tm, Data, F> Iterator for Iter<'tm, Data, F>
where
    F: TraversalFilter,
{
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}
