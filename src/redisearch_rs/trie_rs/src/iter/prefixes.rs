/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::node::Node;
use memchr::arch::all::is_prefix;

/// Iterate over all trie entries whose key is a prefix of [`Self::target`].
///
/// It can be instantiated by calling [`TrieMap::prefixes_iter`](crate::TrieMap::prefixes_iter).
pub struct PrefixesIter<'tm, Data> {
    /// The term whose prefixes we are looking for.
    target: &'tm [u8],
    /// The node we are currently examining.
    current_node: Option<&'tm Node<Data>>,
}

impl<'tm, Data> PrefixesIter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn new(root: Option<&'tm Node<Data>>, target: &'tm [u8]) -> Self {
        Self {
            current_node: root,
            target,
        }
    }
}

impl<'tm, Data> Iterator for PrefixesIter<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let current: &Node<_> = self.current_node.take()?;

            // We only visit a node if all its precedessors were prefixes of `self.target`.
            // We can thus check exclusively the key portion that belongs to this label.
            if !is_prefix(self.target, current.label()) {
                return None;
            }

            // When we move on to the next node, we only need to examine the remaining
            // characters in the target. We can thus "discard" what has already been
            // compared to the current label.
            self.target = &self.target[current.label_len() as usize..];

            // If target is not empty, there is a chance than one of the descendants
            // of the current node is a prefix we need to include in our result set.
            if let Some(next_char) = self.target.first() {
                self.current_node = current.child_starting_with(*next_char);
            }

            if let Some(data) = current.data() {
                return Some(data);
            } else {
                continue;
            }
        }
    }
}
