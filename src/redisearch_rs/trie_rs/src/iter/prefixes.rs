/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use memchr::arch::all::is_prefix;

use crate::node::Node;

/// Iterate over all trie entries whose key is a prefix of [`Self::target`].
///
/// It can be instantiated by calling [`TrieMap::prefixes_iter`](crate::TrieMap::prefixes_iter).
pub struct PrefixesIter<'tm, Data> {
    /// The node whose prefixes we are looking for.
    target: &'tm [u8],
    /// The node we are currently examining.
    current_node: Option<&'tm Node<Data>>,
    /// Concatenation of the labels of current node and its ancestors,
    /// i.e. the key of the current node.
    key: Vec<u8>,
}

impl<'tm, Data> PrefixesIter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn new(root: Option<&'tm Node<Data>>, target: &'tm [u8]) -> Self {
        Self {
            current_node: root,
            key: vec![],
            target,
        }
    }

    /// Advance this iterator to the next node, and set the
    /// key to the one matching that node's entry
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let current: &Node<_> = self.current_node.take()?;

            // We only visit a node if all its precedessors were prefixes of `self.target`.
            // We can thus check exclusively the key portion that belongs to this label.
            if !is_prefix(&self.target[self.key.len()..], current.label()) {
                return None;
            }

            self.key.extend(current.label());

            // If the current key is strictly shorter than the target, there is a chance
            // than one of its descendants is a prefix we need to include in our result set.
            if self.target.len() > self.key.len() {
                if let Some(next_char) = self.target.get(self.key.len()) {
                    self.current_node = current.child_starting_with(*next_char);
                }
            }

            if let Some(data) = current.data() {
                return Some(data);
            } else {
                continue;
            }
        }
    }
}

impl<'tm, Data> Iterator for PrefixesIter<'tm, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}
