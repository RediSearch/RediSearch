/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::node::Node;
use memchr::memmem::Finder;

/// Iterates over all the entries in a [`TrieMap`](crate::TrieMap) that contain the target fragment,
/// in lexicographical order.
///
/// Invoke [`TrieMap::contains_iter`](crate::TrieMap::contains_iter) to create an instance of this iterator.
pub struct ContainsIter<'tm, Data> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<StackItem<'tm, Data>>,
    /// Concatenation of the labels of current node and its ancestors,
    /// i.e. the key of the current node.
    key: Vec<u8>,
    /// The target fragment we are looking for.
    finder: Finder<'tm>,
}

struct StackItem<'a, Data> {
    node: &'a Node<Data>,
    was_visited: bool,
    /// Set to `true` if we can skip checking if the current key contains the target fragment.
    ///
    /// This happens when the concatenation of the parent nodes of [`Self::node`] have already
    /// been verified to contain the target fragment, thus allowing us to avoid redundant work.
    skip_check: bool,
}

impl<'tm, Data> ContainsIter<'tm, Data> {
    /// Creates a new contains iterator over the entries of a [`TrieMap`](crate::TrieMap).
    pub(crate) fn new(root: Option<&'tm Node<Data>>, target: &'tm [u8]) -> Self {
        let finder = Finder::new(target);
        Self {
            stack: root
                .into_iter()
                .map(|node| StackItem {
                    node,
                    was_visited: false,
                    skip_check: false,
                })
                .collect(),
            key: vec![],
            finder,
        }
    }
}

impl<'tm, Data> ContainsIter<'tm, Data> {
    /// The current key, obtained by concatenating the labels of the nodes
    /// between the root and the current node.
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    /// Advance this iterator to the next node, and set the
    /// key to the one matching that node's entry
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let StackItem {
                node,
                was_visited,
                skip_check,
            } = self.stack.pop()?;

            if was_visited {
                // We have now visited this node and all its descendants.
                // We restore the key to the value matching its parent.
                self.key
                    .truncate(self.key.len() - node.label_len() as usize);
                continue;
            }

            // Push the current node into the stack to remember, once all
            // its descendants have been visited, to remove its label
            // from the key buffer.
            self.stack.push(StackItem {
                node,
                was_visited: true,
                skip_check,
            });
            self.key.extend(node.label());

            let is_match = skip_check || self.finder.find(&self.key).is_some();

            self.stack.reserve(node.children().len());
            for child in node.children().iter().rev() {
                self.stack.push(StackItem {
                    node: child,
                    was_visited: false,
                    skip_check: is_match,
                });
            }

            if is_match {
                if let Some(data) = node.data() {
                    return Some(data);
                }
            }
        }
    }
}

impl<'tm, Data> Iterator for ContainsIter<'tm, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}
