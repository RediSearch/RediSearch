/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::node::Node;

/// Consume a [`TrieMap`](crate::TrieMap) instance to iterate over its values, in lexicographical order.
///
/// It only yields the values attached to the nodes, without reconstructing
/// the corresponding keys.
///
/// It can be instantiated by calling [`TrieMap::into_values`](crate::TrieMap::into_values).
pub struct IntoValues<Data> {
    stack: Vec<Node<Data>>,
}

impl<Data> IntoValues<Data> {
    /// Create a new [`IntoValues`] iterator.
    pub(crate) fn new(root: Option<Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
        }
    }
}

impl<Data> Iterator for IntoValues<Data> {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            if let Some(data) = node.into_raw_parts_reversed(&mut self.stack) {
                return Some(data);
            }
        }
        None
    }
}
