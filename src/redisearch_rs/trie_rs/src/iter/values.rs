/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::node::Node;

/// Iterate over the values stored in a [`TrieMap`](crate::TrieMap), in lexicographical order.
///
/// It only yields the values attached to the nodes, without reconstructing
/// the corresponding keys.
///
/// It can be instantiated by calling [`TrieMap::values`](crate::TrieMap::values).
pub struct Values<'tm, Data> {
    stack: Vec<&'tm Node<Data>>,
}

impl<'tm, Data> Values<'tm, Data> {
    /// Create a new [`Values`] iterator.
    pub(crate) fn new(root: Option<&'tm Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
        }
    }
}

impl<'tm, Data> Iterator for Values<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;

        self.stack.extend(node.children().iter().rev());

        if let Some(data) = node.data() {
            return Some(data);
        }

        self.next()
    }
}
