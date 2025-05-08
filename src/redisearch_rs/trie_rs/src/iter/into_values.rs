/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


use crate::node::Node;

/// Iterate over mutable references to the values stored in a [`TrieMap`](crate::TrieMap), in lexicographical order.
///
/// It only yields the values attached to the nodes, without reconstructing
/// the corresponding keys.
///
/// It can be instantiated by calling [`TrieMap::into_values`](crate::TrieMap::into_values).
pub struct IntoValues<Data> {
    stack: Vec<Node<Data>>,
    buffer: Vec<Node<Data>>,
}

impl<Data> IntoValues<Data> {
    /// Create a new [`IntoValues`] iterator.
    pub(crate) fn new(root: Option<Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
            buffer: Vec::new(),
        }
    }
}

impl<Data> Iterator for IntoValues<Data> {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        let data = node.into_raw_parts(&mut self.buffer);
        // We need to push the children onto the stack in
        // reverse order, to ensure the overall lexicographic
        // ordering of the iterator.
        self.stack.extend(self.buffer.drain(..).rev());

        if let Some(data) = data {
            return Some(data);
        }

        self.next()
    }
}
