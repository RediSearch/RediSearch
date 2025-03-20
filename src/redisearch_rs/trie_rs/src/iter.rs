use std::{ffi::c_char, fmt};

use crate::trie::Node;

/// Iterates over the entries of a [`TrieMap`] in lexicographical order
/// of the keys.
pub struct Iter<'tm, Data> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<(&'tm Node<Data>, bool)>,
    /// Labels of the parent nodes, used to reconstruct the key.
    prefixes: Vec<&'tm [c_char]>,
}

impl<'tm, Data> Iter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`].
    pub(crate) fn new(root: Option<&'tm Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().map(|node| (node, false)).collect(),
            prefixes: Vec::new(),
        }
    }
}

impl<'tm, Data: fmt::Debug> Iterator for Iter<'tm, Data> {
    type Item = (Vec<c_char>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (node, was_visited) = self.stack.pop()?;

        if !was_visited {
            let data = node.data.as_ref();
            self.stack.push((node, true));

            for child in node.children.0.iter().rev() {
                self.stack.push((&child.node, false));
            }

            self.prefixes.push(&node.label);
            if let Some(data) = data {
                // Combine the labels of the parent nodes and the current node,
                // thereby reconstructing the key.
                let label = self
                    .prefixes
                    .iter()
                    .flat_map(|p| p.iter())
                    .copied()
                    .collect();
                return Some((label, data));
            }
        } else {
            self.prefixes.pop();
        }
        self.next()
    }
}
