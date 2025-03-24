use std::{ffi::c_char, fmt};

use crate::trie::Node;

/// Iterates over the entries of a [`crate::trie::TrieMap`] in lexicographical order
/// of the keys.
/// Can be instantiated by calling [`crate::trie::TrieMap::iter`].
pub struct Iter<'tm, Data> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<(&'tm Node<Data>, bool)>,
    /// Labels of the parent nodes, used to reconstruct the key.
    prefixes: Vec<&'tm [c_char]>,
}

impl<'tm, Data> Iter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`crate::trie::TrieMap`].
    pub(crate) fn new(root: Option<&'tm Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().map(|node| (node, false)).collect(),
            prefixes: Vec::new(),
        }
    }
}

impl<'tm, Data> Iterator for Iter<'tm, Data> {
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

/// Consuming iterator over the values of a trie in lexicographical
/// order of the corresponding keys. Removes an entry each iteration.
/// Can be instantiated by calling [`crate::trie::TrieMap::into_values`].
pub struct IntoValues<Data> {
    stack: Vec<Node<Data>>,
}

impl<Data> IntoValues<Data> {
    pub(crate) fn new(root: Option<Node<Data>>) -> Self {
        Self {
            stack: root.into_iter().collect(),
        }
    }
}

impl<Data> Iterator for IntoValues<Data> {
    type Item = Data;

    fn next(&mut self) -> Option<Self::Item> {
        let mut node = self.stack.pop()?;

        for child in node.children.0.into_iter().rev() {
            self.stack.push(child.node);
        }

        if let Some(data) = node.data.take() {
            return Some(data);
        }

        self.next()
    }
}

pub struct PatternIter<'tm, Data> {
    pattern: String,
    stack: Vec<(&'tm Node<Data>, bool)>,
}

impl<'tm, Data> Iterator for PatternIter<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct RangeIter<'tm, Data> {
    stack: Vec<(&'tm Node<Data>, bool)>,
}

impl<'tm, Data> Iterator for RangeIter<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    proptest::proptest! {
        #[test]
        /// Test whether the [`super::Iter`] iterator yields the same results as the BTreeMap entries iterator.
        fn test_iter(entries: std::collections::BTreeMap<Vec<std::ffi::c_char>,i32> ) {
            let mut trie = crate::trie::TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<std::ffi::c_char>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();
            let btree_entries: Vec<(Vec<std::ffi::c_char>, i32)> = entries.iter().map(|(k, v)| (k.clone(), *v)).collect();

            assert_eq!(trie_entries, btree_entries);
        }

        #[test]
        /// Test whether the [`super::IntoValues`] iterator yields the same results as the BTreeMap values iterator.
        fn test_values(entries: std::collections::BTreeMap<Vec<std::ffi::c_char>,i32> ) {
            let mut trie = crate::trie::TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }

            let trie_values: Vec<i32> = trie.into_values().collect();
            let btree_values: Vec<i32> = entries.into_values().collect();

            assert_eq!(trie_values, btree_values);
        }
    }
}
