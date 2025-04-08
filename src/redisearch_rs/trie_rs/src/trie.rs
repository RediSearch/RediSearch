use crate::{node::Node, utils::strip_prefix};
use std::{ffi::c_char, fmt};

#[derive(Clone)]
/// A trie data structure that maps keys of type `&[c_char]` to values.
pub struct TrieMap<Data> {
    /// The root node of the trie.
    root: Option<Node<Data>>,
}

impl<Data> Default for TrieMap<Data> {
    fn default() -> Self {
        Self { root: None }
    }
}

impl<Data> TrieMap<Data> {
    /// Create a new (empty) [`TrieMap`].
    ///
    /// # Allocations
    ///
    /// No allocation is performed on creation.
    /// Memory is allocated only when the first insertion occurs.
    pub fn new() -> Self {
        Default::default()
    }

    /// Insert a key-value pair into the trie.
    ///
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[c_char], data: Data) -> Option<Data> {
        let mut old_data = None;
        self.insert_with(key, |curr_data| {
            old_data = curr_data;
            data
        });
        old_data
    }

    /// Remove an entry from the trie.
    ///
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, key: &[c_char]) -> Option<Data> {
        // If there's no root, there's nothing to remove.
        let root = self.root.as_mut()?;

        // The key is not in the trie if the root's label is not a
        // prefix of the key.
        let suffix = strip_prefix(key, root.label())?;

        // If the root turns out to be the node that needs removal,
        // we check whether it has any children. If it doesn't, we can
        // simply remove the root node. If it does, we remove the root's
        // data and attempt to merge the children.
        if suffix.is_empty() {
            if root.n_children() == 0 {
                self.root.take().and_then(|mut n| n.data_mut().take())
            } else {
                let data = root.data_mut().take();
                root.merge_child_if_possible();
                data
            }
        } else {
            // The node we need to remove is deeper in the trie.
            let data = root.remove_descendant(suffix);
            // After removing the child, we attempt to merge the child into the root.
            root.merge_child_if_possible();
            data
        }
    }

    /// Get a reference to the value associated with a key.
    ///
    /// Returns `None` if there is no entry for the key.
    pub fn find(&self, key: &[c_char]) -> Option<&Data> {
        self.root.as_ref().and_then(|n| n.find(key))
    }

    /// Insert an entry into the trie.
    ///
    /// The value is obtained by calling the provided callback function.
    /// If the key already exists, the existing value is passed to the callback,
    /// otherwise `f(None)` is inserted.
    pub fn insert_with<F>(&mut self, key: &[c_char], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        match &mut self.root {
            None => {
                let data = f(None);
                self.root = Some(Node::new_leaf(key, Some(data)));
            }
            Some(root) => root.insert_or_replace_with(key, f),
        }
    }

    /// Get the memory usage of the trie in bytes.
    /// Includes the memory usage of the root node on the stack.
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.root.as_ref().map(|r| r.mem_usage()).unwrap_or(0)
    }

    /// Compute the number of nodes in the trie.
    pub fn n_nodes(&self) -> usize {
        1 + self.root.as_ref().map_or(0, |r| r.n_nodes())
    }

    /// Iterate over the entries, in (lexicographical) key order.
    pub fn iter(&self) -> Iter<'_, Data> {
        Iter::new(self.root.as_ref())
    }
}

/// Iterates over the entries of a [`TrieMap`] in lexicographical order
/// of the keys.
///
/// Use [`TrieMap::iter`] to create an instance of this iterator.
pub struct Iter<'tm, Data> {
    /// Stack of nodes and whether they have been visited.
    stack: Vec<(&'tm Node<Data>, bool)>,
    /// Labels of the parent nodes, used to reconstruct the key.
    prefixes: Vec<&'tm [c_char]>,
}

impl<'tm, Data> Iter<'tm, Data> {
    /// Creates a new iterator over the entries of a [`TrieMap`].
    fn new(root: Option<&'tm Node<Data>>) -> Self {
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
            let data = node.data();
            self.stack.push((node, true));

            for child in node.children().iter().rev() {
                self.stack.push((child, false));
            }

            self.prefixes.push(node.label());
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

impl<Data: std::fmt::Debug> std::fmt::Debug for TrieMap<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(r) => r.fmt(f),
            None => f.write_str("(empty)"),
        }
    }
}
