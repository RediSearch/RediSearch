use std::{cmp::Ordering, ffi::c_char, fmt};

use low_memory_thin_vec::{LowMemoryThinVec, low_memory_thin_vec};

use crate::iter::{IntoValues, Iter, LendingIter, Values};

#[derive(Default, Clone, PartialEq, Eq)]
/// A trie data structure that maps keys of type `&[c_char]` to values.
/// The node labels and children are stored in a [`LowMemoryThinVec`],
/// so as to minimize memory usage.
pub struct TrieMap<T> {
    /// The root node of the trie.
    root: Option<Node<T>>,
}

impl<T> TrieMap<T> {
    /// Create a new [`TrieMap`].
    pub fn new() -> Self {
        Self { root: None }
    }

    /// Insert a key-value pair into the trie.
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[c_char], data: T) -> Option<T> {
        let mut old_data = None;
        self.insert_with(key, |curr_data| {
            old_data = curr_data;
            data
        });
        old_data
    }

    /// Remove an entry from the trie.
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, key: &[c_char]) -> Option<T> {
        // If there's no root, there's nothing to remove.
        let root = self.root.as_mut()?;

        // If the root turns out to be the node that needs removal,
        // we check whether it has any children. If it doesn't, we can
        // simply remove the root node. If it does, we remove the root's
        // data and attempt to merge the children.
        if root.label == key {
            if root.children.is_empty() {
                return self.root.take().and_then(|n| n.data);
            } else {
                let data = root.data.take();
                root.merge_child_if_possible();
                return data;
            }
        }

        // If the root is not the node that needs removal, we delegate traverse
        // the trie given the suffix of the key.
        let suffix = key.strip_prefix(root.label.as_slice())?;
        let data = root.remove_child(suffix);
        // After removing the child, we attempt to merge the child into the root.
        root.merge_child_if_possible();
        data
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the no entry for the key is present.
    pub fn find(&self, key: &[c_char]) -> Option<&T> {
        self.root.as_ref().and_then(|n| n.find(key))
    }

    /// Insert an entry into the trie. The value is obtained by calling the
    /// provided callback function. If the key already exists, the existing
    /// value is passed to the callback, otherwise `None` is passed.
    pub fn insert_with<F>(&mut self, key: &[c_char], f: F)
    where
        F: FnOnce(Option<T>) -> T,
    {
        match &mut self.root {
            None => {
                let data = f(None);
                self.root = Some(Node {
                    children: ChildRefs::new(),
                    data: Some(data),
                    label: LowMemoryThinVec::from_slice(key),
                });
            }
            Some(root) => root.insert_or_replace_with(key, f),
        }
    }

    /// Get the memory usage of the trie in bytes.
    /// Includes the memory usage of the root node on the stack.
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.root.as_ref().map(|r| r.mem_usage()).unwrap_or(0)
    }

    pub fn num_nodes(&self) -> usize {
        1 + self.root.as_ref().map_or(0, |r| r.num_nodes())
    }

    /// Get an iterator over the map entries in order of keys.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter::new(self.root.as_ref(), [])
    }

    /// Get an iterator over the map entries with keys that start
    /// with the given prefix, in order of keys.
    pub fn iter_prefix(&self, prefix: &[c_char]) -> Iter<'_, T> {
        let buf = &mut vec![];
        let Some((root, prefix_init)) = self
            .root
            .as_ref()
            .and_then(|root| root.find_node_for_prefix(prefix, buf))
        else {
            return Iter::new(None, vec![]);
        };

        Iter::new(Some(root), prefix_init.to_vec())
    }

    /// Get a lending iterator over the map entries in order of keys.
    pub fn lending_iter(&self) -> LendingIter<'_, T> {
        Iter::new(self.root.as_ref(), []).into_lending_iter()
    }

    /// Get a lending iterator over the map entries with keys that start
    /// with the given prefix, in order of keys.
    pub fn lending_iter_prefix(&self, prefix: &[c_char]) -> LendingIter<'_, T> {
        self.iter_prefix(prefix).into_lending_iter()
    }

    /// Get an iterator over the map values in order of corresponding keys
    pub fn values(&self) -> Values<'_, T> {
        Values::new(self.root.as_ref())
    }

    /// Get an iterator over the map values of which the keys
    /// start with the given prefix, in order of keys.
    pub fn values_prefix(&self, prefix: &[c_char]) -> Values<'_, T> {
        let buf = &mut vec![];
        let Some((root, _)) = self
            .root
            .as_ref()
            .and_then(|root| root.find_node_for_prefix(prefix, buf))
        else {
            return Values::new(None);
        };

        Values::new(Some(root))
    }

    /// Get a consuming iterator over the values in the map
    /// in order of corresponding keys.
    pub fn into_values(self) -> IntoValues<T> {
        IntoValues::new(self.root)
    }
}

impl<T: fmt::Debug> fmt::Debug for TrieMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(f, "{:?}", root),
            None => f.write_str("(empty)"),
        }
    }
}

/// A trie data structure that maps labels comprised of [`c_char`] sequences to values.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Node<Data> {
    /// The children of this node.
    pub children: ChildRefs<Data>,
    /// Optional data attached to the key leading to this node.
    ///
    /// # Invariants
    ///
    /// - Data may not be `None` for leaf nodes.
    ///
    pub data: Option<Data>,
    /// The portion of the key attached to this node.
    ///
    /// # Invariants
    ///
    /// - `label` can only be empty for the root node.
    pub label: LowMemoryThinVec<c_char>,
}

impl<Data> Node<Data> {
    /// Inserts a new key-value pair into the trie.
    ///
    /// If the key already exists, the current value is passed to provided function,
    /// and replaced with the value returned by that function.
    fn insert_or_replace_with<F>(&mut self, key: &[c_char], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        if let Some(suffix) = key.strip_prefix(self.label.as_slice()) {
            let Some(first_byte) = suffix.first() else {
                // Suffix is empty, so the key and the node label are equal.
                // Replace the data attached to the current node
                // with the new data.
                let current = self.data.take();
                let new = f(current);
                self.data.replace(new);
                return;
            };

            // Suffix is not empty, therefore the insertion needs to happen
            // in a descendant of the current node.
            // `find_or_insert` will determine if it becomes a new direct
            // child or if we already have a child with the same first byte.
            let child = self.children.find_or_insert(*first_byte, || Node {
                children: ChildRefs::new(),
                // Data will be set when recursing, as we'll end up in the
                // equality case handled just above, because this child's label
                // is equal to the suffix.
                data: None,
                label: LowMemoryThinVec::from_slice(suffix),
            });
            // In both cases, we recurse.
            // If we added a new direct child, we'll end up in the equality
            // case that we handled just above.
            // Otherwise the behaviour will depend on the label of the relevant
            // pre-existing child node.
            return child.insert_or_replace_with(suffix, f);
        }

        if self.label.as_slice().starts_with(key) {
            // The key we want to insert is a strict prefix of the current node's label.
            // Therefore we need to insert a new _parent_ node.
            //
            // .split(index)
            // .split(prefix, suffix)
            //
            // # Case 1: No children for the current node
            //
            // Add `bike` with data `B` to a trie with `biker`, where `biker` has data `A`.
            // ```text
            // biker (A)  ->   bike (B)
            //                /
            //               r (A)
            // ```

            // # Case 2: Current node has children
            //
            // Add `b` to a trie with `bi` and `bike`.
            // `b` has data `C`, `bi` has data `A`, `bike` has data `B`.
            //
            // ```text
            // bi (A)   ->    b (C)
            //   \             \
            //    ke (B)        i (A)
            //                   \
            //                    ke (B)
            // ```
            self.split(key.len());
            self.data = Some(f(None));
            return;
        }

        // In this case, only part of the key matches the current node's label.
        // Add `bis` (D) to a trie with `bike` (A), `biker` (B) and `bikes` (C).
        //
        // ```text
        //     bike (A)   ->      bi (-)
        //     /  \             /       \
        // r (B)   s (C)      ke (A)     s (D)
        //                   /     \
        //                 r (B)   s (C)
        // ```
        let Some((equal_up_to, _)) = self
            .label
            .iter()
            .zip(key.iter())
            .enumerate()
            .find(|(_, (c1, c2))| c1 != c2)
        else {
            unreachable!("We know that neither is a prefix of the other at this point")
        };

        if equal_up_to == 0 {
            // The node's label and the key don't share a common prefix.
            // This can only happen if the current node is the root node of the trie.
            //
            // Create a new root node with an empty label,
            // insert the old root as a child of the new root,
            // and add a new child to the empty root.
            let new_child = Node {
                children: ChildRefs::default(),
                data: Some(f(None)),
                label: LowMemoryThinVec::from_slice(key),
            };
            let children = ChildRefs(low_memory_thin_vec![ChildRef {
                first_byte: new_child.label[0],
                node: new_child
            }]);
            let new_root = Node {
                children,
                data: None,
                label: LowMemoryThinVec::new(),
            };
            let old_root = std::mem::replace(self, new_root);
            self.children
                .find_or_insert(old_root.label[0], move || old_root);
            return;
        }

        // In this case, the node and the key do share a common prefix.
        // That shared prefix is `&key[..equal_up_to].
        //
        // Create a new node that uses the shared prefix as its label.
        // The prefix is then stripped from both the current label and the
        // new key; the resulting suffixes are used as labels for the new child nodes.

        // Note: we will be swapping the old parent with the new parent.
        // Before the swap, `self` points to the old parent. After the swap,
        // `self` points to the new parent.

        // The label of the old parent, stripped of the shared prefix.
        // We'll assign the old parent this label once we swap out the old parent with the new one.
        let old_parent_suffix = LowMemoryThinVec::from_slice(&self.label[equal_up_to..]);

        // The label of the new child node that is to be inserted, which is the key stripped of the shared prefix.
        let newly_inserted_child_suffix = LowMemoryThinVec::from_slice(&key[equal_up_to..]);

        // The label of the new parent node, which is the shared prefix.
        let new_parent_label = {
            let mut l = std::mem::take(&mut self.label);
            l.truncate(equal_up_to);
            l
        };

        // The new parent node, which holds the shared prefix as label,
        // and will be swapped with the old parent.
        let new_parent = Node {
            children: ChildRefs::default(),
            data: None,
            label: new_parent_label,
        };

        // Swap the old parent with the new parent.
        // After this statement, `self` refers to the new parent node.
        let mut old_parent = std::mem::replace(self, new_parent);

        // The new child node, which holds the key suffix as label,
        // and will be inserted as a child of the new parent.
        let newly_inserted_child = Node {
            children: ChildRefs::default(),
            data: Some(f(None)),
            label: newly_inserted_child_suffix,
        };
        // Set the old parent's label to the old parent's suffix,
        // i.e. its original label stripped of the shared prefix.
        old_parent.label = old_parent_suffix;

        // Create ChildRefs for the old parent and the new child,
        // so that they can be inserted into the new parent's children.
        let old_parent_first_byte = old_parent.label[0];
        let old_parent_ref = ChildRef {
            first_byte: old_parent_first_byte,
            node: old_parent,
        };
        let new_child_first_byte = newly_inserted_child.label[0];
        let new_child_ref = ChildRef {
            first_byte: new_child_first_byte,
            node: newly_inserted_child,
        };
        // Build the children vector for the new parent,
        // which will hold the old parentt as well as the
        // newly inserted child.
        let children = match old_parent_first_byte.cmp(&new_child_first_byte) {
            Ordering::Less => {
                low_memory_thin_vec![old_parent_ref, new_child_ref]
            }
            Ordering::Greater => {
                low_memory_thin_vec![new_child_ref, old_parent_ref]
            }
            Ordering::Equal => {
                unreachable!(
                    "The shared prefix has already been stripped,\
                    therefore the first byte of the suffixes must be different."
                )
            }
        };

        // Assign the children vector to the new parent.
        self.children = ChildRefs(children);
    }

    /// Remove a child from the node.
    /// Returns the data associated with the key if it was present.
    fn remove_child(&mut self, key: &[c_char]) -> Option<Data> {
        // Find the index of child whose label starts with the first byte of the key,
        // as well as the child itself.
        // If we find none, there's nothing to remove.
        // Note that `key.first()?` will cause this function to return None if the key is empty.
        let child_index = self.children.index_of(*key.first()?)?;
        let child = &mut self.children.0[child_index].node;

        // If the child's label is equal to the key, we remove the child.
        if child.label == key {
            let data = child.data.take();

            let child_is_leaf = child.children.is_empty();

            if child_is_leaf {
                // If the child is a leaf, we remove the child node itself.
                self.children.remove(child_index);
            } else {
                // If there's a single grandchild,
                // we merge the grandchild into the child.
                child.merge_child_if_possible();
            }

            return data;
        }

        // If the child's label is prefixed by the key, we recurse into the child.
        // If not, there's nothing to remove.
        let suffix = key.strip_prefix(child.label.as_slice())?;
        debug_assert!(!suffix.is_empty());

        let data = child.remove_child(suffix);
        child.merge_child_if_possible();
        data
    }

    /// If `self` has exactly one child, and `self` doesn't hold
    /// any data, merge child into `self`, by moving the child's data and
    /// childreninto `self`. Depending on the spare capacity of `self`s label
    /// and that of the child, we either extend `self`s label with the child's label,
    /// or vice versa.
    fn merge_child_if_possible(&mut self) {
        if self.data.is_some() {
            return;
        }
        if self.children.is_single() {
            let child = self.children.remove(0);

            let self_label_fits_child_label =
                self.label.capacity() - self.label.len() >= child.label.len();

            let child_label_fits_self_label =
                || child.label.capacity() - child.label.len() >= self.label.len();

            if self_label_fits_child_label || !child_label_fits_self_label() {
                // If self's label can fit the child's label, or none of them can
                // fit the other's label, we merge the child into self.
                self.label.extend_from_slice(&child.label);
                self.children = child.children;
                self.data = child.data;
            } else {
                // If the child's label can fit self's label,
                // we swap self and child, and prepend the child's label to self's label.
                let old_self = std::mem::replace(self, child);
                self.label.prepend_with_slice(&old_self.label);
            }
        }
    }

    /// Split the label of the current node at the given index.
    /// Then assign the first chunk of the label to the current node,
    /// while the suffix is assigned to a newly-created child node.
    ///
    /// If the current node had any data attached to it, we re-assign
    /// data to the new child node.
    ///
    /// ```text
    /// // Splitting at index 4
    /// biker (A)  ->   bike (-)
    ///                /
    ///               r (A)
    /// ```
    ///
    /// If the current node has any children, they become children
    /// of the new child node.
    ///
    /// ```text
    /// // Splitting at index 1
    /// bi (A)   ->    b (-)
    ///   \             \
    ///    ke (B)        i (A)
    ///                   \
    ///                    ke (B)
    /// ```
    fn split(&mut self, index: usize) {
        let suffix = LowMemoryThinVec::from_slice(&self.label.as_slice()[index..]);
        let suffix_first_byte = suffix[0];

        let new_node = Node {
            children: std::mem::take(&mut self.children),
            data: self.data.take(),
            label: suffix,
        };

        let parent_children = {
            let child_ref = ChildRef {
                first_byte: suffix_first_byte,
                node: new_node,
            };
            ChildRefs(low_memory_thin_vec![child_ref])
        };

        self.label.truncate(index);
        self.children = parent_children;
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the key is not present.
    fn find(&self, key: &[c_char]) -> Option<&Data> {
        self.find_node(key)?.data.as_ref()
    }

    /// Get a reference to the node associated with a key.
    /// Returns `None` if the key is not present.
    fn find_node(&self, key: &[c_char]) -> Option<&Node<Data>> {
        let suffix = key.strip_prefix(self.label.as_slice())?;
        let Some(first_byte) = suffix.first() else {
            // The suffix is empty, so the key and the label are equal.
            return Some(self);
        };
        self.children.find(*first_byte)?.find_node(suffix)
    }

    /// Get a reference to the subtree associated with a key prefix.
    /// Returns `None` if the key prefix is not present.
    fn find_node_for_prefix<'k>(
        &self,
        key: &[c_char],
        key_prefix: &'k mut Vec<c_char>,
    ) -> Option<(&Node<Data>, &'k [c_char])> {
        if self.label.starts_with(key) {
            return Some((self, key_prefix.as_slice()));
        }

        let suffix = key.strip_prefix(self.label.as_slice())?;
        let prefix_len = key.len() - suffix.len();
        key_prefix.extend_from_slice(&key[..prefix_len]);
        self.children
            .find(*suffix.first()?)?
            .find_node_for_prefix(suffix, key_prefix)
    }

    fn mem_usage(&self) -> usize {
        self.label.mem_usage()
            + self.children.0.mem_usage()
            + self
                .children
                .0
                .iter()
                .map(|c| c.node.mem_usage())
                .sum::<usize>()
    }

    pub fn num_nodes(&self) -> usize {
        self.children.0.len()
            + self
                .children
                .0
                .iter()
                .map(|c| c.node.num_nodes())
                .sum::<usize>()
    }

    fn label_to_string_lossy(&self) -> String {
        let slice = self.label.iter().map(|&c| c as u8).collect::<Vec<_>>();
        String::from_utf8_lossy(&slice).into_owned()
    }
}

impl<Data: fmt::Debug> fmt::Debug for Node<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut stack = vec![(self, 0, 0)];

        while let Some((next, white_indentation, line_indentation)) = stack.pop() {
            let label_repr = next.label_to_string_lossy();
            let data_repr = next
                .data
                .as_ref()
                .map_or("(-)".to_string(), |data| format!("({:?})", data));

            let prefix = if white_indentation == 0 && line_indentation == 0 {
                "".to_string()
            } else {
                let whitespace = " ".repeat(white_indentation);
                let line = "–".repeat(line_indentation);
                format!("{whitespace}↳{line}")
            };

            writeln!(f, "{prefix}\"{label_repr}\" {data_repr}")?;

            for child in next.children.0.iter().rev() {
                let ChildRef { node, .. } = &child;
                let new_line_indentation = 4;
                let white_indentation = white_indentation + line_indentation + 2;
                stack.push((node, white_indentation, new_line_indentation));
            }
        }
        Ok(())
    }
}

/// The children of a [`Node`] in a [`TrieMap`].
/// Basically a mapping of [`c_char`] to Node<Data>.
///
/// # Invariants
///
/// - The vector is sorted by the child's first byte,
///   to allow fast binary searches when traversing the trie.
/// - There are no children with the same first byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildRefs<Data>(pub LowMemoryThinVec<ChildRef<Data>>);

impl<Data> Default for ChildRefs<Data> {
    fn default() -> Self {
        Self(LowMemoryThinVec::new())
    }
}

impl<Data> ChildRefs<Data> {
    /// Create a new empty vector of child references.
    fn new() -> Self {
        Self::default()
    }

    /// Find or insert a child with the given first byte.
    /// Returns a mutable reference to the child node.
    fn find_or_insert<CreateNode>(
        &mut self,
        first_byte: c_char,
        create_node: CreateNode,
    ) -> &mut Node<Data>
    where
        CreateNode: FnOnce() -> Node<Data>,
    {
        let index = match self
            .0
            .binary_search_by_key(&first_byte, |child| child.first_byte)
        {
            Ok(match_index) => match_index,
            Err(insertion_index) => {
                self.0.insert(
                    insertion_index,
                    ChildRef {
                        first_byte,
                        node: create_node(),
                    },
                );
                insertion_index
            }
        };
        &mut self.0[index].node
    }

    /// Find a child with the given first byte.
    fn find(&self, first_byte: c_char) -> Option<&Node<Data>> {
        self.index_of(first_byte).map(|index| &self.0[index].node)
    }

    /// Find the index of a child with the given first byte.
    fn index_of(&self, first_byte: c_char) -> Option<usize> {
        self.0
            .binary_search_by_key(&first_byte, |child| child.first_byte)
            .ok()
    }

    /// Remove a child at the given index.
    fn remove(&mut self, index: usize) -> Node<Data> {
        self.0.remove(index).node
    }

    /// Check whether the children vector has only a single child.
    fn is_single(&self) -> bool {
        self.0.len() == 1
    }

    /// Check whether the children vector is empty.
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The reference to the child node held inside its parent node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildRef<Data> {
    /// The first byte of the value that this child node holds.
    pub first_byte: c_char,
    /// The actual child node.
    pub node: Node<Data>,
}

#[cfg(test)]
mod test {

    use super::*;

    trait ToCCharArray<const N: usize> {
        /// Convenience method to convert a byte array to a C-compatible character array.
        fn c_chars(self) -> [c_char; N];
    }

    impl<const N: usize> ToCCharArray<N> for [u8; N] {
        fn c_chars(self) -> [c_char; N] {
            self.map(|b| b as c_char)
        }
    }

    /// Forwards to `insta::assert_debug_snapshot!`,
    /// but is disabled in Miri, as snapshot testing
    /// involves file I/O, which is not supported in Miri.
    macro_rules! assert_debug_snapshot {
        ($($arg:tt)*) => {
            #[cfg(not(miri))]
            insta::assert_debug_snapshot!($($arg)*);
        };
    }

    #[test]
    fn test_trie() {
        let mut trie = TrieMap::new();
        trie.insert(&b"bike".c_chars(), 0);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"cool".c_chars()), None);
        assert_debug_snapshot!(trie, @r#""bike" (0)"#);

        trie.insert(&b"biker".c_chars(), 1);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"cool".c_chars()), None);
        assert_debug_snapshot!(trie, @r#"
        "bike" (0)
          ↳––––"r" (1)
        "#);

        trie.insert(&b"bis".c_chars(), 2);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), None);
        assert_debug_snapshot!(trie, @r#"
        "bi" (-)
          ↳––––"ke" (0)
                ↳––––"r" (1)
          ↳––––"s" (2)
        "#);

        trie.insert(&b"cool".c_chars(), 3);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), Some(&3));
        assert_debug_snapshot!(trie, @r#"
        "" (-)
          ↳––––"bi" (-)
                ↳––––"ke" (0)
                      ↳––––"r" (1)
                ↳––––"s" (2)
          ↳––––"cool" (3)
        "#);

        trie.insert(&b"bi".c_chars(), 4);
        assert_eq!(trie.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(trie.find(&b"biker".c_chars()), Some(&1));
        assert_eq!(trie.find(&b"bis".c_chars()), Some(&2));
        assert_eq!(trie.find(&b"cool".c_chars()), Some(&3));
        assert_eq!(trie.find(&b"bi".c_chars()), Some(&4));
        assert_debug_snapshot!(trie, @r#"
        "" (-)
          ↳––––"bi" (4)
                ↳––––"ke" (0)
                      ↳––––"r" (1)
                ↳––––"s" (2)
          ↳––––"cool" (3)
        "#);

        assert_eq!(trie.remove(&b"cool".c_chars()), Some(3));
        assert_debug_snapshot!(trie, @r#"
        "bi" (4)
          ↳––––"ke" (0)
                ↳––––"r" (1)
          ↳––––"s" (2)
        "#);
        assert_eq!(trie.remove(&b"cool".c_chars()), None);

        assert_eq!(trie.remove(&b"bike".c_chars()), Some(0));
        assert_debug_snapshot!(trie, @r#"
        "bi" (4)
          ↳––––"ker" (1)
          ↳––––"s" (2)
        "#);
        assert_eq!(trie.remove(&b"bike".c_chars()), None);

        assert_eq!(trie.remove(&b"biker".c_chars()), Some(1));
        assert_debug_snapshot!(trie, @r#"
        "bi" (4)
          ↳––––"s" (2)
        "#);
        assert_eq!(trie.remove(&b"biker".c_chars()), None);

        assert_eq!(trie.remove(&b"bi".c_chars()), Some(4));
        assert_debug_snapshot!(trie, @r#"
        "bis" (2)
        "#);
        assert_eq!(trie.remove(&b"bi".c_chars()), None);
    }

    #[test]
    /// Tests whether the trie merges nodes
    /// correctly upon removal of entries.
    fn test_trie_merge() {
        let mut trie = TrieMap::new();
        trie.insert(&b"a".c_chars(), 0);
        assert_debug_snapshot!(trie, @r#""a" (0)"#);

        trie.insert(&b"ab".c_chars(), 1);
        trie.insert(&b"abcd".c_chars(), 2);
        assert_debug_snapshot!(trie, @r#"
        "a" (0)
          ↳––––"b" (1)
                ↳––––"cd" (2)
        "#);

        assert_eq!(trie.remove(&b"ab".c_chars()), Some(1));
        assert_debug_snapshot!(trie, @r#"
        "a" (0)
          ↳––––"bcd" (2)
        "#);

        trie.insert(&b"abce".c_chars(), 3);
        assert_debug_snapshot!(trie, @r#"
        "a" (0)
          ↳––––"bc" (-)
                ↳––––"d" (2)
                ↳––––"e" (3)
        "#);

        assert_eq!(trie.remove(&b"abcd".c_chars()), Some(2));
        assert_debug_snapshot!(trie, @r#"
        "a" (0)
          ↳––––"bce" (3)
        "#);
    }

    #[test]
    fn test_mem_usage() {
        // Allow identity operations for readability
        #![allow(clippy::identity_op)]
        use std::mem;

        // Get the memory usage of a `ThinVec<T>` on the heap with a given capacity
        fn thin_vec_mem_usage_for_cap<T>(cap: usize) -> usize {
            LowMemoryThinVec::<T>::with_capacity(cap).mem_usage()
        }

        const NODE_SIZE: usize = mem::size_of::<Node<i32>>();
        const TRIEMAP_SIZE: usize = mem::size_of::<TrieMap<i32>>();

        assert_eq!(
            NODE_SIZE, TRIEMAP_SIZE,
            "The size of an empty trie should equal the size of a single node"
        );

        let mut trie: TrieMap<i32> = TrieMap::new();
        assert_eq!(
            trie.mem_usage(),
            1 * NODE_SIZE, // Unoccupied space for 1 node
            "The computed size of an empty trie should equal the size of a single node"
        );

        let hello = b"hello".c_chars();
        let world = b"world".c_chars();
        let he = b"he".c_chars();
        trie.insert(&hello, 1);

        assert_eq!(
            trie.mem_usage(),
            1 * NODE_SIZE // Root node
                + 10 // Label b"hello":  header (4) + padding(0) + capacity (5 * 1) + padding (1)
                + 0, // 0 children
            "The size of the trie should equal the size of 1 `Node<i32>`, 0 `ChildRef<i32>`s,\
                and 1 label with a capacity of 5 elements and aligned to 2 bytes"
        );

        trie.insert(&world, 2);

        assert_eq!(
            trie.mem_usage(),
            NODE_SIZE // Root node
                + 10 // Label b"hello": header (4) + padding(0) + capacity (5 * 1) + padding (1)
                + 10 // Label b"world": header (4) + padding(0) + capacity (5 * 1) + padding (1)
                + thin_vec_mem_usage_for_cap::<ChildRef<i32>>(2), // 2 children: header (4) + padding(4) + capacity (2 * 32) + padding (0)
            "The size of the trie should equal the size of 1 `Node<i32>`, 2 `ChildRef<i32>`s,\
            and 2 labels with a capacity of 5 elements each and aligned to 2 bytes"
        );

        trie.insert(&he, 3);

        assert_eq!(
            trie.mem_usage(),
            NODE_SIZE // Root node
            + thin_vec_mem_usage_for_cap::<ChildRef<i32>>(2) // 2 children
            // Node with label b"he" was split, and therefore has and additional capacity of 3 elements
            + 10 // Label b"he" + XXX: header (4) + padding(0) + capacity(5 * 1) + padding(1)
            + 10 // Label b"world": header (4) + padding(0) + capacity(5 * 1) + padding(1)
            + thin_vec_mem_usage_for_cap::<ChildRef<i32>>(1) // 1 grandchild
            + 8, // Label b"llo": header (4) + padding(0) + capacity(3 * 1) + padding(1)
            "The size of the trie should equal the size of 1 `Node<i32>`, 2 `ChildRef<i32>`s for the
            root's children, 1 `ChildRef<i32>` for the root's grandchild, \
            and 3 labels with a capacity of 5 elements each and aligned to 2 bytes"
        );

        trie.remove(&he);

        assert_eq!(
            trie.mem_usage(),
            NODE_SIZE // root node
            + thin_vec_mem_usage_for_cap::<ChildRef<i32>>(2) // 2 children
            + 10 // Label b"hello": header (4) + padding(0) + capacity (5 * 1) + padding (1)
            + 10, // Label b"world": header (4) + padding(0) + capacity(5 * 1) + padding(1)
            "The size of the trie should equal the size of 1 `Node<i32>`, 1 `ChildRef<i32>` for the
            root's child, and 1 label with a capacity of 5 elements and aligned to 2 bytes"
        );
    }

    #[derive(proptest_derive::Arbitrary, Debug)]
    #[cfg(not(miri))]
    /// Enum representing operations that can be performed on a trie.
    /// Used for in the proptest below.
    enum TrieOperation<Data> {
        Insert(Vec<c_char>, Data),
        Remove(Vec<c_char>),
    }

    // Disable the proptest when testing with Miri,
    // as proptest accesses the file system, which is not supported by Miri
    #[cfg(not(miri))]
    proptest::proptest! {
        #[test]
        /// Check whether the trie behaves like a [`std::collections::BTreeMap<Vec<c_char>, _>`]
        /// when inserting and removing elements. We can use the `proptest` crate to generate random
        /// operations and check that the trie behaves identically to the `BTreeMap`.
        fn sanity_check(ops: Vec<TrieOperation<i32>>) {
            let mut triemap = TrieMap::new();
            let mut hashmap = std::collections::BTreeMap::new();

            for op in ops {
                match op {
                    TrieOperation::Insert(k, v) => {
                        triemap.insert(&k, v);
                        hashmap.insert(k, v);
                    }
                    TrieOperation::Remove(k) => {
                        triemap.remove(&k);
                        hashmap.remove(&k);
                    },
                }
            }

            let trie_entries = triemap.iter().collect::<Vec<_>>();
            let hash_entries = hashmap.iter().map(|(label, data)| (label.clone(), data)).collect::<Vec<_>>();
            assert_eq!(trie_entries, hash_entries);
        }
    }
}
