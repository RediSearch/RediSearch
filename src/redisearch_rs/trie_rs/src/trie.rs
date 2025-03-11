use lm_thin_vec::{LMThinVec, lm_thin_vec};

#[derive(Clone, PartialEq, Eq)]
pub struct TrieMap<T> {
    root: Option<Node<T>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for TrieMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.root {
            Some(root) => write!(f, "{:?}", root),
            None => f.write_str("(empty)"),
        }
    }
}

/// The children of a node in a trie.
///
/// # Invariants
///
/// - The vector is sorted by the child's first byte,
///   to allow fast binary searches when traversing the trie.
/// - There are no children with the same first byte.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildRefs<Data>(LMThinVec<ChildRef<Data>>);

impl<Data> Default for ChildRefs<Data> {
    fn default() -> Self {
        Self(LMThinVec::new())
    }
}

impl<Data> ChildRefs<Data> {
    /// Create a new empty vector of child references.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn find_or_insert<CreateNode>(
        &mut self,
        first_byte: u8,
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
}

/// A trie data structure that maps strings to values.
#[derive(Clone, PartialEq, Eq)]
struct Node<Data> {
    /// Basically a mapping of Option<u8> to Node<Data>,
    children: ChildRefs<Data>,
    /// Optional data attached to the key leading to this node.
    ///
    /// It may be `Some` for any node, even if it's not a leaf node.
    data: Option<Data>,
    /// The portion of the key attached to this node.
    ///
    /// # Invariants
    ///
    /// - `label` can only be empty for the root node.
    label: LMThinVec<u8>,
}

impl<Data: std::fmt::Debug> std::fmt::Debug for Node<Data> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stack = vec![(self, 0, 0)];
        while let Some((next, white_indentation, line_indentation)) = stack.pop() {
            let label_repr = std::string::String::from_utf8_lossy(next.label.as_slice());
            let data_repr = next
                .data
                .as_ref()
                .map_or("(-)".to_string(), |data| format!("({:?})", data));
            let prefix = if white_indentation == 0 && line_indentation == 0 {
                "".to_string()
            } else {
                let whitespace = " ".repeat(white_indentation);
                let line = "-".repeat(line_indentation);
                format!("{whitespace}|{line}")
            };
            writeln!(f, "{prefix}\"{label_repr}\" {data_repr}")?;
            for child in next.children.0.iter().rev() {
                let ChildRef { node, .. } = &child;
                let new_line_indentation = label_repr.len() + 2 + data_repr.len();
                let white_indentation = white_indentation + line_indentation + 1;
                stack.push((node, white_indentation, new_line_indentation));
            }
        }
        Ok(())
    }
}

/// The reference to the child node held inside its parent node.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildRef<Data> {
    /// The first byte of the value that this child node holds.
    first_byte: u8,
    /// The reference to the actual child node.
    node: Node<Data>,
}

impl<T> Default for TrieMap<T> {
    fn default() -> Self {
        Self { root: None }
    }
}

impl<T> TrieMap<T> {
    /// Create a new trie.
    pub fn new() -> Self {
        Self { root: None }
    }

    /// Insert a key-value pair into the trie.
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[u8], data: T) -> Option<T> {
        match &mut self.root {
            None => {
                // If there's no root yet, simply create a new node and make it the root
                // TODO: Introduce an optimized `from_slice` method on `LMThinVec`
                let mut label = LMThinVec::with_capacity(key.len());
                label.extend_from_slice(key);
                self.root = Some(Node {
                    children: ChildRefs::new(),
                    data: Some(data),
                    label,
                });
                None
            }
            Some(root) => root.insert(key, data),
        }
    }

    /// Remove a key from the trie.
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, key: &[u8]) -> Option<T> {
        todo!()
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the key is not present.
    pub fn get(&self, key: &[u8]) -> Option<&T> {
        todo!()
    }

    /// Get a mutable reference to the value associated with a key.
    /// Returns `None` if the key is not present.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        todo!()
    }
}

impl<Data> Node<Data> {
    pub fn insert(&mut self, key: &[u8], data: Data) -> Option<Data> {
        // if node is Some
        // - [**It can only happens with root node**]
        //   If current node and new node don't share a prefix,
        //   replace current node with a new one with an empty label,
        //   make new node and current node children of the new node
        //   taking into acount the alphabetical order of the key
        // - if current node and new node _do_ share a prefix
        //   split the node at the end of the shared prefix
        //   1. create a new node for the suffix of the current node
        //   2. create a new node for the suffix of the new node
        //   3. create a new node for the prefix, assigning the
        //      previously created nodes as children
        //   4. swap out the old current node with the prefix node
        if let Some(suffix) = key.strip_prefix(self.label.as_slice()) {
            let Some(first_byte) = suffix.first() else {
                // They are equal, so replace the data attached to the current node
                // with the new data.
                return std::mem::replace(&mut self.data, Some(data));
            };
            let child = self.children.find_or_insert(*first_byte, || {
                let mut label = LMThinVec::with_capacity(suffix.len());
                label.extend_from_slice(suffix);
                Node {
                    children: ChildRefs::new(),
                    data: None,
                    label,
                }
            });
            return child.insert(suffix, data);
        }

        if key.is_prefix_of(self.label.as_slice()) {
            // in this case, the current node's label is prefixed with the key
            // so we split the current node up until the point they're the same
            //
            // .split(index)
            // .split(prefix, suffix)
            //
            // # Case 1: No children for the current node
            //
            // Add `bike` with data `B` to a trie with `biker`, where `biker` has data `A`.
            //
            //               PREFIX     biker (A)  ->   bike (B)
            //              /                          /
            // old_root_suffix                        r (A)
            //
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
            self.data = Some(data);
            return None;
        }

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
            let new_child = Node {
                children: ChildRefs::default(),
                data: Some(data),
                label: {
                    let mut label = LMThinVec::with_capacity(key.len());
                    label.extend_from_slice(key);
                    label
                },
            };
            let children = ChildRefs(lm_thin_vec![ChildRef {
                first_byte: new_child.label[0],
                node: new_child
            }]);
            let new_root = Node {
                children,
                data: None,
                label: LMThinVec::new(),
            };
            let old_root = std::mem::replace(self, new_root);
            self.children
                .find_or_insert(old_root.label[0], move || old_root);
            None
        } else {
            let old_root_suffix = {
                let old_root_suffix = &self.label[equal_up_to..];
                let mut label = LMThinVec::with_capacity(old_root_suffix.len());
                label.extend_from_slice(old_root_suffix);
                label
            };
            let child_suffix = {
                let k = &key[equal_up_to..];
                let mut label = LMThinVec::with_capacity(k.len());
                label.extend_from_slice(k);
                label
            };
            let shared_prefix = {
                let mut l = std::mem::take(&mut self.label);
                l.truncate(equal_up_to);
                l
            };

            let new_child = Node {
                children: ChildRefs::default(),
                data: Some(data),
                label: child_suffix,
            };

            let new_root = Node {
                children: ChildRefs(lm_thin_vec![ChildRef {
                    first_byte: new_child.label[0],
                    node: new_child
                }]),
                data: None,
                label: shared_prefix,
            };

            let mut old_root = std::mem::replace(self, new_root);
            old_root.label = old_root_suffix;

            self.children
                .find_or_insert(old_root.label[0], move || old_root);
            None
        }
    }

    fn split(&mut self, index: usize) {
        let suffix = {
            let mut v = LMThinVec::with_capacity(index);
            v.extend_from_slice(&self.label.as_slice()[index..]);
            v
        };
        let suffix_beginning = suffix[0];

        let new_node = Node {
            children: std::mem::take(&mut self.children),
            data: self.data.take(),
            label: suffix,
        };
        let parent_children = {
            let child_ref = ChildRef {
                first_byte: suffix_beginning,
                node: new_node,
            };
            ChildRefs(lm_thin_vec![child_ref])
        };

        self.label.truncate(index);
        self.children = parent_children;
    }
}

trait LabelExt {
    fn is_prefix_of(&self, other: &[u8]) -> bool;
}

impl LabelExt for &[u8] {
    fn is_prefix_of(&self, other: &[u8]) -> bool {
        other.starts_with(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_trie() {
        let mut trie = TrieMap::new();
        trie.insert(b"bike", 0);
        insta::assert_debug_snapshot!(trie, @r#""bike" (0)"#);

        trie.insert(b"biker", 1);
        insta::assert_debug_snapshot!(trie, @r#"
        "bike" (0)
         |---------"r" (1)
        "#);

        trie.insert(b"bis", 2);
        insta::assert_debug_snapshot!(trie, @r#"
        "bi" (-)
         |-------"ke" (0)
                 |-------"r" (1)
         |-------"s" (2)
        "#);

        trie.insert(b"cool", 3);
        insta::assert_debug_snapshot!(trie, @r#"
        "" (-)
         |-----"bi" (-)
               |-------"ke" (0)
                       |-------"r" (1)
               |-------"s" (2)
         |-----"cool" (3)
        "#);
    }
}

