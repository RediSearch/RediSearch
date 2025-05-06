/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Trie operations.
use super::Node;
use crate::utils::{longest_common_prefix, memchr_c_char, strip_prefix};
use std::{
    alloc::dealloc, cmp::Ordering, ffi::c_char, marker::PhantomData, mem::ManuallyDrop,
    ptr::NonNull,
};

impl<Data> Node<Data> {
    /// Inserts a new key-value pair into the trie.
    ///
    /// If the key already exists, the current value is passede to provided function,
    /// and replaced with the value returned by that function.
    pub fn insert_or_replace_with<F>(&mut self, mut key: &[c_char], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        let mut current = self;
        loop {
            match longest_common_prefix(current.label(), key) {
                Some((0, ordering)) => {
                    // The node's label and the key don't share a common prefix.
                    // This can only happen if the current node is the root node of the trie.
                    //
                    // Create a new root node with an empty label,
                    // insert the old root as a child of the new root,
                    // and add a new child to the empty root.
                    current.map(|old_root| {
                        let new_child = Node::new_leaf(key, Some(f(None)));
                        let children = if ordering == Ordering::Greater {
                            [new_child, old_root]
                        } else {
                            [old_root, new_child]
                        };
                        // SAFETY:
                        // - Both `key` and `current.label()` are at least one byte long,
                        //   since `longest_common_prefix` found that their `0`th bytes differ.
                        unsafe { Node::new_unchecked(&[], children, None) }
                    });
                    break;
                }
                Some((equal_up_to, _)) => {
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
                    //
                    // Create a new node that uses the shared prefix as its label.
                    // The prefix is then stripped from both the current label and the
                    // new key; the resulting suffixes are used as labels for the new child nodes.
                    current.map(|old_root| {
                        // SAFETY:
                        // - `key` is at least `equal_up_to` bytes long, since `longest_common_prefix`
                        //   found that its `equal_up_to` byte differed from the corresponding byte in
                        //   the current label.
                        let (_, new_child_suffix) = unsafe { key.split_at_unchecked(equal_up_to) };
                        let new_child = Node::new_leaf(new_child_suffix, Some(f(None)));
                        // SAFETY:
                        // - `old_root.label()` is at least `equal_up_to` bytes long, since `longest_common_prefix`
                        //   found that its `equal_up_to` byte differed from the corresponding byte in
                        //   `key`.
                        unsafe { old_root.split_unchecked(equal_up_to, Some(new_child)) }
                    });
                    break;
                }
                None => {
                    match key.len().cmp(&(current.label_len() as usize)) {
                        Ordering::Less => {
                            // The key we want to insert is a strict prefix of the current node's label.
                            // Therefore we need to insert a new _parent_ node.
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
                            current.map(|old_root| {
                                // SAFETY:
                                // - In this branch, `old_root.label()` is strictly longer than `key`,
                                //   so `key.len()` is in range for `old_root.label()`.
                                let mut new_root =
                                    unsafe { old_root.split_unchecked(key.len(), None) };
                                *new_root.data_mut() = Some(f(None));
                                new_root
                            });
                            break;
                        }
                        Ordering::Equal => {
                            // Suffix is empty, so the key and the node label are equal.
                            // Replace the data attached to the current node
                            // with the new data.
                            let data = current.data_mut();
                            let current_data = data.take();
                            let new_data = f(current_data);
                            *data = Some(new_data);
                            break;
                        }
                        Ordering::Greater => {
                            // Suffix is not empty, therefore the insertion needs to happen
                            // in a child (or grandchild) of the current node.

                            // SAFETY:
                            // - In this branch, `key` is strictly longer than `current.label()`,
                            //   so `current.label_len()` is in range for `key`.
                            key = unsafe { key.get_unchecked(current.label_len() as usize..) };
                            let first_byte = key[0];
                            match current.child_index_starting_with(first_byte) {
                                Some(i) => {
                                    current =
                                        // SAFETY:
                                        // - The index returned by `child_index_starting_with` is
                                        //   always in range for the children pointers array.
                                        unsafe { current.children_mut().get_unchecked_mut(i) };
                                    // Recursion!
                                    continue;
                                }
                                None => {
                                    let insertion_index = current
                                        .children_first_bytes()
                                        .binary_search(&first_byte)
                                        // We know we won't find match at this point.
                                        .unwrap_err();

                                    current.map(|root| {
                                        let new_child = Node::new_leaf(key, Some(f(None)));
                                        // SAFETY:
                                        // - The index returned by `binary_search` is
                                        //   never greater than the length of the searched array.
                                        unsafe {
                                            root.add_child_unchecked(new_child, insertion_index)
                                        }
                                    });
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the key is not present.
    pub fn find(&self, mut key: &[c_char]) -> Option<&Data> {
        let mut current = self;
        loop {
            key = strip_prefix(key, current.label())?;
            let Some(first_byte) = key.first() else {
                // The suffix is empty, so the key and the label are equal.
                return current.data();
            };
            current = current.child_starting_with(*first_byte)?;
        }
    }

    /// Get a reference to the child node whose label starts with the given byte.
    /// Returns `None` if there is no such child.
    pub fn child_starting_with(&self, c: c_char) -> Option<&Node<Data>> {
        let i = self.child_index_starting_with(c)?;
        // SAFETY:
        // Guaranteed by invariant 1. in [`Self::child_index_starting_with`].
        Some(unsafe { self.children().get_unchecked(i) })
    }

    /// Get the index of the child node whose label starts with the given byte.
    /// Returns `None` if there is no such child.
    ///
    /// # Invariants
    ///
    /// 1. The index returned by this function is guaranteed to be within
    ///    the bounds of the children pointers array and the children
    ///    first bytes array.
    #[inline]
    pub fn child_index_starting_with(&self, c: c_char) -> Option<usize> {
        memchr_c_char(c, self.children_first_bytes())
    }

    /// Remove the descendant of this node that matches the given key, if any.
    ///
    /// Returns the data associated with the removed node, if any.
    pub fn remove_descendant(&mut self, key: &[c_char]) -> Option<Data> {
        // Find the index of child whose label starts with the first byte of the key,
        // as well as the child itself.
        // If the we find none, there's nothing to remove.
        // Note that `key.first()?` will cause this function to return None if the key is empty.
        let child_index = self.child_index_starting_with(*key.first()?)?;
        let child = &mut self.children_mut()[child_index];

        let suffix = strip_prefix(key, child.label())?;

        if suffix.is_empty() {
            // The child's label is equal to the key, so we remove the child.
            let data = child.data_mut().take();

            let is_leaf = child.n_children() == 0;
            if is_leaf {
                let mut current = std::mem::replace(
                    self,
                    Node {
                        ptr: NonNull::dangling(),
                        _phantom: PhantomData,
                    },
                );
                // If the child is a leaf, we remove the child node itself.
                // SAFETY:
                // Guaranteed by invariant 1. in [`Self::child_index_starting_with`].
                current = unsafe { current.remove_child_unchecked(child_index) };
                std::mem::forget(std::mem::replace(self, current));
            } else {
                // If there's a single grandchild,
                // we merge the grandchild into the child.
                child.merge_child_if_possible();
            }

            data
        } else {
            let data = child.remove_descendant(suffix);
            child.merge_child_if_possible();
            data
        }
    }

    /// If `self` has exactly one child, and `self` doesn't hold
    /// any data, merge child into `self`, by moving the child's data and
    /// children into `self`.
    pub fn merge_child_if_possible(&mut self) {
        if self.data().is_some() || self.n_children() != 1 {
            return;
        }
        let old_child = std::mem::replace(
            &mut self.children_mut()[0],
            Node {
                ptr: NonNull::dangling(),
                _phantom: Default::default(),
            },
        );
        let new_parent = old_child.prepend(self.label());
        let old_self = ManuallyDrop::new(std::mem::replace(self, new_parent));
        // There is no data and we removed the child, so we can
        // just free the buffer and be done.
        // SAFETY:
        // - The pointer was allocated via the same global allocator
        //    we are invoking via `dealloc` (see invariant 3. in [`Self::ptr`])
        // - `old_self.metadata().layout()` is the same layout that was used
        //   to allocate the buffer (see invariant 1. in [`Self::ptr`])
        unsafe {
            dealloc(old_self.ptr.as_ptr().cast(), old_self.metadata().layout());
        }
    }

    /// The memory usage of this node and his descendants, in bytes.
    pub fn mem_usage(&self) -> usize {
        let mut total_size = self.metadata().layout().size();
        let mut stack: Vec<&Node<Data>> = self.children().iter().collect();

        while let Some(node) = stack.pop() {
            total_size += node.metadata().layout().size();
            stack.extend(node.children().iter());
        }

        total_size
    }

    /// The number of descendants of this node.
    pub fn n_descendants(&self) -> usize {
        let mut stack: Vec<&Node<Data>> = self.children().iter().collect();
        let mut n_descendants = self.n_children() as usize;
        while let Some(node) = stack.pop() {
            n_descendants += node.n_children() as usize;
            stack.extend(node.children().iter());
        }
        n_descendants
    }
}
