use std::cmp::Ordering;
use std::fmt;
use std::{ffi::c_char, marker::PhantomData, ops::Deref, ptr::NonNull, usize};

use crate::branching::BranchingNode;
use crate::header::{AllocationHeader, NodeKind};
use crate::leaf::LeafNode;
use crate::to_string_lossy;

// N<D>: 8bytes ---> dyn Bytes Heap
// BN: 8bytes ---> dyn Bytes Heap

#[derive(Clone, PartialEq, Eq)]
pub struct Node<Data> {
    pub(crate) ptr: std::ptr::NonNull<AllocationHeader>,
    pub(crate) _phantom: PhantomData<Data>,
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

pub fn get_shared_prefix_len(lh_label: &[c_char], rh_label: &[c_char]) -> usize {
    lh_label
        .iter()
        .zip(rh_label)
        .enumerate()
        .find_map(|(i, (c1, c2))| if c1 != c2 { Some(i) } else { None })
        .unwrap_or(0)
}

impl<Data> Node<Data> {
    /// A node that can be used as a cheap placeholder whenever we need to temporarily
    /// take ownership of a node from a `&mut` reference.
    pub fn dummy_node() -> Self {
        BranchingNode::create_root().into()
    }

    pub fn root() -> Self {
        BranchingNode::create_root().into()
    }

    /// Create a new leaf node.
    pub fn leaf(label: &[c_char], data: Data) -> Self {
        LeafNode::new(data, label).into()
    }

    /// Create a new branching node.
    pub fn branching(label: &[c_char], children: &[Node<Data>]) -> Self {
        // Safety:
        // We know there is a least one children which makes calling allocate save
        let bnode = unsafe { BranchingNode::allocate(label, &[children]) };

        bnode.into()
    }

    /// Get a reference to the allocation header.
    ///
    /// # Implementation notes
    ///
    /// The header is the only is the only portion of the allocated buffer
    /// that can be safely accessed using [`Node`].
    /// All other fields require casting first into either [`LeafNode`] or
    /// [`BranchingNode`].
    pub fn header(&self) -> &AllocationHeader {
        unsafe { self.ptr.as_ref() }
    }

    pub fn kind(&self) -> NodeKind {
        self.header().kind()
    }

    pub fn is_leaf(&self) -> bool {
        self.kind() == NodeKind::Leaf
    }

    pub fn is_branching(&self) -> bool {
        self.kind() == NodeKind::Branching
    }

    pub fn get_shared_prefix_len(&self, rh_label: &[c_char]) -> usize {
        get_shared_prefix_len(self.label(), rh_label)
    }

    /// Grows this node by one child
    ///
    /// - child_label: the label of the new child
    /// - child_data: the data of the new child
    /// - index_or_insert: a Result with the semantic of [std::slice::binary_search]
    ///
    /// Internally this uses implementation of [crate::node::branching] and [crate::node::leaf] and ensures
    /// the shallow drop if neccessary
    ///
    /// When growing by a child we end up with the following cases:
    ///
    /// 1. self is a leaf
    ///   1.1 this leaf maybe an empty labeled node -> Replace data
    ///   1.2 the leaf is not empty labeled -> use [leaf::LeafNode::add_child]
    /// 2. self is a branching node -> delegate to [branching::BranchingNode::grow]
    ///
    fn grow(
        &mut self,
        child_label: &[c_char],
        child_data: Data,
        index_or_insert_at: Result<usize, usize>,
    ) -> Option<Data> {
        match self.cast_mut() {
            // case 1---: self is a leaf
            Either::Left(leaf) => {
                // case 1-1: We grow an empty labeled child
                if child_label.is_empty() {
                    let old_data = std::mem::replace(leaf.data_mut(), child_data);
                    Some(old_data)
                // case 1-2: We use [leaf::LeafNode]
                } else {
                    // Safety: self changes from leaf to branching, that's ok as we leave the method
                    //          here and callers only know the upcast type.
                    unsafe {
                        // todo check if this is sound as `leaf` is actually a `&mut BranchingNode` after this call
                        leaf.add_child(child_label, child_data);
                    }

                    None
                }
            }
            // case 2---: self is a branch
            Either::Right(mut _self) => {
                let opt_data = _self.grow(child_label, child_data, index_or_insert_at);
                opt_data
            }
        }
    }

    fn shrink(&mut self, child_index: usize) -> Node<Data> {
        let Either::Right(branching) = self.cast_mut() else {
            unreachable!("self is always going to be a branching node")
        };

        if branching.num_children() >= 1 {
            unreachable!()
        } else if branching.num_children() == 2 {
            // switch to leaf, merge self.label with new_leaf's label
            todo!();
            //let new_leaf = branching[!child_index];
            //let removed_leaf = branching[child_index];
            //std::mem::replace(self, new_leaf);
            //removed_leaf
        } else {
            // just shrink
            branching.shrink(child_index)
        }
    }

    pub fn insert(&mut self, mut key: &[c_char], data: Data) -> Option<Data> {
        let mut current = self;
        loop {
            match current
                .label()
                .iter()
                .zip(key.iter())
                .enumerate()
                .find_map(|(i, (c1, c2))| if c1 != c2 { Some(i) } else { None })
            {
                Some(0) => {
                    let Self {
                        ptr: current_ptr,
                        _phantom,
                    } = current;
                    let old_root = Self {
                        ptr: std::mem::replace(current_ptr, NonNull::dangling()),
                        _phantom: Default::default(),
                    };
                    // The node's label and the key don't share a common prefix.
                    // This can only happen if the current node is the root node of the trie.
                    //
                    // Create a new root node with an empty label,
                    // insert the old root as a child of the new root,
                    // and add a new child to the empty root.
                    let new_leaf = Node::leaf(key, data);
                    let children = if key < old_root.label() {
                        [old_root, new_leaf]
                    } else {
                        [new_leaf, old_root]
                    };
                    let new_root: Node<_> =
                        unsafe { BranchingNode::allocate(&[], &[&children]) }.into();
                    let Self {
                        ptr: new_root_ptr, ..
                    } = new_root;
                    *current_ptr = new_root_ptr;

                    return None;
                }
                Some(equal_up_to) => {
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
                    let Self {
                        ptr: current_ptr,
                        _phantom,
                    } = current;
                    let old_root = Self {
                        ptr: std::mem::replace(current_ptr, NonNull::dangling()),
                        _phantom: Default::default(),
                    };
                    let (shared_prefix, old_root_suffix) = old_root.label().split_at(equal_up_to);

                    let new_child: Node<_> = Node::leaf(&key[equal_up_to..], data).into();
                    let shortened_root = Node::branching(&old_root_suffix[..], old_root.children());

                    let children = if new_child.label() < shortened_root.label() {
                        &[new_child, shortened_root]
                    } else {
                        &[shortened_root, new_child]
                    };

                    let Self {
                        ptr: new_root_ptr, ..
                    } = Node::branching(shared_prefix, children);
                    *current_ptr = new_root_ptr;

                    return None;
                }
                None => {
                    match key.len().cmp(&current.label().len()) {
                        Ordering::Less => {
                            // The key we want to insert is a strict prefix of the current node's label.
                            // Therefore we need to insert a new _parent_ node.
                            //
                            // .split(index)
                            // .split(prefix, suffix)
                            //
                            let Self {
                                ptr: current_ptr,
                                _phantom,
                            } = current;
                            let old_node = Self {
                                ptr: std::mem::replace(current_ptr, NonNull::dangling()),
                                _phantom: Default::default(),
                            };

                            let new_child: Node<Data> = match old_node.cast() {
                                // # Case 1: No children for the current node
                                //
                                // Add `bike` with data `B` to a trie with `biker`, where `biker` has data `A`.
                                // ```text
                                // biker (A)  ->   bike (B)
                                //                /
                                //               r (A)
                                // ```
                                Either::Left(mut leaf) => {
                                    leaf.truncate_label_prefix(key.len());
                                    leaf.into()
                                }
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
                                Either::Right(branching) => Node::branching(
                                    &branching.label()[key.len()..],
                                    branching.children(),
                                ),
                            };

                            let Self {
                                ptr: new_parent_ptr,
                                ..
                            } = Node::branching(key, &[new_child]);
                            *current_ptr = new_parent_ptr;

                            return None;
                        }
                        Ordering::Equal => {
                            // Suffix is empty, so the key and the node label are equal.
                            // Replace the data attached to the current node
                            // with the new data.
                            return match current.cast_mut() {
                                Either::Left(leaf) => {
                                    Some(std::mem::replace(leaf.data_mut(), data))
                                }
                                Either::Right(branching) => {
                                    if branching.has_empty_labeled_child() {
                                        let Some(child) = branching.children_mut().last_mut()
                                        else {
                                            unreachable!()
                                        };
                                        let Either::Left(child) = child.cast_mut() else {
                                            unreachable!()
                                        };
                                        Some(std::mem::replace(child.data_mut(), data))
                                    } else {
                                        let new_leaf = Node::leaf(&[], data);
                                        let new_branching = unsafe {
                                            BranchingNode::allocate(
                                                branching.label(),
                                                &[branching.children(), &[new_leaf]],
                                            )
                                        }
                                        .into();
                                        let old = std::mem::replace(branching, new_branching);
                                        // TODO: deallocate the buffer without dropping the children
                                        std::mem::forget(old);
                                        None
                                    }
                                }
                            };
                        }
                        Ordering::Greater => {
                            // Suffix is not empty, therefore the insertion needs to happen
                            // in a child (or grandchild) of the current node.

                            let suffix = &key[current.label().len()..];
                            if current.is_branching() {
                                match current.children_first_bytes().binary_search(&suffix[0]) {
                                    Ok(i) => {
                                        // We recurse!
                                        current = &mut current.children_mut()[i];
                                        key = suffix;
                                        continue;
                                    }
                                    Err(i) => {
                                        let new_child = Node::leaf(suffix, data);

                                        let (children_with_labels, empty_labeled) =
                                            if current.data().is_some() {
                                                current
                                                    .children()
                                                    .split_at(current.children().len() - 1)
                                            } else {
                                                (current.children(), [].as_slice())
                                            };

                                        let (before, after) =
                                            match children_with_labels.split_at_checked(i) {
                                                Some((before, after)) => (before, after),
                                                None => (children_with_labels, [].as_slice()),
                                            };
                                        let new_branching = unsafe {
                                            BranchingNode::allocate(
                                                current.label(),
                                                &[before, &[new_child], after, empty_labeled],
                                            )
                                        }
                                        .into();
                                        *current = new_branching;
                                        return None;
                                    }
                                }
                            } else {
                                let new_leaf = Node::leaf(suffix, data);
                                let new_branching_label: Vec<_> = current.label().to_owned();
                                {
                                    let Either::Left(old_leaf) = current.cast_mut() else {
                                        unreachable!()
                                    };
                                    old_leaf.truncate_label_prefix(old_leaf.label_len() as usize);
                                }

                                let Self {
                                    ptr: current_ptr,
                                    _phantom,
                                } = current;
                                let empty_labeled_child = Self {
                                    ptr: std::mem::replace(current_ptr, NonNull::dangling()),
                                    _phantom: Default::default(),
                                };
                                let here = &[new_leaf, empty_labeled_child];
                                let Self {
                                    ptr: new_branching_ptr,
                                    _phantom,
                                } = Node::branching(&new_branching_label, here);
                                *current_ptr = new_branching_ptr;
                                return None;
                            }
                        }
                    }
                }
            }
        }
    }

    /*
    pub fn tree<I: Iterator<Item = (&[c_char], Data)>>() -> Self {
        todo!("later for optimization")
    }
    */

    pub fn label(&self) -> &[c_char] {
        match self.cast_ref() {
            Either::Left(n) => n.label(),
            Either::Right(n) => n.label(),
        }
    }

    pub fn data(&self) -> Option<&Data> {
        match self.cast_ref() {
            Either::Left(n) => Some(n.data()),
            Either::Right(n) => n.data(),
        }
    }

    pub fn cast(self) -> Either<LeafNode<Data>, BranchingNode<Data>> {
        match self.kind() {
            NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn cast_ref(&self) -> Either<&LeafNode<Data>, &BranchingNode<Data>> {
        match self.kind() {
            NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn cast_mut(&mut self) -> Either<&mut LeafNode<Data>, &mut BranchingNode<Data>> {
        match self.kind() {
            NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn find<'a>(&'a self, mut key: &[c_char]) -> Option<&'a Data> {
        let mut current = self;
        loop {
            key = key.strip_prefix(current.label())?;
            let Some(first_byte) = key.first() else {
                // The suffix is empty, so the key and the label are equal.
                return current.data();
            };
            let child_index = current
                .children_first_bytes()
                .binary_search(first_byte)
                .ok()?;
            current = &current.children()[child_index];
        }
    }

    /// Finds the parent of the node matching the given key, along with the index
    /// of that node in the list of chilren of the parent node.
    /// todo naming:
    /// examples: find "a", find "aaa"
    ///
    //           ""
    //         /  |
    //     ""(C) "aa"
    //          /   \
    //      "aa" (A)  "" (B)
    fn find_node_parent_mut<'a>(
        &mut self,
        key: &'a [c_char],
    ) -> (&mut Node<Data>, Result<usize, usize>, &'a [c_char]) {
        // case 1: key is empty -> self is parent -> must be a branch, so add we add an empty node to that branch
        //
        // case 2-x: self is a leaf node
        // case 2-1: leaf node is < suffix -> we insert new at 1
        // case 2-2: leaf node is > suffix -> we insert new at 0
        // case 2-3: leaf node label == suffix -> we replace that leaf node (how to encode via return value?)
        //
        // case 3-x-y: self is a branching node
        // case-3-1-y: found a child candidate via first bytes
        // case-3-1-1: child candidate is leaf -> return self as parent
        // case-3-1-2: child candidate is branching and has a partial prefix, e.g. 'aa' and 'ab' -> recursion on candidate
        // case-3-1-3: child candidate is branching and has a full prefix -> self is parent and candidate was searched
        // case-3-2: didn't found a candidate via first bytes -> self is parent: decide where to insert via binary search

        // case 1
        if key.is_empty() {
            debug_assert!(self.is_branching());
            //println!("case-1");
            return (self, Err(usize::MAX), &[]); //  we insert an empty labeled node at the end
        }

        // get shared_prefix:
        let shared_prefix_len = get_shared_prefix_len(key, self.label());
        let suffix: &[c_char] = &key[shared_prefix_len..];

        match self.cast_mut() {
            // case 2-x self is leaf:
            Either::Left(leaf) => {
                let index_or_insert_at = if suffix.is_empty() {
                    // Replace this leaf
                    Ok(0)
                } else {
                    // Insert after this leaf
                    Err(0)
                };
                //println!("case-2");
                (leaf.as_mut(), index_or_insert_at, suffix)
            }
            // case 3-x-y: self is branching:
            Either::Right(branching) => {
                // todo: use first binary_bytes
                let search_result = branching.children_first_bytes().binary_search(&key[0]);
                //let search_result=  branching.children().binary_search_by(|e| e.label()[0].cmp(&key[0]));
                //println!("case-3");
                match search_result {
                    Ok(found_at) => {
                        let candidate = &branching.children()[found_at];
                        let shared_candidate_suffix_len =
                            get_shared_prefix_len(candidate.label(), suffix);
                        let candidate_suffix: &[c_char] = &suffix[shared_candidate_suffix_len..];
                        if candidate.is_leaf() {
                            //println!("case-3-1-1");
                            (branching.as_mut(), Ok(found_at), candidate_suffix)
                        } else {
                            if shared_candidate_suffix_len < candidate.label().len() {
                                //println!("case-3-1-2");
                                branching.children_mut()[found_at]
                                    .find_node_parent_mut(candidate_suffix)
                            } else {
                                // we know shared_candidate_suffix_len = candidate_label_len
                                //println!("case-3-1-3");
                                (branching.as_mut(), Ok(found_at), candidate_suffix)
                            }
                        }
                    }
                    Err(insert_at) => {
                        //println!("case-3-2");
                        (branching.as_mut(), Err(insert_at), suffix)
                    }
                }
            }
        }
    }

    pub fn add_child(&mut self, key: &[c_char], data: Data) -> Option<Data> {
        // Adding 'bis' then 'bikers' to the trie
        // ```text
        //          ""                                ""                               ""
        //      /    |                            /        \                       /       \
        //   "" (F) "bike"        ->          "" (F)      "bi"            ->   "" (F)      "bi"
        //        /    |    \                           /       \                          /       \
        //  "" (A)  "r" (B)  "s" (C)              "ke"          "s" (D)              "ke"          "s" (D)
        //                                     /    |    \                        /    |    \
        //                               "" (A)  "r" (B)  "s" (C)             "" (A)  "r"       "s" (C)
        //                                                                           /  \
        //                                                                      "" (B)  "s" (E)
        // ```

        // Alloc new branching (bi) with space for 2 children
        // Alloc new leaf (s)
        // Alloc a new branching (ke) with space for as many children as the node I'm splitting
        // *self = (bi) node

        // Empty trie, with one branching node with no children (the root)
        // Alloc new branching (root) with 1 child
        // Alloc new leaf
        // *self = new branching

        let (parent, index_or_insert_at, child_label) = self.find_node_parent_mut(key);

        parent.grow(child_label, data, index_or_insert_at)

        // parent.label always shares a prefix of child_label
        //    if parent.label == child_label && parent.is_branching()
        //        add a (value-child) to the parent
        //    else if parent.label == child_label && parent.is_leaf_node() --> we have a replace
        //         replace data!
        //    else split the parent at the length of the shared prefix
        //       if parent.is_leaf_node()
        //          replace parent with a branching node with the shared prefix as a label,
        //               add to it a node with an empty label and one with child_label
        //       else
        //          create a new branching node to be the parent, with capacity for one more child
        //          create a new child node with the child label and the data we want
        //          copy over the old children and the new child
        //          replace old branching node with new one
    }

    pub fn remove_child(&mut self, key: &[c_char]) -> Option<Data> {
        let (_parent, _index_or_insert_at, _label) = self.find_node_parent_mut(key);
        // if parent.child_first_bytes()[idx] != label[0] {
        //     return None;
        // }

        // take data from the child at parent.children[idx]

        // replace parent with a new node
        // Depending on whether parent will a single child after removing
        // the other one, it's gonna be a merged leaf node. If there are
        // more than one child, it's gonna be a branching node with 1 fewer child.

        todo!()
    }

    fn children(&self) -> &[Node<Data>] {
        match self.cast_ref() {
            Either::Left(_) => &[],
            Either::Right(branching) => branching.children(),
        }
    }

    fn children_first_bytes(&self) -> &[c_char] {
        match self.cast_ref() {
            Either::Left(_) => return &[],
            Either::Right(branching) => return branching.children_first_bytes(),
        }
    }

    pub fn children_iter(&self) -> impl Iterator<Item = &Node<Data>> {
        self.children().iter()
    }

    fn children_mut(&mut self) -> &mut [Node<Data>] {
        match self.cast_mut() {
            Either::Left(_) => return &mut [],
            Either::Right(branching) => return branching.children_mut(),
        }
    }

    pub fn children_iter_mut(&mut self) -> impl Iterator<Item = &mut Node<Data>> {
        self.children_mut().iter_mut()
    }
}

impl<Data: fmt::Debug> fmt::Debug for Node<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut stack = vec![(self, 0, 0)];

        while let Some((next, white_indentation, line_indentation)) = stack.pop() {
            let label_repr = to_string_lossy(next.label());
            let data_repr = match next.cast_ref() {
                Either::Left(leaf) => {
                    format!("({:?})", leaf.data())
                }
                Either::Right(_) => "(-)".to_string(),
            };

            let prefix = if white_indentation == 0 && line_indentation == 0 {
                "".to_string()
            } else {
                let whitespace = " ".repeat(white_indentation);
                let line = "–".repeat(line_indentation);
                format!("{whitespace}↳{line}")
            };

            writeln!(f, "{prefix}\"{label_repr}\" {data_repr}")?;

            for node in next.children().iter().rev() {
                let new_line_indentation = 4;
                let white_indentation = white_indentation + line_indentation + 2;
                stack.push((node, white_indentation, new_line_indentation));
            }
        }
        Ok(())
    }
}
