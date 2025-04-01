#![allow(
    dead_code,
    unused_imports
    )]
    
use std::{ffi::c_char, marker::PhantomData, ops::Deref, ptr::NonNull, usize};

use branching::BranchingNode;
use header::{AllocationHeader, NodeKind};
use leaf::LeafNode;
//use proptest::bits::BitSetLike;
mod branching;
mod header;
// mod layout;
mod leaf;

// N<D>: 8bytes ---> dyn Bytes Heap
// BN: 8bytes ---> dyn Bytes Heap

#[derive(Clone, PartialEq, Eq)]
pub struct Node<Data> {
    ptr: std::ptr::NonNull<AllocationHeader>,
    _phantom: PhantomData<Data>,
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
        .find(|(_, (c1, c2))| c1 != c2)
        .map(|(i, _)| i)
        .unwrap_or(0)
}

impl<Data> Node<Data> {
    /// Create a new leaf node.
    pub fn leaf(label: &[c_char], data: Data) -> Self {
        leaf::LeafNode::new(data, label).into()
    }

    pub fn branching(label: &[c_char], children: &[Node<Data>]) -> Self {
        // Safety:
        // We know there is a least one children which makes calling allocate save
        let bnode = unsafe {
            branching::BranchingNode::allocate(label, children, None, &[])
        };

        bnode.into()
    }

    pub fn root() -> Self {
        branching::BranchingNode::create_root().into()
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

    pub fn cast(self) -> Either<leaf::LeafNode<Data>, branching::BranchingNode<Data>> {
        match unsafe { self.ptr.as_ref() }.kind() {
            NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn cast_ref(&self) -> Either<&leaf::LeafNode<Data>, &branching::BranchingNode<Data>> {
        match unsafe { self.ptr.as_ref() }.kind() {
            NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn cast_mut(
        &mut self,
    ) -> Either<&mut leaf::LeafNode<Data>, &mut branching::BranchingNode<Data>> {
        match unsafe { self.ptr.as_ref() }.kind() {
            NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn is_leaf(&self) -> bool {
        let header = unsafe { self.ptr.as_ref() };
        header.kind() == NodeKind::Leaf
    }

    pub fn is_branching(&self) -> bool {
        let header = unsafe { self.ptr.as_ref() };
        header.kind() == NodeKind::Branching
    }

    pub fn find_node<'a>(&'a mut self, key: &[c_char]) -> Option<&'a Node<Data>> {
        let find_state = self.find_node_parent_mut(&key);
        match find_state.1 {
            Ok(found) => Some(&find_state.0.children()[found]),
            Err(_) => None
        }
    }

    pub fn find<'a>(&'a mut self, key: &[c_char]) -> Option<&'a Data> {
        let node = self.find_node(key)?;
        match node.cast_ref() {
            Either::Left(l) => Some(l.data()),
            Either::Right(_) => None,
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
                let index_or_insert_at = match leaf.label().cmp(suffix) {
                    std::cmp::Ordering::Less => Err(1),     // insert new before this leaf
                    std::cmp::Ordering::Equal => Ok(0),     // replace this leaf
                    std::cmp::Ordering::Greater => Err(0),  // insert new after this leaf
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
                        let shared_candidate_suffix_len = get_shared_prefix_len(candidate.label(), suffix);
                        let candidate_suffix: &[c_char] = &suffix[shared_candidate_suffix_len..];
                        if candidate.is_leaf() {
                            //println!("case-3-1-1");
                            (branching.as_mut(), Ok(found_at), candidate_suffix)
                        } else {
                            if shared_candidate_suffix_len < candidate.label().len() {
                                //println!("case-3-1-2");
                                branching.children_mut()[found_at].find_node_parent_mut(candidate_suffix)
                            } else {
                                // we know shared_candidate_suffix_len = candidate_label_len
                                //println!("case-3-1-3");
                                (branching.as_mut(), Ok(found_at), candidate_suffix)
                            }
                        }
                    },
                    Err(insert_at) => {
                        //println!("case-3-2");
                        (branching.as_mut(), Err(insert_at), suffix)
                    },
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
            Either::Left(_) => return &[],
            Either::Right(branching) => return branching.children(),
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::ToCCharArray;

    #[test]
    fn test_new_trie() {
        // create root node
        let mut node: Node<i32> = Node::branching(&b"".c_chars(), &[]);
        node.add_child(&b"bike".c_chars(), 0);
        println!("find now");
        assert_eq!(node.find(&b"bike".c_chars()), Some(&0));
        assert_eq!(node.find(&b"cool".c_chars()), None);
        
        // todo: implement debug snapshot print
        //assert_debug_snapshot!(trie, @r#""bike" (0)"#);
    }
}