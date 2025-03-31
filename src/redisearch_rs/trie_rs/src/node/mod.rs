use std::{ffi::c_char, marker::PhantomData};

use branching::BranchingNode;
use header::{AllocationHeader, NodeKind};
mod branching;
mod header;
// mod layout;
mod leaf;

pub struct Node<Data> {
    ptr: std::ptr::NonNull<AllocationHeader>,
    _phantom: PhantomData<Data>,
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<Data> Node<Data> {
    /// Create a new leaf node.
    pub fn leaf(label: &[c_char], data: Data) -> Self {
        leaf::LeafNode::new(data, label).into()
    }

    pub fn branching(label: &[c_char], children: &[Node<Data>]) -> Self {
        
        todo!()
    }

    /// Grows this node by one children
    /// 
    /// - child_label: the label of the new child
    /// - child_data: the data of the new child
    /// - index_or_insert: a Result with the semantic of [std::slice::binary_search]
    /// 
    /// Internally this uses implementation of [crate::node::branching] and [crate::node::leaf] and ensures
    /// the shallow drop if neccessary
    fn grow(
        &mut self, 
        child_label: &[c_char], 
        child_data: Data, 
        index_or_insert_at: Result<usize, usize>
    ) -> Option<Data> {
        match self.cast_mut() {
            Either::Left(leaf) => {
                // case 1-1: We grow an empty labeled child
                if child_label.is_empty() {
                    let old_data = std::mem::replace(leaf.data_mut(), child_data);
                    Some(old_data)
                } else {
                    // Safety: self changes from leaf to branching, that's ok as we leave the method
                    //          here and callers only know the upcast type.
                    unsafe {
                        leaf.add_child(child_label, child_data);
                        None
                    }
                }
            }
            Either::Right(_self) => {
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

    /// Finds the parent of the node matching the given key, along with the index
    /// of that node in the list of chilren of the parent node.
    /// todo naming
    pub fn find_node_parent_mut(
        &mut self,
        key: &[c_char],
    ) -> (&mut Node<Data>, Result<usize, usize>, &[c_char]) {
        // if self is a leaf, then return None
        //if self.is_leaf() { return None; }

        // if self is a branching node, then find the child node that corresponds to the first byte of the key
        //  if that node exactly matches the key, then self is the parent of the node we're looking for
        //  if that node doesn't match the key, then call find_node_parent_mut on that child node with the rest of the key,
        //  the rest if the key being key.strip_prefix(self.label)

        // if no child node corresponds to the first byte of the key, then return None

        todo!()
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
        let (parent, idx, label) = self.find_node_parent_mut(key);
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

    fn children(&self) -> impl Iterator<Item = Node<Data>> {
        [].into_iter()
    }
}
