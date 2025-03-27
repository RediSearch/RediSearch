use std::{ffi::c_char, marker::PhantomData};

use branching::BranchingNode;
use header::{AllocationHeader, AllocationHeader};
mod branching;
mod header;
mod layout;
mod leaf;

static mut SENTINEL_HEADER: AllocationHeader = AllocationHeader::sentinel();

pub struct Node<Data> {
    ptr: std::ptr::NonNull<AllocationHeader>,
    _phantom: PhantomData<Data>,
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<Data> Node<Data> {
    fn sentinel() -> Self {
        Node {
            ptr: unsafe {std::ptr::NonNull::new_unchecked(&mut SENTINEL_HEADER as *mut AllocationHeader)},
            _phantom: PhantomData,
        }
    }

    fn sentinel_leaf() -> Self {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe {
            std::mem::transmute(Self::sentinel())
        }
    }

    fn sentinel_branching() -> BranchingNode<Data> {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe {
            std::mem::transmute(Self::sentinel())
        }
    }

    /// Create a new leaf node.
    pub fn leaf(label: &[c_char], data: Data) -> Self {
        leaf::LeafNode::new(data, label).into()
    }

    pub fn branching(num_children: u8, label: &[c_char]) -> Self {
        todo!()
    }

    fn grow(&mut self, child: Node<Data>) {
        let new_parent = match self.cast_mut() {
            Either::Left(leaf) => {
                let leaf = std::mem::replace(
                    leaf,
                    Node::<Data>::sentinel().into(),
                );

                let new_parent:  BranchingNode<Data> = leaf.add_child(child);
                new_parent.upcast() // transform into Node<Data>
            }
            Either::Right(_self) => {
                let old_parent  = std::mem::replace(
                    _self,
                    Node::<Data>::sentinel().into(),
                );

                let new_parent = old_parent.grow(child, child_label);

                new_parent.upcast() // transform into Node<Data>
            }
        };

        // change new_parent drop flag 
        std::mem::replace(self, new_parent);
        // hcnage self drop flag
    }

    fn shrink(&mut self, child_index: usize) -> Node<Data> {
        let Either::Right(branching) = self.cast_mut() else {
            unreachable!("self is always going to be a branching node")
        };

        if branching.num_children() >= 1 {
            unreachable!()
        } else if branching.num_children() == 2 {
            // switch to leaf, merge self.label with new_leaf's label
            let new_leaf = branching[!child_index];
            let removed_leaf = branching[child_index];
            std::mem::replace(self, new_leaf);
            removed_leaf
        } else {
            // just shrink
            branching.shrink(child_index)
        }
    }

    pub fn tree<I: Iterator<Item = (&[c_char], Data)>>() -> Self {
        todo!("later for optimization")
    }

    pub fn label(&self) -> &[c_char] {
        match self.cast_ref() {
            Either::Left(n) => n.label(),
            Either::Right(n) => n.label(),
        }
    }

    pub fn cast(self) -> Either<leaf::LeafNode<Data>, branching::BranchingNode<Data>> {
        match unsafe { self.ptr.as_ref() }.kind() {
            header::NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            header::NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn cast_ref(&self) -> Either<&leaf::LeafNode<Data>, &branching::BranchingNode<Data>> {
        match unsafe { self.ptr.as_ref() }.kind() {
            header::NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            header::NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn cast_mut(
        &mut self,
    ) -> Either<&mut leaf::LeafNode<Data>, &mut branching::BranchingNode<Data>> {
        match unsafe { self.ptr.as_ref() }.kind() {
            header::NodeKind::Leaf => Either::Left(unsafe { std::mem::transmute(self) }),
            header::NodeKind::Branching => Either::Right(unsafe { std::mem::transmute(self) }),
        }
    }

    pub fn is_leaf(&self) -> bool {
        let header = unsafe { self.ptr.as_ref() };
        todo!()

        // todo! use msb of len_and_type to determine if leaf or branching
    }

    pub fn is_branching(&self) -> bool {
        todo!()
    }

    /// Finds the parent of the node matching the given key, along with the index
    /// of that node in the list of chilren of the parent node.
    /// todo naming
    pub fn find_node_parent_mut(
        &mut self,
        key: &[c_char],
    ) -> (&mut Node<Data>, Result<usize, usize>, &[c_char]) {
        // if self is a leaf, then return None

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

        todo!()
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

#[repr(u8)]
enum NodeKind {
    Leaf = 0,
    Branching = 1,
}
