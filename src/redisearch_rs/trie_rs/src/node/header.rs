use bilge::prelude::*;

#[bitsize(16)]
#[repr(C)]
pub(crate) struct AllocationHeader {
    /// The length of the label stored inside this node.
    label_len: u15,
    /// The kind of node you are working with—either a leaf
    /// or a branching node.
    kind_: u1,
}

#[repr(C)]
pub(crate) struct NewAllocationHeader {
    /// The length of the label stored inside this node.
    label_len: u16,
    /// The kind of node you are working with—either a leaf
    /// or a branching node.
    flag: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Leaf,
    Branching,
}

const LEAF: u1 = u1::new(0b0);
const BRANCHING: u1 = u1::new(0b1);

impl AllocationHeader {
    /// Create the header for a new leaf node.
    pub fn leaf(label_len: u16) -> Self {
        let label_len = label_len.try_into().expect("The length of the label exceeds the maximum capacity of a single node");
        Self::new(label_len, LEAF)
    }

    /// Create the header for a new branching node.
    pub fn branching(label_len: u16) -> Self {
        let label_len = label_len.try_into().expect("The length of the label exceeds the maximum capacity of a single node");
        Self::new(label_len, BRANCHING)
    }

    /// The node kind, as an enum.
    pub fn kind(&self) -> NodeKind {
        if self.kind_() == BRANCHING {
            NodeKind::Branching
        } else {
            NodeKind::Leaf
        }
    }

    /// The length of the label associated with this node.
    pub fn len(&self) -> u16 {
        self.label_len().into()
    }
}