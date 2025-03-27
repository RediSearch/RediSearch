use bilge::prelude::*;


#[bitsize(24)]
#[repr(C)]
pub(crate) struct AllocationHeader {
    /// The length of the label stored inside this node.
    label_len: u16,
    
    /// The kind of node you are working with—either a leaf
    /// or a branching node.
    flag_terminal_or_branching: u1,

    /// used by internal unsafe code to determine the correct drop impl, see [NodeDropState]
    flag_drop_state: u2,

    reserved: u5,

}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Leaf,
    Branching,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeDropState {
    DropRecursive,
    DropShallow,
    DropSentinel,
}

const LEAF: u1 = u1::new(0b0);
const BRANCHING: u1 = u1::new(0b1);

const DROP_SHALLOW: u2 = u2::new(0b00);
const DROP_RECURSIVE: u2 = u2::new(0b01);
const DROP_SENTINEL: u2 = u2::new(0b10);

impl AllocationHeader {
    /// Create the header for a new leaf node.
    pub fn leaf(label_len: u16) -> Self {
        let label_len = label_len.try_into().expect("The length of the label exceeds the maximum capacity of a single node");
        Self::new(label_len, LEAF, DROP_RECURSIVE)
    }

    /// Create the header for a new branching node.
    pub fn branching(label_len: u16) -> Self {
        let label_len = label_len.try_into().expect("The length of the label exceeds the maximum capacity of a single node");
        Self::new(label_len, BRANCHING, DROP_RECURSIVE)
    }

    pub const fn sentinel() -> Self {
        //Self::new(0.try_into().expect("We know that zero is in rage"), LEAF, DROP_SENTINEL)
        Self {
            value: u24::new(0b_0000_0000_0000_0000_0100_0000),
        }
    }

    /// The node kind, as an enum.
    pub fn kind(&self) -> NodeKind {
        if self.flag_terminal_or_branching() == BRANCHING {
            NodeKind::Branching
        } else {
            NodeKind::Leaf
        }
    }

    /// The current drop state
    pub fn drop_state(&self) -> NodeDropState {
        if self.flag_drop_state() == DROP_RECURSIVE {
            NodeDropState::DropRecursive
        } else if self.flag_drop_state() == DROP_SHALLOW {
            NodeDropState::DropShallow
        } else {
            NodeDropState::DropSentinel
        }
    }

    pub fn set_drop_state(&mut self, new_state: NodeDropState) {
        self.set_drop_state(new_state);
    }

    /// The length of the label associated with this node.
    pub fn len(&self) -> u16 {
        self.label_len().into()
    }
}
