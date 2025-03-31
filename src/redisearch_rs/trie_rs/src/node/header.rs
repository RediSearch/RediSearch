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
    flag_drop_state: u1,

    /// indicates if there is an empty labeled child
    flag_has_empty_labeled_child: u1,

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
}

impl From<NodeDropState> for u1 {
    fn from(v: NodeDropState) -> Self {
        match v {
            NodeDropState::DropRecursive => DROP_RECURSIVE,
            NodeDropState::DropShallow => DROP_SHALLOW,
        }
    }
}

const LEAF: u1 = u1::new(0b0);
const BRANCHING: u1 = u1::new(0b1);

const DROP_SHALLOW: u1 = u1::new(0b0);
const DROP_RECURSIVE: u1 = u1::new(0b1);

pub(crate) enum NodeHasEmptyLabeledChild {
    NoEmptyLabeledChild,
    HasEmptyLabeledChild,
}

impl From<NodeHasEmptyLabeledChild> for u1 {
    fn from(v: NodeHasEmptyLabeledChild) -> Self {
        match v {
            NodeHasEmptyLabeledChild::NoEmptyLabeledChild => NO_EMPTY_LABELED_CHILD,
            NodeHasEmptyLabeledChild::HasEmptyLabeledChild => HAS_EMPTY_LABELED_CHILD,
        }
    }
}

const NO_EMPTY_LABELED_CHILD: u1 = u1::new(0b0);
const HAS_EMPTY_LABELED_CHILD: u1 = u1::new(0b1);

impl AllocationHeader {
    /// Create the header for a new leaf node.
    pub fn leaf(label_len: u16) -> Self {
        let label_len = label_len.try_into().expect("The length of the label exceeds the maximum capacity of a single node");
        Self::new(label_len, LEAF, DROP_RECURSIVE, NO_EMPTY_LABELED_CHILD)
    }

    /// Create the header for a new branching node.
    pub fn branching(label_len: u16) -> Self {
        let label_len = label_len.try_into().expect("The length of the label exceeds the maximum capacity of a single node");
        Self::new(label_len, BRANCHING, DROP_RECURSIVE, NO_EMPTY_LABELED_CHILD)
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
        } else {
            NodeDropState::DropShallow
        } 
    }

    pub fn set_drop_state(&mut self, new_state: NodeDropState) {
        self.set_flag_drop_state(new_state.into());
    }

    pub fn has_empty_labeled_child(&self) -> bool {
        self.flag_has_empty_labeled_child() == HAS_EMPTY_LABELED_CHILD
    }

    pub fn mark_as_empty_labeled_child(&mut self, marked: bool) {
        if marked {
            self.set_flag_has_empty_labeled_child(HAS_EMPTY_LABELED_CHILD);
        } else {
            self.set_flag_has_empty_labeled_child(NO_EMPTY_LABELED_CHILD);
        }
    }

    /// The length of the label associated with this node.
    pub fn len(&self) -> u16 {
        self.label_len().into()
    }
}

