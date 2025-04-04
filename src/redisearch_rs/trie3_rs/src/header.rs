use crate::layout::NodeLayout;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
/// The first field in the allocated buffer for a [`Node`].
///
/// It is the only field we know the size of at compile-time.
pub struct NodeHeader {
    /// The length of the label associated with this node.
    ///
    /// It can be 0, for the root node.
    pub label_len: u16,
    /// The number of children of this node.
    pub n_children: u8,
}

impl NodeHeader {
    pub fn layout<Data>(self) -> NodeLayout<Data> {
        NodeLayout::<Data>::compute(self)
    }
}
