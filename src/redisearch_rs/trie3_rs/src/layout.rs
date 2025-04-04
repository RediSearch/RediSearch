use std::{alloc::Layout, ffi::c_char, marker::PhantomData, ptr::NonNull};

use crate::{header::NodeHeader, node::Node};

/// Details on the layout of the buffer allocated for a [`Node`].
///
/// Layout structure:
///
/// ```text
/// NodeHeader         | u16
///                    | u8
/// Label              | [c_char; label_len]
/// Children 1st bytes | [c_char; n_children]
/// (Padding)          | [u8; (2 + 1 + label_len + n_children) % 8]
/// Children pointers  | [NonNull<Node>; numChildren]
/// (Padding)          | [u8; number depends on the alignment of value]
/// Value              | Option<Data>
/// ```
pub(super) struct NodeLayout<Data> {
    /// The size and alignment of the allocated buffer.
    layout: Layout,
    /// The offset (in bytes) of the children first-bytes array,
    /// relative to the beginning of the allocated buffer.
    children_first_bytes_offset: usize,
    /// The offset (in bytes) of the children pointer array,
    /// relative to the beginning of the allocated buffer.
    children_offset: usize,
    /// The offset (in bytes) of the node value,
    /// relative to the beginning of the allocated buffer.
    value_offset: usize,
    _phantom: PhantomData<Data>,
}

impl<Data> NodeLayout<Data> {
    /// The offset (in bytes) of the label associated with this node,
    /// relative to the beginning of the allocated buffer.
    const LABEL_OFFSET: usize = const {
        let layout = Layout::new::<NodeHeader>();

        // The offset doesn't depend on the actual number of children.
        let Ok(label) = Layout::array::<c_char>(1) else {
            unreachable!()
        };
        let Ok((_, offset)) = layout.extend(label) else {
            unreachable!()
        };
        offset
    };

    /// Compute the layout of a node, given its header.
    pub const fn compute(header: NodeHeader) -> Self {
        let layout = Layout::new::<NodeHeader>();

        // Node label
        let Ok(label) = Layout::array::<c_char>(header.label_len as usize) else {
            unreachable!()
        };
        let Ok((layout, _)) = layout.extend(label) else {
            unreachable!()
        };

        // Children first-bytes
        let Ok(first_bytes) = Layout::array::<c_char>(header.n_children as usize) else {
            unreachable!()
        };
        let Ok((layout, children_first_bytes_offset)) = layout.extend(first_bytes) else {
            unreachable!()
        };

        // Children pointers
        let Ok(children_pointers) = Layout::array::<Node<Data>>(header.n_children as usize) else {
            unreachable!()
        };
        let Ok((layout, children_offset)) = layout.extend(children_pointers) else {
            unreachable!()
        };

        // Value
        let Ok((layout, value_offset)) = layout.extend(Layout::new::<Option<Data>>()) else {
            panic!("Capacity overflow when adding the node value field to the layout of the node");
        };

        Self {
            layout: layout.pad_to_align(),
            children_first_bytes_offset,
            children_offset,
            value_offset,
            _phantom: PhantomData,
        }
    }

    /// The size and alignment of the allocated buffer for this node.
    pub const fn layout(&self) -> Layout {
        self.layout
    }

    /// A pointer to the first element of the child first-bytes array for this node.
    pub const fn child_first_bytes_ptr(&self, header_ptr: NonNull<NodeHeader>) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(self.children_first_bytes_offset as isize) }.cast()
    }

    /// A pointer to the first element of the child pointer array for this node.
    pub const fn children_ptr(&self, header_ptr: NonNull<NodeHeader>) -> NonNull<Node<Data>> {
        unsafe { header_ptr.byte_offset(self.children_offset as isize) }.cast()
    }

    /// A pointer to label associated with this node.
    pub const fn label_ptr(header_ptr: NonNull<NodeHeader>) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(Self::LABEL_OFFSET as isize) }.cast()
    }

    /// A pointer to value stored in this node.
    pub const fn value_ptr(&self, header_ptr: NonNull<NodeHeader>) -> NonNull<Option<Data>> {
        unsafe { header_ptr.byte_offset(self.value_offset as isize) }.cast()
    }
}
