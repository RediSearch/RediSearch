use crate::node::header::NodeDropState;

use super::header::AllocationHeader;
use super::leaf::LeafNode;
use super::{Either, Node};
use std::alloc::*;
use std::ffi::c_char;
use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
pub(crate) struct BranchingNode<Data>(Node<Data>);

impl<Data> From<BranchingNode<Data>> for Node<Data> {
    fn from(v: BranchingNode<Data>) -> Self {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe { std::mem::transmute(v) }
    }
}

impl<Data> BranchingNode<Data> {
    pub fn shrink(&mut self, child_index: usize) -> Node<Data> {
        todo!("replace self with a smaller branching node")
    }

    fn header(&self) -> &AllocationHeader {
        unsafe { self.0.ptr.as_ref() }
    }

    fn header_mut(&mut self) -> &mut AllocationHeader {
        unsafe { self.0.ptr.as_mut() }
    }

    fn layout(&self) -> BranchLayout<Data> {
        BranchLayout::new(self.label_len(), self.num_children())
    }

    /// Get the length of the label
    pub fn label_len(&self) -> u16 {
        self.header().len()
    }

    /// Returns the number of children, including the empty-labeled child if it exists.
    pub fn num_children(&self) -> u16 {
        unsafe {
            *StaticBranchLayout::new(self.label_len())
                .num_non_empty_labeled_children_ptr(self.0.ptr)
                .as_ref() as u16
                + self.has_empty_labeled_child() as u16
        }
    }

    /// Check whether the branching node has an empty-labeled child.
    fn has_empty_labeled_child(&self) -> bool {
        self.header().has_empty_labeled_child()
    }

    pub fn label(&self) -> &[c_char] {
        let label_ptr = self.layout().label_ptr(self.0.ptr);
        unsafe { std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize) }
    }

    /// Gets the first bytes of the labels of the non-empty-labeled children.
    pub fn children_first_bytes(&self) -> &[c_char] {
        let children_first_bytes_ptr = self.layout().child_first_bytes_ptr(self.0.ptr);
        unsafe {
            std::slice::from_raw_parts(
                children_first_bytes_ptr.as_ptr(),
                self.num_children() as usize,
            )
        }
    }

    /// Get the children of the branching node.
    /// The empty-labeled child, if any, is located at the end of the slice.
    pub fn children(&self) -> &[Node<Data>] {
        let children = self.layout().children_ptr(self.0.ptr);
        unsafe {
            std::slice::from_raw_parts(children.as_ptr() as *const _, self.num_children() as usize)
        }
    }

    /// Get mutable access to the children of the branching node.
    /// The empty-labeled child, if any, is located at the end of the slice.
    pub fn children_mut(&mut self) -> &mut [Node<Data>] {
        let children = self.layout().children_ptr(self.0.ptr);
        unsafe { std::slice::from_raw_parts_mut(children.as_ptr(), self.num_children() as usize) }
    }

    /// grows the branching node by one element
    pub fn grow(
        &mut self,
        child_label: &[c_char],
        child_data: Data,
        found_or_insert_at: Result<usize, usize>,
    ) -> Option<Data> {
        if child_label.is_empty() {
            // the child exists and it is a branching node
            if self.has_empty_labeled_child() {
                // we replace its data because that's always a leaf
                let Either::Left(leaf) = self.children_mut().last_mut().unwrap().cast_mut() else {
                    unreachable!(
                        "The empty labeled  child must be a leaf, otherwise it should have been merged"
                    );
                };
                let old_data = std::mem::replace(leaf.data_mut(), child_data);

                return Some(old_data);
            } else {
                let new_child: LeafNode<Data> = LeafNode::new(child_data, child_label);
                let self_children = self.children();

                // Safety: pinky promise we're gonna mark the old node as drop shallow
                let mut new_branch =
                    unsafe { Self::allocate(child_label, self_children, new_child.into(), &[]) };

                // ye olde switcheroo
                std::mem::swap(&mut new_branch, self);

                // Ensure the children that got moved to the newly created node are not dropped.
                new_branch
                    .header_mut()
                    .set_drop_state(NodeDropState::DropShallow);
            }

            return None;
        }

        match found_or_insert_at {
            Ok(child_index) => {
                let cur_child = &mut self.children_mut()[child_index];
                match cur_child.cast_mut() {
                    super::Either::Left(leaf) => {
                        // Replace the leaf's data.
                        let old_data = std::mem::replace(leaf.data_mut(), child_data);
                        Some(old_data)
                    }
                    super::Either::Right(brn) => {
                        // Add an empty-labled child to the branching node.
                        brn.grow(&[], child_data, found_or_insert_at);
                        None
                    }
                }
            }
            Err(insert_index) => {
                // copy old childs and place new child
                let left_slice = &self.children()[..insert_index];
                let right_slice = &self.children()[insert_index..];

                let shared_prefix_len = self
                    .label()
                    .iter()
                    .zip(child_label)
                    .enumerate()
                    .find(|(_, (c1, c2))| c1 != c2)
                    .map(|(i, _)| i)
                    .unwrap_or(0);

                // calculate label of the new branching node, which is the shared prefix
                let new_brnch_label = &self.label()[..shared_prefix_len];

                // The label of new_child is its original label stripped of the shared prefix
                let new_child_label = &child_label[shared_prefix_len..];
                // do somehting we don't want to reallocate the new_child
                let new_child: LeafNode<Data> = LeafNode::new(child_data, new_child_label);

                // allocate branching node with num_children+1
                let mut new_branch = unsafe {
                    Self::allocate(new_brnch_label, left_slice, new_child.into(), right_slice)
                };

                // swap with self
                std::mem::swap(self, &mut new_branch);

                // SAFETY:
                // ensure right drop implementation is used.
                // todo: wrap in zero
                unsafe {
                    new_branch
                        .0
                        .ptr
                        .as_mut()
                        .set_drop_state(NodeDropState::DropShallow);
                }
                None
            }
        }
    }

    /// removes the children at the given index
    ///
    /// Moves out the Some(Node) or None if the index is out of bouce
    ///
    fn remove(&mut self, idx: usize) -> Option<NonNull<Node<Data>>> {
        if idx >= self.num_children() as usize {
            return None;
        }

        let to_remove = &self.children()[idx];

        // todo: replace self, better at higher level? I think C defered deletion in some case?

        // Some(to_remove)
        todo!()
    }

    /// # Panics
    ///
    /// Panics if the label length is greater than u15::MAX.
    ///
    /// # Invariant
    /// - The empty-labeled child, if any, is the last element of the children slice.
    ///
    /// # Safety
    /// - The caller must ensure that the children are not dropped.
    unsafe fn allocate(
        label: &[c_char],
        left: &[Node<Data>],
        new_node: Node<Data>,
        right: &[Node<Data>],
    ) -> Self {
        // TODO: Check *here* that we're smaller than u15::MAX.
        let label_len: u16 = label.len().try_into().unwrap();

        let has_empty_labeled_child = right.last().map_or_else(
            || new_node.label().is_empty(),
            |right_last| right_last.label().is_empty(),
        );

        let num_children_total = left.len() + 1 + right.len();
        let num_non_empty_labeled_children = num_children_total - has_empty_labeled_child as usize;

        let layout = BranchLayout::<Data>::new(label_len, num_children_total as u16);
        let buffer_ptr = {
            debug_assert!(layout.layout.size() > 0);
            // SAFETY:
            // `layout.size()` is greater than zero, since the header has a well-known
            // non-zero size (2 bytes).
            unsafe { alloc(layout.layout) as *mut AllocationHeader }
        };

        let Some(mut buffer_ptr) = NonNull::new(buffer_ptr) else {
            // The allocation failed!
            handle_alloc_error(layout.layout)
        };
        // Initialize the allocated buffer with valid values.

        unsafe {
            buffer_ptr
                .as_mut()
                .mark_as_empty_labeled_child(has_empty_labeled_child);
        }

        // SAFETY:
        // - The destination and the value are properly aligned,
        //   since the allocation was performed against a type layout
        //   that begins with a header field.
        unsafe {
            buffer_ptr.write(AllocationHeader::branching(label_len));
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                label.as_ptr(),
                layout.label_ptr(buffer_ptr).as_ptr(),
                label.len(),
            );
        }

        // Set the number of children in the buffer.
        unsafe {
            layout
                .num_non_empty_labeled_children_ptr(buffer_ptr)
                .as_ptr()
                .write(num_non_empty_labeled_children as u8);
        }

        // Copy the first bytes of the non-empty-labeled children's labels.
        for (idx, child) in left
            .iter()
            .chain(std::iter::once(&new_node))
            .chain(right.iter())
            .enumerate()
            .take(num_non_empty_labeled_children)
        {
            if let Some(first_byte) = child.label().first() {
                unsafe {
                    layout
                        .child_first_bytes_ptr(buffer_ptr)
                        .as_ptr()
                        .add(idx)
                        .write(*first_byte)
                };
            } else {
                unreachable!(
                    "The empty labeled child, if any, should be at the end and if so,\
                it should be skipped due to num_child_first_bytes being 1 less than children.len()"
                );
            }
        }

        // memcpy the children, including the non-empty labeled child
        if !left.is_empty() {
            // copy the left children to children.add(offset)
            unsafe {
                std::ptr::copy_nonoverlapping(
                    left.as_ptr(),
                    layout.children_ptr(buffer_ptr).as_ptr(),
                    left.len(),
                );
            }
        }
        // copy new child to children.add(offset)
        let offset = left.len();
        unsafe {
            layout
                .children_ptr(buffer_ptr)
                .as_ptr()
                .add(offset)
                .write(new_node);
        }
        // Copy the right children to children.add(offset + 1)
        let offset = offset + 1;
        if !right.is_empty() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    right.as_ptr(),
                    layout.children_ptr(buffer_ptr).add(offset).as_ptr(),
                    right.len(),
                );
            }
        }

        BranchingNode(Node {
            ptr: buffer_ptr,
            _phantom: PhantomData,
        })
    }
}

impl<Data> Drop for BranchingNode<Data> {
    #[inline]
    fn drop(&mut self) {
        if self.header().drop_state() == NodeDropState::DropSentinel {
            return;
        }
        //~

        if self.header().drop_state() == NodeDropState::DropRecursive {
            todo!("Delete children")
        }

        unsafe {
            dealloc(self.0.ptr.as_ptr() as *mut u8, self.layout().layout);
        }
    }
}

/// Information about the layout of the buffer allocated for a leaf node.
///
/// Layout structure:
/// u16              (AllocationHeader)
/// u8               (numChildren)
/// [c_char; len]    (label)
/// [c_char; numChildren]  (first bytes of children's labels)
/// padding if sum % 8 == 0 { 0 } else { 8 - sum % 8 }
/// \[NonNull<Node>; numChildren]
///
struct BranchLayout<Data> {
    /// The layout of the allocated buffer.
    layout: Layout,

    /// The offset at which the number of children is located within the allocated block.
    num_non_empty_labeled_children_offset: usize,

    /// The offset at which the label is located within the allocated block.
    label_offset: usize,

    /// The offet at which the first bytes of the the children's labels is located within the allocated block.
    children_first_bytes_offset: usize,

    /// The offset at which the child node pointers are located within the allocated block.
    children_offset: usize,

    _phantom: PhantomData<Data>,
}

struct StaticBranchLayout {
    /// The layout of the allocated buffer.
    layout: Layout,

    /// The offset at which the number of children is located within the allocated block.
    num_children_offset: usize,

    /// The offset at which the label is located within the allocated block.
    label_offset: usize,
}

impl StaticBranchLayout {
    pub const fn new(label_len: u16) -> Self {
        let layout = Layout::new::<AllocationHeader>();

        // number of children
        let Ok((layout, num_children_offset)) = layout.extend(Layout::new::<u8>()) else {
            unreachable!()
        };

        // label part of array
        let Ok(c_char_array) = Layout::array::<c_char>(label_len as usize) else {
            panic!("Boom")
        };
        let Ok((layout, label_offset)) = layout.extend(c_char_array) else {
            unreachable!()
        };

        Self {
            layout: layout.pad_to_align(),
            num_children_offset,
            label_offset,
        }
    }

    pub const fn num_non_empty_labeled_children_ptr(
        &self,
        header_ptr: NonNull<AllocationHeader>,
    ) -> NonNull<u8> {
        unsafe { header_ptr.byte_offset(self.num_children_offset as isize) }.cast()
    }
}

impl<Data> BranchLayout<Data> {
    /// Gets the layout of the allocated memory for a [`LeafNode`] with the given capacity.
    /// num_children is u16, as it may have more than 255 children with the empty labeled children
    pub const fn new(label_len: u16, num_children: u16) -> Self {
        let layout = Layout::new::<AllocationHeader>();

        // number of children
        let Ok((layout, num_children_offset)) = layout.extend(Layout::new::<u8>()) else {
            unreachable!()
        };

        // label part of array
        let Ok(c_char_array) = Layout::array::<c_char>((label_len) as usize) else {
            panic!("Boom")
        };
        let Ok((layout, label_offset)) = layout.extend(c_char_array) else {
            unreachable!()
        };

        // first byte part of array
        let Ok(c_char_array) = Layout::array::<c_char>((label_len) as usize) else {
            panic!("Boom")
        };
        let Ok((layout, children_first_bytes_offset)) = layout.extend(c_char_array) else {
            unreachable!()
        };

        // array of children pointers
        let Ok(c_ptr_array) = Layout::array::<&mut Node<Data>>(num_children as usize) else {
            panic!("Boom")
        };
        let Ok((layout, children_offset)) = layout.extend(c_ptr_array) else {
            unreachable!()
        };

        Self {
            layout: layout.pad_to_align(),
            num_non_empty_labeled_children_offset: num_children_offset,
            children_first_bytes_offset,
            label_offset,
            children_offset,
            _phantom: PhantomData,
        }
    }

    /// Gets the alignment for the allocation owned by a [`LeafNode`].
    pub const fn alignment() -> usize {
        Self::new(1, 1).layout.align()
    }

    pub const fn num_non_empty_labeled_children_ptr(
        &self,
        header_ptr: NonNull<AllocationHeader>,
    ) -> NonNull<u8> {
        unsafe { header_ptr.byte_offset(self.num_non_empty_labeled_children_offset as isize) }
            .cast()
    }

    pub const fn child_first_bytes_ptr(
        &self,
        header_ptr: NonNull<AllocationHeader>,
    ) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(self.children_first_bytes_offset as isize) }.cast()
    }

    pub const fn children_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<Node<Data>> {
        unsafe { header_ptr.byte_offset(self.children_offset as isize) }.cast()
    }

    pub const fn label_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(self.label_offset as isize) }.cast()
    }
}
