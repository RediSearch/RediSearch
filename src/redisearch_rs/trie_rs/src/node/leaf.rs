use super::branching::BranchingNode;
use super::Node;
use std::ptr::NonNull;
use std::alloc::*;
use std::ffi::c_char;
use std::marker::PhantomData;
use super::header::AllocationHeader;

#[repr(transparent)]
pub(crate) struct LeafNode<Data>(Node<Data>);

impl<Data> From<LeafNode<Data>> for Node<Data> {
    fn from(v: LeafNode<Data>) -> Self {
        /// SAFETY: All good, repr(transparent) to the rescue.
        unsafe { std::mem::transmute(v) }
    }
}

impl<Data> LeafNode<Data> {
    pub(crate) fn new(data: Data, label: &[c_char]) -> Self {
        Self(Node {
            ptr: Self::allocate(data, label),
            _phantom: PhantomData,
        })
    }

    fn header(&self) -> &AllocationHeader {
        unsafe { self.0.ptr.as_ref() }
    }

    fn layout(&self) -> LeafLayout<Data> {
        LeafLayout::new(self.label_len())
    }

    pub(crate) fn label_len(&self) -> u16 {
        self.header().len()
    }

    pub(crate) fn label(&self) -> &[c_char] {
        let label_ptr = self.layout().label_ptr(self.0.ptr);
        unsafe {
            std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize)
        }
    }

    pub(crate) fn data(&self) -> &Data {
        let data_ptr = self.layout().data_ptr(self.0.ptr);
        unsafe {
            data_ptr.as_ref()
        }
    }

    pub(crate) fn data_mut(&mut self) -> &mut Data {
        let mut data_ptr = self.layout().data_ptr(self.0.ptr);
        unsafe {
            data_ptr.as_mut()
        }
    }

    /// Adds a child to the leaf node
    /// 
    /// - label: the postfix label o of the child
    /// - data: the data of the child
    /// 
    /// Safety:
    /// Changes self to branching node --> the caller needs
    /// to be aware of that and ensure that it's reference is not
    /// used a a leaf node.
    /// 
    /// Returns the newly crated branching node
    pub(crate) unsafe fn add_child(
        &mut self,
        label: &[c_char],
        child_data: Data) 
    {
        // only one case, caller needs to check if this.value needs replacement
        let child2 = LeafNode::new(child_data, label);
        let new_branch = BranchingNode::new_binary_branch(label, &self.0, &child2.0);

        unsafe {
            BranchingNode::swap_ensure_shallow_del(new_branch, &mut self.0);
        };
    }

    /// # Panics
    ///
    /// Panics if the label length is greater than u15::MAX.
    fn allocate(data: Data, label: &[c_char]) -> NonNull<AllocationHeader> {
        // TODO: Check *here* that we're smaller than u15::MAX.
        let label_len: u16 = label.len().try_into().unwrap();
        let layout = LeafLayout::<Data>::new(label_len);
        let buffer_ptr = {
            debug_assert!(layout.layout.size() > 0);
            // SAFETY:
            // `layout.size()` is greater than zero, since the header has a well-known
            // non-zero size (2 bytes).
            unsafe { alloc(layout.layout) as *mut AllocationHeader }
        };
    
        let Some(buffer_ptr) = NonNull::new(buffer_ptr) else {
            // The allocation failed!
            handle_alloc_error(layout.layout)
        };

        // Initialize the allocated buffer with valid values.

        // SAFETY:
        // - The destination and the value are properly aligned,
        //   since the allocation was performed against a type layout
        //   that begins with a header field.
        unsafe {
            buffer_ptr.write(AllocationHeader::leaf(label_len));
        }

        unsafe {
            std::ptr::copy_nonoverlapping(label.as_ptr(), layout.label_ptr(buffer_ptr).as_ptr(), label.len());
        }
        unsafe {
            layout.data_ptr(buffer_ptr).write(data);
        }
        buffer_ptr
    }
}

impl<Data> Drop for LeafNode<Data> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.data_mut());
        }
        unsafe {
            dealloc(self.0.ptr.as_ptr() as *mut u8, self.layout().layout);
        }
    }
}

/// Information about the layout of the buffer allocated for a leaf node.
struct LeafLayout<Data> {
    /// The layout of the allocated buffer.
    layout: Layout,
    /// The offset at which the label is located within the allocated block.
    label_offset: usize,
    /// The offset at which the data is located within the allocated block.
    data_offset: usize,
    _phantom: PhantomData<Data>
}

impl<Data> LeafLayout<Data> {
    /// Gets the layout of the allocated memory for a [`LeafNode`] with the given capacity.
    pub const fn new(label_len: u16) -> Self {
        let layout = Layout::new::<AllocationHeader>();
        let Ok(c_char_array) = Layout::array::<c_char>(label_len as usize) else {
            panic!("Boom")
        };
        let Ok((layout, label_offset)) = layout.extend(c_char_array) else { unreachable!() };
        let Ok((layout, data_offset)) = layout.extend(Layout::new::<Data>()) else {
            // The panic message must be known at compile-time if we want `allocation_layout` to be a `const fn`.
            // Therefore we can't capture the error (nor the faulty capacity value) in the panic message.
            panic!(
                "The size of the allocated buffer for a leaf node would exceed `isize::MAX`, \
                which is the maximum size that can be allocated."
            )
        };
        Self {
            layout: layout.pad_to_align(),
            data_offset,
            label_offset,
            _phantom: PhantomData
        }
    }

    /// Gets the alignment for the allocation owned by a [`LeafNode`].
    pub const fn alignment() -> usize {
        Self::new(1).layout.align()
    }

    pub const fn data_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<Data> {
        unsafe { header_ptr.byte_offset(self.data_offset as isize) }.cast()
    }

    pub const fn label_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(self.label_offset as isize) }.cast()
    }
}
