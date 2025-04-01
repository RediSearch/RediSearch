use super::Node;
use super::branching::BranchingNode;
use super::header::AllocationHeader;
use std::alloc::*;
use std::ffi::c_char;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;

#[repr(transparent)]
pub(crate) struct LeafNode<Data>(Node<Data>);

impl<Data> From<LeafNode<Data>> for Node<Data> {
    fn from(v: LeafNode<Data>) -> Self {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe {
            std::mem::transmute(v)
        }
    }
}

impl<Data> AsRef<Node<Data>> for LeafNode<Data> {
    fn as_ref(&self) -> &Node<Data> {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe { std::mem::transmute(self) }
    }
}

impl<Data> AsMut<Node<Data>> for LeafNode<Data> {
    fn as_mut(&mut self) -> &mut Node<Data> {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe { std::mem::transmute(self) }
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


    fn header_mut(&mut self) -> &mut AllocationHeader {
        unsafe { self.0.ptr.as_mut() }
    }

    fn layout(&self) -> LeafLayout<Data> {
        LeafLayout::new(self.label_len())
    }

    pub(crate) fn label_len(&self) -> u16 {
        self.header().len()
    }

    pub(crate) fn label(&self) -> &[c_char] {
        let label_ptr = self.layout().label_ptr(self.0.ptr);
        unsafe { std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize) }
    }

    pub(crate) fn data(&self) -> &Data {
        let data_ptr = self.layout().data_ptr(self.0.ptr);
        unsafe { data_ptr.as_ref() }
    }

    pub(crate) fn data_mut(&mut self) -> &mut Data {
        let mut data_ptr = self.layout().data_ptr(self.0.ptr);
        unsafe { data_ptr.as_mut() }
    }

    /// Adds a child to the leaf node
    ///
    /// - label: the postfix label o of the child
    /// - data: the data of the child
    ///
    /// Preconditions:
    /// - self is leaf (case 1)
    /// - child_label cannot be empty (handled in case 1.1)
    ///
    /// Safety:
    /// Changes self to branching node --> the caller needs
    /// to be aware of that and ensure that it's reference is not
    /// used a a leaf node.
    ///
    /// Returns the newly crated branching node
    pub(crate) unsafe fn add_child(&mut self, child_label: &[c_char], child_data: Data) {
        debug_assert!(
            !child_label.is_empty(),
            "Child cannot have an empty label. (case 1.1)"
        );
        debug_assert!(
            !self.label().is_empty(),
            "The parent node cannot have an empty label, otherwise we should \
            be growing the parent branching node instead"
        );

    

        // only one case, caller needs to check if this.value needs replacement

        //let label = unsafe { std::slice::from_raw_parts(self.layout().label_ptr(self.0.ptr), self.label_len() as usize) };
        let (label_ref, data) = self.parts();
        let adapt_child_that_was_there = LeafNode::new(data, &[]);
        // unsafe access of label:
        let new_child = LeafNode::new(child_data, child_label);
        // Safety: We ensured the right order of the data
        
        let branching = unsafe {
            BranchingNode::allocate(
                &label_ref,
                &[],
                Some(new_child.into()),
                &[adapt_child_that_was_there.into()],
            )
        };
        
        BranchingNode::swap_ensure_shallow_del(branching, &mut self.0);
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
            std::ptr::copy_nonoverlapping(
                label.as_ptr(),
                layout.label_ptr(buffer_ptr).as_ptr(),
                label.len(),
            );
        }
        unsafe {
            layout.data_ptr(buffer_ptr).write(data);
        }
        buffer_ptr
    }

    fn into_data(self) -> Data {
        unsafe { self.layout().data_ptr(self.0.ptr).as_ptr().read() }
        
        // todo make sure data doesn't get dropped
    }

    fn parts(&mut self) -> (&[c_char], Data) {
        self.header_mut().set_drop_state(super::header::NodeDropState::DropShallow);
        let data = unsafe { self.layout().data_ptr(self.0.ptr).as_ptr().read() };
        (self.label(), data)
    }
}

impl<Data> Drop for LeafNode<Data> {
    #[inline]
    fn drop(&mut self) {
        // todo: check if we need to drop shallow

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
    _phantom: PhantomData<Data>,
}

impl<Data> LeafLayout<Data> {
    /// Gets the layout of the allocated memory for a [`LeafNode`] with the given capacity.
    pub const fn new(label_len: u16) -> Self {
        let layout = Layout::new::<AllocationHeader>();
        let Ok(c_char_array) = Layout::array::<c_char>(label_len as usize) else {
            panic!("Boom")
        };
        let Ok((layout, label_offset)) = layout.extend(c_char_array) else {
            unreachable!()
        };
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
            _phantom: PhantomData,
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
