use super::leaf::LeafNode;
use super::Node;
use super::header::AllocationHeader;
use std::alloc::*;
use std::ffi::c_char;
use std::marker::PhantomData;
use std::ptr::NonNull;

#[repr(transparent)]
pub(crate) struct BranchingNode<Data>(Node<Data>);

impl<Data> From<BranchingNode<Data>> for Node<Data> {
    fn from(v: BranchingNode<Data>) -> Self {
        // SAFETY: All good, repr(transparent) to the rescue.
        unsafe {
            std::mem::transmute(v)
        }
    }
}

impl<Data> BranchingNode<Data> {
    // todo: open to discuss, do we always know all children or do we need a form of "not yet initalized"?
    // and is slice the way to go?
    pub fn new(label: &[c_char], children: &[&Node<Data>]) -> Self {
        let reval = Self::new_impl(label, children.len());

        // copy children and set first bytes
        for (idx, child) in children.iter().enumerate() {
            reval.children_first_bytes()[idx] = child.label()[0];
            
            // use memcpy unsafe
            // reval.children()[idx] = child;
        }
        
        reval
    }

    pub fn new_for_grow(label: &[c_char], left: &[Node<Data>], new_node: Node<Data>, right: &[Node<Data>]) {
        let reval = Self::new_impl(label, children.len());

        // tod
        //let it = left.

        // copy children and set first bytes
        for (idx, child) in children.iter().enumerate() {
            reval.children_first_bytes()[idx] = child.label()[0];
            reval.children()[idx] = child;
        }

        reval
    }

    fn new_impl(label: &[c_char], num_children: u8) -> Self {
        Self(Node {
            ptr: Self::allocate(label, children),
            _phantom: PhantomData,
        })
    }

    pub fn shrink(&mut self, child_index: usize) -> Node<Data> {
        todo!("replace self with a smaller branching node")
    } 

    fn header(&self) -> &AllocationHeader {
        unsafe { self.0.ptr.as_ref() }
    }

    fn layout(&self) -> BranchLayout<Data> {
        BranchLayout::new(self.label_len(), self.num_children())
    }

    pub fn label_len(&self) -> u16 {
        self.header().len()
    }

    pub fn num_children(&self) -> u8 {
        unsafe {*self.layout().num_children_ptr(self.0.ptr).as_ref() }
    }

    pub fn label(&self) -> &[c_char] {
        let label_ptr = self.layout().label_ptr(self.0.ptr);
        unsafe { std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize) }
    }
    
    pub fn children_first_bytes(&self) -> &[c_char] {
        let children_first_bytes_ptr = self.layout().child_first_bytes_ptr(self.0.ptr);
        unsafe {std::slice::from_raw_parts(children_first_bytes_ptr.as_ptr(), self.num_children() as usize)}
    }

    pub fn children(&self) -> &[NonNull<Node<Data>>] {
        let children = self.layout().children_ptr(self.0.ptr);
        unsafe {std::slice::from_raw_parts(children.as_ptr(), self.num_children() as usize)}
    }

    /// grows the branching node by one element
    ///
    fn grow(self, new_child: Node<Data>) -> Self {
        if new_child.label().is_empty() { todo!() }

        // find right place for new_child
        let insert_index = match self.children_first_bytes().binary_search(&new_child.label()[0]) {
            Ok(_found) => unreachable!("we assume that we call grow for first level children and that this child does not exist"),
            Err(idx) => idx
        };

        // copy old childs and place new child
        let left_slice = todo!();
        let right_slice = todo!();

        // allocate branching node with num_children+1
        let new_branch = Self::new_for_grow(label, left_slice, new_child, right_slice);

        // replace self with new branching node
        // set old parent's numChilren to 0
        // drop old parent
        todo!("return new parent")
        }
    }


    fn into_parts(self) -> (Box<[c_char]>, Box<[Node<Data>]>){
        let label = self.label().to_vec().into_boxed_slice();
        let children = unsafe {
            Box::from_raw(self.layout().children_ptr(self.0.ptr).as_ptr())
        };
        self.num_children = 0;
        (label, children)
    }


    /// removes the children at the given index
    ///
    /// Moves out the Some(Node) or None if the index is out of bouce
    /// 
    fn remove(&mut self, idx: usize) -> Option<NonNull<Node<Data>>> {
        if idx >= self.num_children() as usize {
            return None;
        }

        let to_remove = self.children()[idx];

        // todo: replace self, better at higher level? I think C defered deletion in some case?

        Some(to_remove)
    }

    /// # Panics
    ///
    /// Panics if the label length is greater than u15::MAX.
    /// 
    /// # Safety
    /// - The caller must ensure that the children are not dropped.
    unsafe fn allocate(label: &[c_char], children: &[&Node<Data>]) -> NonNull<AllocationHeader> {
        // TODO: Check *here* that we're smaller than u15::MAX.
        let label_len: u16 = label.len().try_into().unwrap();
        let layout = BranchLayout::<Data>::new(label_len, children.len() as u8);
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
            buffer_ptr.write(AllocationHeader::branching(label_len));
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                label.as_ptr(),
                layout.label_ptr(buffer_ptr).as_ptr(),
                label.len(),
            );
        }


        /*
            I just realized that with our two types of nodes we run into the following problem:

            1. We added empty labels: ""
            2. Therefore we have to adapt the first_bytes representation to handle the empty case
            3. I think we don't have space for it, because of the border-case of a branching node with 256 children

            We may find a "magic encoding" but only if not all 256 bytes are used.

            Rare edge case but still.
         */
        let vec: Vec<Option<i8>> = children.iter().map(|e| match e.cast_ref() {
            super::Either::Left(leaf) => leaf.label().iter().copied().next(),
            super::Either::Right(node) => node.label().iter().copied().next(),
        }).collect();
 
        for (idx, opt_i8) in children.iter().map(|e| e.label().iter()).copied().enumurate().next() {
        }

        std::mem::forget(children);// do not drop the children cause they have been moved into the new allocation

        // todo: how do we handle the "option"
        let first_bytes_ptr = todo!();

        unsafe {
            std::ptr::copy_nonoverlapping(
                first_bytes_ptr,
                layout.child_first_bytes_ptr(buffer_ptr).as_ptr(),
                children.len(),
            );
        }

        buffer_ptr
    }
}

impl<Data> Drop for BranchingNode<Data> {
    #[inline]
    fn drop(&mut self) {
        // 1. iterate over children
        //   drop each chi
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
    num_children_offset: usize,
    
    /// The offset at which the label is located within the allocated block.
    label_offset: usize,

    /// The offet at which the first bytes of the the children's labels is located within the allocated block.
    children_first_bytes_offset: usize,

    /// The offset at which the child node pointers are located within the allocated block.
    children_offset: usize,
    
    _phantom: PhantomData<Data>,
}

impl<Data> BranchLayout<Data> {
    /// Gets the layout of the allocated memory for a [`LeafNode`] with the given capacity.
    pub const fn new(label_len: u16, num_children: u8) -> Self {
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
            num_children_offset,
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

    pub const fn num_children_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<u8> {
        unsafe { header_ptr.byte_offset(self.num_children_offset as isize) }.cast()
    }

    pub const fn child_first_bytes_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(self.children_first_bytes_offset as isize) }.cast()
    }

    pub const fn children_ptr(
        &self,
        header_ptr: NonNull<AllocationHeader>,
    ) -> NonNull<NonNull<Node<Data>>> {
        unsafe { header_ptr.byte_offset(self.children_offset as isize) }.cast()
    }

    pub const fn label_ptr(&self, header_ptr: NonNull<AllocationHeader>) -> NonNull<c_char> {
        unsafe { header_ptr.byte_offset(self.label_offset as isize) }.cast()
    }
}

pub struct TempNodeParts<Data> {
    node: Node<Data>
}


pub struct BranchingNodeParts<Data> {
    children: Box<[Node<Data>]>,
    children_first_bytes: Box<[c_char]>,
}

impl<Data> BranchingNode<Data> {
    fn into_parts(self) -> BranchingNodeParts<Data> {
        let children = Box::new(self.children.clone());
        let chilren_first_bytes = Box::new(self.children_first_bytes.clone());
        self.set_num_children(0);
        BranchingNodeParts { children, children_first_bytes }
    }

    fn children() -> &[Node<Data>] {

    }
}

impl<Data> std::mem::Drop for BranchLayout<Data> {
    fn drop(&mut self) {
        
    }
}