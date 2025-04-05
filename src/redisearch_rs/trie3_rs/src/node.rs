use std::{
    alloc::{alloc, dealloc, handle_alloc_error},
    cmp::Ordering,
    ffi::c_char,
    fmt,
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, RangeFrom},
    ptr::NonNull,
    usize,
};

use crate::{
    header::NodeHeader,
    layout::NodeLayout,
    utils::{memchr_c_char, strip_prefix, to_string_lossy},
};

/// A node in a [`TrieMap`](crate::TrieMap).
pub struct Node<Data> {
    pub(crate) ptr: std::ptr::NonNull<NodeHeader>,
    pub(crate) _phantom: PhantomData<Data>,
}

impl<Data: Clone> Clone for Node<Data> {
    fn clone(&self) -> Self {
        // Get the layout for the node we're cloning
        let layout = self.layout();

        // Allocate a new buffer with the same layout
        let new_ptr = {
            // SAFETY: The layout size is greater than zero (at minimum it contains a header)
            unsafe { alloc(layout.layout()) as *mut NodeHeader }
        };

        let Some(new_ptr) = NonNull::new(new_ptr) else {
            // The allocation failed!
            handle_alloc_error(layout.layout())
        };

        // Copy the header
        unsafe {
            new_ptr.as_ptr().write(*self.header());
        }

        // Copy the label
        unsafe {
            std::ptr::copy_nonoverlapping(
                NodeLayout::<Data>::label_ptr(self.ptr).as_ptr(),
                NodeLayout::<Data>::label_ptr(new_ptr).as_ptr(),
                self.label_len() as usize,
            );
        }

        // Copy the children first bytes
        unsafe {
            std::ptr::copy_nonoverlapping(
                layout.child_first_bytes_ptr(self.ptr).as_ptr(),
                layout.child_first_bytes_ptr(new_ptr).as_ptr(),
                self.n_children() as usize,
            );
        }

        // Clone the children
        let mut next_child = layout.children_ptr(new_ptr).as_ptr();
        for child in self.children() {
            unsafe { next_child.write(child.clone()) };
            unsafe { next_child = next_child.add(1) };
        }

        // Clone the value if present
        unsafe {
            layout
                .value_ptr(new_ptr)
                .as_ptr()
                .write(self.data().cloned());
        }

        Self {
            ptr: new_ptr,
            _phantom: PhantomData,
        }
    }
}

/// Constructors.
impl<Data> Node<Data> {
    /// Create a new node without children.
    ///
    /// # Requirements
    ///
    /// To guarantee `Node`'s invariants, the caller must ensure that
    /// the length of the label fits in a `u16`.
    /// If the length of the label is longer than a `u16`, the exceeding portion
    /// will be silently ignored.
    pub(crate) fn new_leaf(label: &[c_char], value: Option<Data>) -> Self {
        // SAFETY:
        // There are no children, so the number of children fits within a u8.
        unsafe { Self::new_unchecked(label, [], value) }
    }

    /// # Requirements
    ///
    /// To guarantee `Node`'s invariants, the caller must ensure that:
    ///
    /// - The first byte of each child is unique within the provided children collection
    /// - All children have a non-empty label
    /// - The children iterator is sorted in ascending order
    /// - The length of the label fits in a `u16`
    ///
    /// # Violations
    ///
    /// If the length of the label is longer than a `u16`, the exceeding portion
    /// will be silently ignored.
    ///
    /// # Safety
    ///
    /// - The number of children fits in a `u8`
    pub(crate) unsafe fn new_unchecked<I>(
        label: &[c_char],
        children: I,
        value: Option<Data>,
    ) -> Self
    where
        I: IntoIterator<Item = Node<Data>>,
        I::IntoIter: ExactSizeIterator,
    {
        debug_assert!(
            label.len() <= u16::MAX as usize,
            "The label length exceeds u16::MAX, {}",
            u16::MAX
        );
        let children = children.into_iter();
        debug_assert!(
            children.len() <= u8::MAX as usize,
            "The number of children exceeds u8::MAX, {}",
            u8::MAX
        );
        let header = NodeHeader {
            // We don't support labels longer than u16::MAX.
            label_len: label.len() as u16,
            // A node can have at most 255 children, since that's
            // the number of unique `c_char` values.
            n_children: children.len() as u8,
        };
        let layout = header.layout();
        let buffer_ptr = {
            // SAFETY:
            // `layout.size()` is greater than zero, since the header has a well-known
            // non-zero size.
            unsafe { alloc(layout.layout()) as *mut NodeHeader }
        };

        let Some(buffer_ptr) = NonNull::new(buffer_ptr) else {
            // The allocation failed!
            handle_alloc_error(layout.layout())
        };
        // Initialize the allocated buffer with valid values.

        // SAFETY:
        // - The destination and the value are properly aligned,
        //   since the allocation was performed against a type layout
        //   that begins with a header field.
        unsafe {
            buffer_ptr.write(header);
        }

        // Initialize the node label.
        unsafe {
            std::ptr::copy_nonoverlapping(
                label.as_ptr(),
                NodeLayout::<Data>::label_ptr(buffer_ptr).as_ptr(),
                header.label_len as usize,
            );
        }

        // Initialize the two children-related arrays.
        let mut first_byte_array_ptr = layout.child_first_bytes_ptr(buffer_ptr).as_ptr();
        let mut children_array_ptr = layout.children_ptr(buffer_ptr).as_ptr();
        for child in children.into_iter() {
            // SAFETY:
            // We require the caller of this method to guarantee that child labels are non-empty,
            // thus it's safe to access the first element.
            let first_byte = unsafe { child.label().get_unchecked(0) };
            unsafe { first_byte_array_ptr.write(*first_byte) };
            first_byte_array_ptr = unsafe { first_byte_array_ptr.add(1) };

            unsafe { children_array_ptr.write(child) };
            children_array_ptr = unsafe { children_array_ptr.add(1) };
        }

        // Set the value.
        let value_ptr = layout.value_ptr(buffer_ptr).as_ptr();
        unsafe { value_ptr.write(value) };

        Self {
            ptr: buffer_ptr,
            _phantom: PhantomData,
        }
    }

    /// Allocate a buffer big enough to contain the specified label
    /// and number of children.
    ///
    /// Only the header and the label are initialized.
    ///
    /// # Safety
    ///
    /// - The caller must initialize the
    fn allocate_and_init_header(
        label: &[c_char],
        n_children: u8,
    ) -> (NonNull<NodeHeader>, NodeLayout<Data>) {
        debug_assert!(
            label.len() <= u16::MAX as usize,
            "The label length exceeds u16::MAX, {}",
            u16::MAX
        );
        let header = NodeHeader {
            // We don't support labels longer than u16::MAX.
            label_len: label.len() as u16,
            n_children,
        };
        let layout = header.layout();
        let buffer_ptr = {
            // SAFETY:
            // `layout.size()` is greater than zero, since the header has a well-known
            // non-zero size.
            unsafe { alloc(layout.layout()) as *mut NodeHeader }
        };

        let Some(buffer_ptr) = NonNull::new(buffer_ptr) else {
            // The allocation failed!
            handle_alloc_error(layout.layout())
        };
        // Initialize the allocated buffer with valid values.

        // SAFETY:
        // - The destination and the value are properly aligned,
        //   since the allocation was performed against a type layout
        //   that begins with a header field.
        unsafe {
            buffer_ptr.write(header);
        }

        // Initialize the node label.
        unsafe {
            std::ptr::copy_nonoverlapping(
                label.as_ptr(),
                NodeLayout::<Data>::label_ptr(buffer_ptr).as_ptr(),
                header.label_len as usize,
            );
        }

        (buffer_ptr, layout)
    }

    /// Set the node label to `old_label[offset..offset + new_length]`.
    pub fn relabel(mut self, offset: usize, new_length: usize) -> Self {
        let (new_ptr, new_layout) = Self::allocate_and_init_header(
            &self.label()[offset..offset + new_length],
            self.n_children(),
        );
        let old_layout = self.layout();

        unsafe {
            std::ptr::copy_nonoverlapping(
                old_layout.child_first_bytes_ptr(self.ptr).as_ptr(),
                new_layout.child_first_bytes_ptr(new_ptr).as_ptr(),
                self.n_children() as usize,
            );
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                old_layout.children_ptr(self.ptr).as_ptr(),
                new_layout.children_ptr(new_ptr).as_ptr(),
                self.n_children() as usize,
            );
        }

        unsafe {
            new_layout.value_ptr(new_ptr).write(self.data_mut().take());
        }

        unsafe {
            dealloc(self.ptr.as_ptr().cast(), old_layout.layout());
        }

        // We don't want to drop the children or a dangling pointer!
        std::mem::forget(self);

        Self {
            ptr: new_ptr,
            _phantom: Default::default(),
        }
    }

    /// Set the node label to `concat!(prefix, old_label)`.
    pub fn prepend(mut self, prefix: &[c_char]) -> Self {
        debug_assert!(
            self.label_len() as usize + prefix.len() <= u16::MAX as usize,
            "The new label length exceeds u16::MAX, {}",
            u16::MAX
        );
        let header = NodeHeader {
            label_len: self.label_len() + prefix.len() as u16,
            n_children: self.n_children(),
        };
        let new_layout = header.layout();
        let new_ptr = {
            // SAFETY:
            // `layout.size()` is greater than zero, since the header has a well-known
            // non-zero size.
            unsafe { alloc(new_layout.layout()) as *mut NodeHeader }
        };

        let Some(new_ptr) = NonNull::new(new_ptr) else {
            // The allocation failed!
            handle_alloc_error(new_layout.layout())
        };
        // Initialize the allocated buffer with valid values.

        // SAFETY:
        // - The destination and the value are properly aligned,
        //   since the allocation was performed against a type layout
        //   that begins with a header field.
        unsafe {
            new_ptr.write(header);
        }

        // Initialize the node label.
        {
            // First copy the prefix (the new beginning of the label)
            unsafe {
                std::ptr::copy_nonoverlapping(
                    prefix.as_ptr(),
                    NodeLayout::<Data>::label_ptr(new_ptr).as_ptr(),
                    prefix.len(),
                );
            }

            // Then copy the original label after the prefix
            unsafe {
                std::ptr::copy_nonoverlapping(
                    NodeLayout::<Data>::label_ptr(self.ptr).as_ptr(),
                    NodeLayout::<Data>::label_ptr(new_ptr)
                        .as_ptr()
                        .offset(prefix.len() as isize),
                    self.label_len() as usize,
                );
            }
        }
        let old_layout = self.layout();

        unsafe {
            std::ptr::copy_nonoverlapping(
                old_layout.child_first_bytes_ptr(self.ptr).as_ptr(),
                new_layout.child_first_bytes_ptr(new_ptr).as_ptr(),
                self.n_children() as usize,
            );
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                old_layout.children_ptr(self.ptr).as_ptr(),
                new_layout.children_ptr(new_ptr).as_ptr(),
                self.n_children() as usize,
            );
        }

        unsafe {
            new_layout.value_ptr(new_ptr).write(self.data_mut().take());
        }

        unsafe {
            dealloc(self.ptr.as_ptr().cast(), old_layout.layout());
        }

        // We don't want to drop the children or a dangling pointer!
        std::mem::forget(self);

        Self {
            ptr: new_ptr,
            _phantom: Default::default(),
        }
    }

    /// Add a child who is going to occupy the i-th spot in the children arrays.
    ///
    /// # Safety
    ///
    /// `i` must be lower or equal than the current number of children.
    pub unsafe fn add_child_unchecked(self, new_child: Node<Data>, i: usize) -> Self {
        let (new_ptr, new_layout) =
            Self::allocate_and_init_header(self.label(), self.n_children() + 1);
        // We don't want to drop the children or a dangling pointer!
        let mut old = ManuallyDrop::new(self);
        let old_layout = old.layout();

        // Init the first-bytes array.
        {
            let old_child_ptr = old_layout.child_first_bytes_ptr(old.ptr);
            let new_child_ptr = new_layout.child_first_bytes_ptr(new_ptr);
            unsafe {
                std::ptr::copy_nonoverlapping(old_child_ptr.as_ptr(), new_child_ptr.as_ptr(), i);
            }
            unsafe {
                new_child_ptr.offset(i as isize).write(new_child.label()[0]);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    old_child_ptr.offset(i as isize).as_ptr(),
                    new_child_ptr.offset(i as isize + 1).as_ptr(),
                    old.n_children() as usize - i,
                );
            }
        }

        // Init the children array.
        {
            let old_child_ptr = old_layout.children_ptr(old.ptr);
            let new_child_ptr = new_layout.children_ptr(new_ptr);
            unsafe {
                std::ptr::copy_nonoverlapping(old_child_ptr.as_ptr(), new_child_ptr.as_ptr(), i);
            }
            unsafe {
                new_child_ptr.offset(i as isize).write(new_child);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    old_child_ptr.offset(i as isize).as_ptr(),
                    new_child_ptr.offset(i as isize + 1).as_ptr(),
                    old.n_children() as usize - i,
                );
            }
        }

        unsafe {
            new_layout.value_ptr(new_ptr).write(old.data_mut().take());
        }

        unsafe {
            dealloc(old.ptr.as_ptr().cast(), old_layout.layout());
        }

        Self {
            ptr: new_ptr,
            _phantom: Default::default(),
        }
    }

    /// Remove the child that occupies the i-th spot in the children arrays.
    ///
    /// # Safety
    ///
    /// `i` must be lower or equal than the current number of children.
    pub unsafe fn remove_child_unchecked(self, i: usize) -> Self {
        let (new_ptr, new_layout) =
            Self::allocate_and_init_header(self.label(), self.n_children() - 1);
        // We don't want to drop the children or a dangling pointer!
        let mut old = ManuallyDrop::new(self);
        let old_layout = old.layout();

        // Init the first-bytes array.
        {
            let old_child_ptr = old_layout.child_first_bytes_ptr(old.ptr);
            let new_child_ptr = new_layout.child_first_bytes_ptr(new_ptr);
            unsafe {
                std::ptr::copy_nonoverlapping(old_child_ptr.as_ptr(), new_child_ptr.as_ptr(), i);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    old_child_ptr.offset(i as isize + 1).as_ptr(),
                    new_child_ptr.offset(i as isize).as_ptr(),
                    old.n_children() as usize - i - 1,
                );
            }
        }

        // Init the children array.
        {
            let old_child_ptr = old_layout.children_ptr(old.ptr);
            let new_child_ptr = new_layout.children_ptr(new_ptr);
            unsafe {
                std::ptr::copy_nonoverlapping(old_child_ptr.as_ptr(), new_child_ptr.as_ptr(), i);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    old_child_ptr.offset(i as isize + 1).as_ptr(),
                    new_child_ptr.offset(i as isize).as_ptr(),
                    old.n_children() as usize - i - 1,
                );
            }
            // Drop the child we are removing
            unsafe {
                std::ptr::drop_in_place(old_child_ptr.offset(i as isize).as_ptr());
            }
        }

        unsafe {
            new_layout.value_ptr(new_ptr).write(old.data_mut().take());
        }

        unsafe {
            dealloc(old.ptr.as_ptr().cast(), old_layout.layout());
        }

        Self {
            ptr: new_ptr,
            _phantom: Default::default(),
        }
    }
}

/// Accessor methods.
impl<Data> Node<Data> {
    /// Returns a reference to the header for this node.
    #[inline]
    fn header(&self) -> &NodeHeader {
        unsafe { self.ptr.as_ref() }
    }

    fn layout(&self) -> NodeLayout<Data> {
        self.header().layout()
    }

    /// Returns the length of the label associated with this node.
    #[inline]
    pub fn label_len(&self) -> u16 {
        self.header().label_len
    }

    /// Returns the number of children for this node.
    #[inline]
    pub fn n_children(&self) -> u8 {
        self.header().n_children
    }

    /// Returns a reference to the label associated with this node.
    pub fn label(&self) -> &[c_char] {
        let label_ptr = NodeLayout::<Data>::label_ptr(self.ptr);
        unsafe { std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize) }
    }

    /// Returns a mutable reference to the data associated with this node, if any.
    pub fn data_mut(&mut self) -> &mut Option<Data> {
        let mut data_ptr = self.layout().value_ptr(self.ptr);
        unsafe { data_ptr.as_mut() }
    }

    /// Returns a reference to the data associated with this node, if any.
    pub fn data(&self) -> Option<&Data> {
        let data_ptr = self.layout().value_ptr(self.ptr);
        unsafe { data_ptr.as_ref() }.as_ref()
    }

    /// Returns a reference to the children of this node.
    ///
    /// # Invariants
    ///
    /// The index of a child in this array matches the index of its first byte
    /// in the array returned by [`Self::children_first_bytes`].
    pub fn children(&self) -> &[Node<Data>] {
        let children_ptr = self.layout().children_ptr(self.ptr);
        unsafe { std::slice::from_raw_parts(children_ptr.as_ptr(), self.n_children() as usize) }
    }

    /// Returns a mutable reference to the children of this node.
    ///
    /// # Invariants
    ///
    /// The index of a child in this array matches the index of its first byte
    /// in the array returned by [`Self::children_first_bytes`].
    pub fn children_mut(&self) -> &mut [Node<Data>] {
        let children_ptr = self.layout().children_ptr(self.ptr);
        unsafe { std::slice::from_raw_parts_mut(children_ptr.as_ptr(), self.n_children() as usize) }
    }

    /// Returns a reference to the array containing the first byte of
    /// each child of this node.
    ///
    /// # Invariants
    ///
    /// The index of a byte in this array matches the index of the child
    /// it belongs to in the array returned by [`Self::children`].
    pub fn children_first_bytes(&self) -> &[c_char] {
        let ptr = self.layout().child_first_bytes_ptr(self.ptr);
        unsafe { std::slice::from_raw_parts(ptr.as_ptr(), self.n_children() as usize) }
    }
}

/// Trie operations.
impl<Data> Node<Data> {
    /// Inserts a new key-value pair into the trie.
    ///
    /// If the key already exists, the current value is passede to provided function,
    /// and replaced with the value returned by that function.
    pub fn insert_or_replace_with<F>(&mut self, mut key: &[c_char], f: F)
    where
        F: FnOnce(Option<Data>) -> Data,
    {
        fn placeholder<Data>() -> Node<Data> {
            Node {
                ptr: NonNull::dangling(),
                _phantom: PhantomData,
            }
        }

        let mut current = self;
        loop {
            match current
                .label()
                .iter()
                .zip(key.iter())
                .enumerate()
                .find_map(|(i, (c1, c2))| match c1.cmp(c2) {
                    Ordering::Less => Some((i, Ordering::Less)),
                    Ordering::Equal => None,
                    Ordering::Greater => Some((i, Ordering::Greater)),
                }) {
                Some((0, ordering)) => {
                    // The node's label and the key don't share a common prefix.
                    // This can only happen if the current node is the root node of the trie.
                    //
                    // Create a new root node with an empty label,
                    // insert the old root as a child of the new root,
                    // and add a new child to the empty root.
                    let old_root = std::mem::replace(current, placeholder());

                    let new_child = Node::new_leaf(key, Some(f(None)));
                    let children = if ordering == Ordering::Greater {
                        [new_child, old_root]
                    } else {
                        [old_root, new_child]
                    };
                    let new_root = unsafe { Node::new_unchecked(&[], children, None) };

                    std::mem::forget(std::mem::replace(current, new_root));
                    break;
                }
                Some((equal_up_to, ordering)) => {
                    // In this case, only part of the key matches the current node's label.
                    // Add `bis` (D) to a trie with `bike` (A), `biker` (B) and `bikes` (C).
                    //
                    // ```text
                    //     bike (A)   ->      bi (-)
                    //     /  \             /       \
                    // r (B)   s (C)      ke (A)     s (D)
                    //                   /     \
                    //                 r (B)   s (C)
                    // ```
                    //
                    // Create a new node that uses the shared prefix as its label.
                    // The prefix is then stripped from both the current label and the
                    // new key; the resulting suffixes are used as labels for the new child nodes.
                    let mut old_root = std::mem::replace(current, placeholder());
                    let old_root_label_len = old_root.label_len() as usize;

                    let (shared_prefix, new_child_suffix) = key.split_at(equal_up_to);

                    let new_child = Node::new_leaf(new_child_suffix, Some(f(None)));
                    old_root = old_root.relabel(equal_up_to, old_root_label_len - equal_up_to);

                    let children = match ordering {
                        Ordering::Less => [old_root, new_child],
                        Ordering::Greater => [new_child, old_root],
                        Ordering::Equal => {
                            unreachable!(
                                "The shared prefix has already been stripped,\
                                    therefore the first byte of the suffixes must be different."
                            )
                        }
                    };
                    let new_root = unsafe { Node::new_unchecked(shared_prefix, children, None) };

                    std::mem::forget(std::mem::replace(current, new_root));
                    break;
                }
                None => {
                    match key.len().cmp(&(current.label_len() as usize)) {
                        Ordering::Less => {
                            // The key we want to insert is a strict prefix of the current node's label.
                            // Therefore we need to insert a new _parent_ node.
                            //
                            // .split(index)
                            // .split(prefix, suffix)
                            //
                            // # Case 1: No children for the current node
                            //
                            // Add `bike` with data `B` to a trie with `biker`, where `biker` has data `A`.
                            // ```text
                            // biker (A)  ->   bike (B)
                            //                /
                            //               r (A)
                            // ```

                            // # Case 2: Current node has children
                            //
                            // Add `b` to a trie with `bi` and `bike`.
                            // `b` has data `C`, `bi` has data `A`, `bike` has data `B`.
                            //
                            // ```text
                            // bi (A)   ->    b (C)
                            //   \             \
                            //    ke (B)        i (A)
                            //                   \
                            //                    ke (B)
                            // ```
                            let mut old_root = std::mem::replace(current, placeholder());

                            let old_label_len = old_root.label_len() as usize;
                            old_root = old_root.relabel(key.len(), old_label_len - key.len());
                            let new_root =
                                unsafe { Node::new_unchecked(&key, [old_root], Some(f(None))) };

                            std::mem::forget(std::mem::replace(current, new_root));
                            break;
                        }
                        Ordering::Equal => {
                            // Suffix is empty, so the key and the node label are equal.
                            // Replace the data attached to the current node
                            // with the new data.
                            let data = current.data_mut();
                            let current_data = data.take();
                            let new_data = f(current_data);
                            *data = Some(new_data);
                            break;
                        }
                        Ordering::Greater => {
                            // Suffix is not empty, therefore the insertion needs to happen
                            // in a child (or grandchild) of the current node.
                            key = &key[current.label_len() as usize..];
                            match current.children_first_bytes().binary_search(&key[0]) {
                                Ok(i) => {
                                    // Recursion!
                                    current = &mut current.children_mut()[i];
                                    continue;
                                }
                                Err(i) => {
                                    let root = std::mem::replace(current, placeholder());

                                    let new_child = Node::new_leaf(key, Some(f(None)));
                                    let old_root =
                                        unsafe { root.add_child_unchecked(new_child, i) };

                                    std::mem::forget(std::mem::replace(current, old_root));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Get a reference to the value associated with a key.
    /// Returns `None` if the key is not present.
    pub fn find(&self, mut key: &[c_char]) -> Option<&Data> {
        let mut current = self;
        loop {
            key = strip_prefix(key, current.label())?;
            let Some(first_byte) = key.first() else {
                // The suffix is empty, so the key and the label are equal.
                return current.data();
            };
            current = self.child_starting_with(*first_byte)?;
        }
    }

    /// Get a reference to the child node whose label starts with the given byte.
    /// Returns `None` if there is no such child.
    pub fn child_starting_with(&self, c: c_char) -> Option<&Node<Data>> {
        let i = self.child_index_starting_with(c)?;
        Some(unsafe { self.children().get_unchecked(i) })
    }

    /// Get the index of the child node whose label starts with the given byte.
    /// Returns `None` if there is no such child.
    #[inline]
    pub fn child_index_starting_with(&self, c: c_char) -> Option<usize> {
        memchr_c_char(c, self.children_first_bytes())
    }

    /// Remove a child from the node.
    /// Returns the data associated with the key if it was present.
    pub fn remove_child(&mut self, key: &[c_char]) -> Option<Data> {
        // Find the index of child whose label starts with the first byte of the key,
        // as well as the child itself.
        // If the we find none, there's nothing to remove.
        // Note that `key.first()?` will cause this function to return None if the key is empty.
        let child_index = self.child_index_starting_with(*key.first()?)?;
        let child = &mut self.children_mut()[child_index];

        let suffix = key.strip_prefix(child.label())?;

        if suffix.is_empty() {
            // The child's label is equal to the key, so we remove the child.
            let data = child.data_mut().take();

            let is_leaf = child.n_children() == 0;
            if is_leaf {
                let mut current = std::mem::replace(
                    self,
                    Node {
                        ptr: NonNull::dangling(),
                        _phantom: PhantomData,
                    },
                );
                // If the child is a leaf, we remove the child node itself.
                current = unsafe { current.remove_child_unchecked(child_index) };
                std::mem::forget(std::mem::replace(self, current));
            } else {
                // If there's a single grandchild,
                // we merge the grandchild into the child.
                child.merge_child_if_possible();
            }

            data
        } else {
            let data = child.remove_child(suffix);
            child.merge_child_if_possible();
            data
        }
    }

    /// If `self` has exactly one child, and `self` doesn't hold
    /// any data, merge child into `self`, by moving the child's data and
    /// childreninto `self`.
    pub fn merge_child_if_possible(&mut self) {
        if self.data().is_some() || self.n_children() != 1 {
            return;
        }
        let old_child = std::mem::replace(
            &mut self.children_mut()[0],
            Node {
                ptr: NonNull::dangling(),
                _phantom: Default::default(),
            },
        );
        let new_parent = old_child.prepend(self.label());
        let old_self = ManuallyDrop::new(std::mem::replace(self, new_parent));
        // There is no data and we removed the child, so we can
        // just free the buffer and be done.
        unsafe {
            dealloc(old_self.ptr.as_ptr().cast(), old_self.layout().layout());
        }
    }

    /// The memory usage of this node and his descendants, in bytes.
    pub fn mem_usage(&self) -> usize {
        self.layout().layout().size() + self.children().iter().map(|c| c.mem_usage()).sum::<usize>()
    }

    /// The number of descendants of this node, plus 1.
    pub fn n_nodes(&self) -> usize {
        self.n_children() as usize + self.children().iter().map(|c| c.n_nodes()).sum::<usize>()
    }
}

impl<Data> Drop for Node<Data> {
    fn drop(&mut self) {
        let layout = self.layout().layout();
        unsafe { std::ptr::drop_in_place(self.data_mut()) };
        unsafe { std::ptr::drop_in_place(self.children_mut()) };

        unsafe { dealloc(self.ptr.as_ptr().cast(), layout) };
    }
}

impl<Data: fmt::Debug> fmt::Debug for Node<Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut stack = vec![(0, self, 0, 0)];

        while let Some((first_byte, next, white_indentation, line_indentation)) = stack.pop() {
            let label_repr = crate::utils::to_string_lossy(next.label());
            let data_repr = next
                .data()
                .as_ref()
                .map_or("(-)".to_string(), |data| format!("({:?})", data));

            let prefix = if white_indentation == 0 && line_indentation == 0 {
                "".to_string()
            } else {
                let whitespace = " ".repeat(white_indentation);
                let line = "–".repeat(line_indentation - 1);
                let first_byte = to_string_lossy(&[first_byte]);
                format!("{whitespace}↳{first_byte}{line}")
            };

            writeln!(f, "{prefix}\"{label_repr}\" {data_repr}")?;

            for (child, first_byte) in next
                .children()
                .iter()
                .zip(next.children_first_bytes())
                .rev()
            {
                let new_line_indentation = 4;
                let white_indentation = white_indentation + line_indentation + 2;
                stack.push((*first_byte, child, white_indentation, new_line_indentation));
            }
        }
        Ok(())
    }
}
