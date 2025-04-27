use metadata::{NodeHeader, PtrMetadata, PtrWithMetadata};
use std::{
    alloc::{handle_alloc_error, realloc},
    ffi::c_char,
    marker::PhantomData,
    mem::ManuallyDrop,
    num::NonZeroUsize,
    ptr::NonNull,
};

mod accessors;
mod metadata;
mod trait_impls;
mod trie_ops;

/// A node in a [`TrieMap`](crate::TrieMap).
///
/// Each node stores:
///
/// - A sequence of [`c_char`] as its label (see [`Self::label`])
/// - The first [`c_char`] of the label of each of its children (see [`Self::children_first_bytes`])
/// - Pointers to each of its children (see [`Self::children`])
/// - An optional payload (see [`Self::data`])
///
/// Check out [`PtrMetadata`]'s documentation for more information as to _how_ the information
/// above is stored in memory.
pub(crate) struct Node<Data> {
    /// # Safety invariants
    ///
    /// 1. The layout of the buffer behind this pointer matches the layout mandated by its header.
    /// 2. All node fields are correctly initialized.
    /// 3. The buffer behind this pointer was allocated using the global allocator.
    ptr: std::ptr::NonNull<NodeHeader>,
    _phantom: PhantomData<Data>,
}

/// Constructors.
impl<Data> Node<Data> {
    /// Create a new node without children.
    ///
    /// # Panics
    ///
    /// Panics if the label length exceeds [`u16::MAX`].
    pub(crate) fn new_leaf(label: &[c_char], value: Option<Data>) -> Self {
        // SAFETY:
        // - There are no children, so all requirements are met.
        unsafe { Self::new_unchecked(label, [], value) }
    }

    /// # Requirements
    ///
    /// To guarantee `Node`'s invariants, the caller must ensure that:
    ///
    /// - The first byte of each child is unique within the provided children collection
    /// - All children have a non-empty label
    /// - The children array is sorted in ascending order
    ///
    /// Those requirements are checked at runtime if `debug_assertions` are enabled,
    /// otherwise they are assumed to hold to minimize runtime overhead on release builds.
    ///
    /// # Panics
    ///
    /// Panics if the label length exceeds [`u16::MAX`].
    /// Panics if the number of children exceeds [`u8::MAX`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that all children have a non-empty label.
    unsafe fn new_unchecked<const N: usize>(
        label: &[c_char],
        children: [Node<Data>; N],
        value: Option<Data>,
    ) -> Self {
        // This has no runtime overhead, since the number of children is known at compile time.
        if N > u8::MAX as usize {
            panic!(
                "There are {} children, which exceeds the maximum allowed number, {}",
                N,
                u8::MAX
            );
        }
        let Ok(label_len) = label.len().try_into() else {
            panic!(
                "The label length is {}, which exceeds the maximum allowed length, {}",
                label.len(),
                u16::MAX
            );
        };

        #[cfg(debug_assertions)]
        {
            for child in &children {
                debug_assert!(
                    !child.label().is_empty(),
                    "The label of one of the children is empty"
                );
            }
            if N > 0 {
                for i in 0..N - 1 {
                    debug_assert!(
                        children[i].label()[0] < children[i + 1].label()[0],
                        "The label of child {} is not lexicographically smaller than the label of child {}",
                        i,
                        i + 1
                    );
                }
            }
        }

        let header = NodeHeader {
            // We don't support labels longer than u16::MAX.
            label_len,
            // A node can have at most 255 children, since that's
            // the number of unique `c_char` values.
            n_children: N as u8,
        };
        let mut new_ptr = header.metadata().allocate();
        // Initialize the allocated buffer with valid values.

        // SAFETY:
        // - We have exclusive access to the buffer that `new_ptr` points to,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_header(header) };

        // SAFETY:
        // 1. The number of elements in `label` matches the capacity of the label buffer,
        //    since it matches the label length we used in the header for the new node.
        // 2. `label` doesn't overlap with the destination buffer, since it was freshly allocated.
        // 3. We have exclusive access to the label buffer, since it was allocated earlier in this function.
        unsafe { new_ptr.label().copy_from_slice_nonoverlapping(label) };

        // Initialize the array of child first bytes.
        {
            let mut next_byte_ptr = new_ptr.children_first_bytes().ptr();
            for child in &children {
                // SAFETY:
                // We require the caller of this method to guarantee that child labels are non-empty,
                // thus it's safe to access the first element.
                let first_byte = unsafe { child.label().get_unchecked(0) };
                // No risk of double-free on drop since `c_char` is `Copy`.
                //
                // SAFETY:
                // - The destination range is all contained within newly allocated buffer.
                // - We have exclusive access to the destination buffer,
                //   since it was allocated earlier in this function.
                // - The destination pointer is well aligned, see 1. in [`PtrMetadata::child_ptr`]
                unsafe { next_byte_ptr.write(*first_byte) };
                // SAFETY:
                // - The offsetted pointer doesn't overflow `isize`, since it is within the bounds
                //   of an allocation for a well-formed `Layout` instance.
                // - The offsetted pointer is within the bounds of the allocation, thanks to
                //   layout we used for the buffer.
                next_byte_ptr = unsafe { next_byte_ptr.add(1) };
            }
        }

        // Initialize the children array.
        //
        // SAFETY:
        // - We have exclusive access to the destination buffer, since it was allocated earlier in this function.
        // - The number of children matches the buffer capacity.
        unsafe { new_ptr.children().write(children) };

        // Set the value.
        // SAFETY:
        // - We have exclusive access to the destination buffer,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_value(value) };

        // SAFETY:
        // - All fields have been initialized, we can now safely return the newly created node.
        unsafe { new_ptr.assume_init() }
    }
}

impl<Data> Node<Data> {
    /// Apply a closure to the node, replacing it with the result.
    ///
    /// The closure is expected to take the node by value.
    fn map<F>(&mut self, f: F)
    where
        F: FnOnce(Node<Data>) -> Node<Data>,
    {
        /// A guard that prevents the content of `target` from
        /// being dropped when active.
        struct DropGuard<'a, Data> {
            target: &'a mut Node<Data>,
            active: bool,
        }

        impl<'a, Data> DropGuard<'a, Data> {
            /// Create a new active guard, returning the value of `target`
            /// alongside the guard.
            ///
            /// `target` will be populated with a placeholder node,
            /// which relies on a dangling pointer.
            fn new(target: &'a mut Node<Data>) -> (Self, Node<Data>) {
                let node = std::mem::replace(
                    target,
                    Node {
                        ptr: NonNull::dangling(),
                        _phantom: PhantomData,
                    },
                );
                let guard = DropGuard {
                    target,
                    active: true,
                };
                (guard, node)
            }

            /// Disarms the guard, putting back a valid value into the `target` slot.
            fn disarm(&mut self, node: Node<Data>) {
                let placeholder = std::mem::replace(self.target, node);
                std::mem::forget(placeholder);
                self.active = false;
            }
        }

        impl<Data> Drop for DropGuard<'_, Data> {
            fn drop(&mut self) {
                if self.active {
                    // The guard is active, so `target` is dangling pointer.
                    // We don't want it to be dropped, so we replace it with
                    // an actual node that's safe to drop.
                    // This incurs a cost (i.e. the node allocation), but this code
                    // path is only triggered when the `f` closure panics,
                    // which should only happen in case of an implementation error.
                    std::mem::forget(std::mem::replace(self.target, Node::new_leaf(&[], None)));
                }
            }
        }

        // To allow the closure to manipulate the node by value, we need
        // to temporarily move it out of `self`.
        // This forces us to provide a "placeholder" node, since `self`
        // can't be left uninitialized.
        // We use a node with a dangling pointer as placeholder: the pointer
        // is well-aligned and not-null, but invoking _any_ node method on it
        // will result in undefined behavior.
        // In particular, we need to be absolutely sure that the placeholder
        // node won't be dropped, since that would result in the dangling pointer
        // being dereferenced.
        // The main danger comes from the possibility of `f` panicking, which
        // may trigger unwinding and thus cause the placeholder node to be dropped.
        // To defend against this case, we wrap `self` in a `DropGuard` to prevent
        // it from being dropped prematurely in case `f` panics.
        let (mut guard, node) = DropGuard::new(self);
        let new_node = f(node);

        // After the closure has been executed, we can safely disarm the guard
        // by replacing the placeholder with the new node value.
        guard.disarm(new_node);
    }
}

/// Operations that modify the label or the node's children, thus requiring
/// new memory allocation (or re-allocations).
impl<Data> Node<Data> {
    /// Downgrade the node to a pointer with metadata.
    ///
    /// Invoke this method if you need to manipulate the buffer
    /// for this node in a way that no longer guarantees that
    /// all node fields will be initialized.
    ///
    /// # Memory leaks
    ///
    /// The caller is responsible for freeing the buffer
    /// that the pointer points to.
    fn downgrade(self) -> PtrWithMetadata<Data> {
        // We don't want the destructor to run, otherwise the buffer
        // will be freed.
        let self_ = ManuallyDrop::new(self);
        // SAFETY:
        // - The buffer size+alignment and the metadata match.
        unsafe { PtrWithMetadata::new(self_.ptr, self_.metadata()) }
    }

    /// Splits the node label at the given offset, creating a new child node with the remaining label.
    /// `self` is then mutated in place to become the parent of the new child, as well as the parent
    /// of `extra_child`, if provided.
    ///
    /// # Case 1: No children for `self`
    ///
    /// Split `biker` at offset `3`.
    ///
    /// ```text
    /// biker (A)  ->   bike (-)
    ///                /
    ///               r (A)
    /// ```
    ///
    /// # Case 2: `self` has children
    ///
    /// Split `bi` at offset `1`.
    ///
    /// ```text
    /// bi (A)   ->    b (-)
    ///   \             \
    ///    ke (B)        i (A)
    ///                   \
    ///                    ke (B)
    /// ```
    ///
    /// # Safety
    ///
    /// The caller must ensure that the offset is within the bounds of the current label.
    unsafe fn split_unchecked(mut self, offset: usize, extra_child: Option<Node<Data>>) -> Self {
        debug_assert!(
            offset < self.label_len() as usize,
            "The label offset must be fully contained within the current label"
        );
        let child_header = NodeHeader {
            label_len: self.label_len() - offset as u16,
            n_children: self.n_children(),
        };
        let old_data = self.data_mut().take();
        let child_metadata = child_header.metadata();
        // We allocate a fresh buffer for the child node that's going to use
        // the suffix of the current label as its own label.
        let mut child_ptr = child_metadata.allocate();

        // SAFETY:
        // - We have exclusive access to the buffer that `child_ptr` points to, since it was just allocated.
        unsafe { child_ptr.write_header(child_header) };

        // Set the suffix as the child's label
        let suffix = &self.label()[offset..];
        // SAFETY:
        // 1. The length of the suffix matches the capacity of the label buffer
        //    of the new child node.
        // 2. We have exclusive access to the destination buffer,
        //    since it was allocated earlier in this function.
        // 3. The source and destination buffers don't overlap, since the destination
        //    buffer was freshly allocated earlier in this function.
        unsafe { child_ptr.label().copy_from_slice_nonoverlapping(suffix) };

        // `self`'s children become the children of the new node.
        // No risk of double-free/use-after-free since &[c_char] is `Copy`.
        //
        // SAFETY:
        // 1. The length of the source slice matches the capacity of the first-byte buffer
        //    of the new child node.
        // 2. We have exclusive access to the destination buffer,
        //    since it was allocated earlier in this function.
        // 3. The source and destination buffers don't overlap, since the destination
        //    buffer was freshly allocated earlier in this function.
        unsafe {
            child_ptr
                .children_first_bytes()
                .copy_from_slice_nonoverlapping(self.children_first_bytes())
        };

        let old_ptr = self.downgrade();

        // Since we downgraded `self` to a `PtrWithMetadata`, the destructor won't run so the children
        // won't be dropped. No risk of double drop or use-after-free here.
        // SAFETY:
        // - The source range is all contained within a single allocation.
        // - The destination range is all contained within a single allocation.
        // - We have exclusive access to the destination buffer, since it was allocated earlier in this function.
        // - No one else is mutating the source buffer, since this function owns it.
        // - Both source and destination pointers are well aligned, see 1. in [`PtrMetadata::children_ptr`]
        // - The two buffers don't overlap. The destination buffer was freshly allocated
        //   earlier in this function.
        unsafe {
            child_ptr.children().ptr().copy_from_nonoverlapping(
                old_ptr.children().ptr(),
                child_header.n_children as usize,
            )
        };

        // Move the value over.
        // SAFETY:
        // - We have exclusive access to the destination buffer, since it was allocated earlier in this function.
        unsafe { child_ptr.write_value(old_data) };

        // SAFETY:
        // The child node is fully initialized now.
        let child = unsafe { child_ptr.assume_init() };

        let new_header = NodeHeader {
            label_len: offset as u16,
            n_children: 1 + if extra_child.is_some() { 1 } else { 0 },
        };
        // Resize the old allocation
        let mut new_ptr = {
            let new_metadata: PtrMetadata<Data> = new_header.metadata();
            let (old_ptr, old_metadata) = old_ptr.into_parts();
            // SAFETY:
            // - `self.ptr` was allocated via the same global allocator
            //    we are invoking via `realloc`.
            // - `old_metadata.layout()` is the same layout that was used
            //   to allocate the buffer behind `self.ptr`.
            // - `new_metadata.layout().size()` is greater than zero, see
            //   1. in [`PtrMetadata::layout`]
            // - `new_metadata.layout().size()` does not overflow `isize`
            //   since both the old and the new layout have the same alignment
            //   and `new_metadata.layout()` already includes the required
            //   padding. See 2. in [`PtrMetadata::layout`]
            let new_ptr = unsafe {
                realloc(
                    old_ptr.as_ptr().cast(),
                    old_metadata.layout(),
                    new_metadata.layout().size(),
                ) as *mut NodeHeader
            };
            let Some(new_ptr) = NonNull::new(new_ptr) else {
                handle_alloc_error(new_metadata.layout());
            };
            // SAFETY:
            // `new_ptr` has been allocated with the layout mandated by `new_metadata`.
            unsafe { PtrWithMetadata::new(new_ptr, new_metadata) }
        };

        // SAFETY:
        // - We have exclusive access to the buffer that `child_ptr` points to, since it was just allocated.
        unsafe { new_ptr.write_header(new_header) };

        // `realloc` guarantees that the range `0..min(layout.size(), new_size)`
        // of the new memory block is guaranteed to have the same values as the original block.
        // Therefore, we don't need to update the label slot: the label must be truncated,
        // but the prefix is in the correct location already.

        if let Some(extra_child) = extra_child {
            // Sort the children before writing them to the new memory block.
            let (first_bytes, children) = {
                let extra_label = extra_child.label();
                let extra_byte = extra_label[0];
                let new_label = child.label();
                let new_byte = new_label[0];
                if extra_byte < new_byte {
                    ([extra_byte, new_byte], [extra_child, child])
                } else {
                    ([new_byte, extra_byte], [child, extra_child])
                }
            };

            // SAFETY:
            // - The number of children matches the buffer capacity.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            unsafe { new_ptr.children_first_bytes().write(first_bytes) };
            // SAFETY:
            // - The number of children matches the buffer capacity.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            unsafe { new_ptr.children().write(children) };
        } else {
            // We add the newly created child node to the node's children.

            // SAFETY:
            // - The number of children matches the buffer capacity.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            unsafe { new_ptr.children_first_bytes().write([child.label()[0]]) };
            // SAFETY:
            // - The number of children matches the buffer capacity.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            unsafe { new_ptr.children().write([child]) };
        }

        // After the split, the parent has no data attached, so we set the value
        // to `None`.
        // SAFETY:
        // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
        unsafe { new_ptr.write_value(None) };

        // SAFETY:
        // - All fields are now correctly initialized.
        unsafe { new_ptr.assume_init() }
    }

    /// Set the node label to `concat!(prefix, old_label)`.
    ///
    /// This version uses `realloc` for more efficient memory usage.
    fn prepend(mut self, prefix: &[c_char]) -> Self {
        debug_assert!(
            self.label_len() as usize + prefix.len() <= u16::MAX as usize,
            "The new label length exceeds u16::MAX, {}",
            u16::MAX
        );

        let new_header = NodeHeader {
            label_len: self.label_len() + prefix.len() as u16,
            n_children: self.n_children(),
        };
        let old_label_len = self.label_len() as usize;
        let data = self.data_mut().take();

        let (mut new_ptr, old_ptr) = {
            let new_metadata = new_header.metadata();
            let (old_ptr, old_metadata) = self.downgrade().into_parts();
            // The size may remain the same: the new label characters may end up
            // occupying bytes that were previously used as padding.
            debug_assert!(
                old_metadata.layout().size() <= new_metadata.layout().size(),
                "When prepending a prefix to the label, the allocation size should not decrease."
            );
            // Reallocate the memory block to the new size.
            // SAFETY:
            // - `old_ptr` was allocated via the same global allocator
            //    we are invoking via `realloc` (see invariant 3. in [`Self::ptr`])
            // - `old_metadata.layout()` is the same layout that was used
            //   to allocate the buffer behind `old_ptr` (see invariant 1. in [`Self::ptr`])
            // - `new_metadata.layout().size()` is greater than zero, see
            //   1. in [`PtrMetadata::layout`]
            // - `new_metadata.layout().size()` does not overflow `isize`
            //   since both the old and the new layout have the same alignment
            //   and `new_metadata.layout()` already includes the required
            //   padding. See 2. in [`PtrMetadata::layout`]
            let new_ptr = unsafe {
                realloc(
                    old_ptr.as_ptr().cast(),
                    old_metadata.layout(),
                    new_metadata.layout().size(),
                ) as *mut NodeHeader
            };

            let Some(raw_ptr) = NonNull::new(new_ptr) else {
                handle_alloc_error(new_metadata.layout())
            };
            // SAFETY:
            // - The buffer was allocated using the layout for this metadata instance.
            let new_ptr = unsafe { PtrWithMetadata::new(raw_ptr, new_metadata) };
            // SAFETY:
            // - The buffer was allocated using a layout with the same alignment and
            //   a size that's greater than or equal to the original layout.
            let old_ptr = unsafe { PtrWithMetadata::new(raw_ptr, old_metadata) };

            (new_ptr, old_ptr)
        };

        // Since we are _expanding_, we set fields in right-to-left order.
        // This ensures that we don't accidentally overwrite any data that we
        // may still need to copy from later.

        // Set the value
        // SAFETY:
        // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
        unsafe { new_ptr.write_value(data) };

        // Copy the children over.
        // The offset of the array has changed since the label length has increased.
        //
        // SAFETY:
        // - Both pointers are well aligned thanks to the layout used for the buffer.
        // - The length of both the source and the destination matches the number of
        //   elements we are copying.
        unsafe {
            new_ptr
                .children()
                .ptr()
                .copy_from(old_ptr.children().ptr(), new_header.n_children as usize);
        }

        // Copy the children first bytes.
        // The offset of the array has changed since the label length has increased.
        // SAFETY:
        // - The length of both the source and the destination matches the number of
        //   elements we are copying.
        // - Both pointers are well aligned thanks to the layout used for the buffer.
        unsafe {
            new_ptr.children_first_bytes().ptr().copy_from(
                old_ptr.children_first_bytes().ptr(),
                new_header.n_children as usize,
            )
        };

        // Initialize the node label - We need to shift the old label to make room for the prefix
        {
            // First, shift the old label to the right position
            //
            // SAFETY:
            // 1. The capacity of the label buffer matches the length of the prefix
            //    + the length of the old label.
            // 2. We have exclusive access to the label buffer,
            //    since it was freshly (re)allocated earlier in this function.
            // 3. The first `old_label_len` bytes of the label buffer are initialized,
            //    since their values were correctly set before the realloc invocations.
            unsafe { new_ptr.label().shift_right(prefix.len(), old_label_len) };

            // Then copy the prefix at the beginning
            // SAFETY:
            // 1. The label buffer is large enough to hold the prefix.
            // 2. The prefix slice does not overlap with newly (re)allocated
            //    buffer that holds the label.
            // 3. We have exclusive access to the buffer behind this pointer,
            //    since it was freshly (re)allocated earlier in this function.
            unsafe { new_ptr.label().copy_from_slice_nonoverlapping(prefix) };
        }

        // SAFETY:
        // - We have exclusive access to the buffer behind this pointer,
        //   since it was freshly (re)allocated earlier in this function.
        unsafe { new_ptr.write_header(new_header) };

        // SAFETY:
        // - All fields have been initialized.
        unsafe { new_ptr.assume_init() }
    }

    /// Add a child who is going to occupy the i-th spot in the children arrays.
    ///
    /// # Safety
    ///
    /// `i` must be lower or equal than the current number of children.
    unsafe fn add_child_unchecked(mut self, new_child: Node<Data>, i: usize) -> Self {
        debug_assert!(
            i <= self.n_children() as usize,
            "Index out of bounds: {} > {}",
            i,
            self.n_children()
        );

        // First, calculate the new layout size
        let new_header = NodeHeader {
            label_len: self.label_len(),
            n_children: self.n_children() + 1,
        };
        let old_n_children = self.n_children() as usize;
        let new_metadata = new_header.metadata();
        let data = self.data_mut().take();

        let (mut new_ptr, old_ptr) = {
            let (old_ptr, old_metadata) = self.downgrade().into_parts();

            // Reallocate the memory block to the new size BEFORE making any changes
            debug_assert!(
                new_metadata.layout().size() >= old_metadata.layout().size(),
                "The layout size after adding a child should not be smaller than the old layout size"
            );
            // SAFETY:
            // - `old_ptr` was allocated via the same global allocator
            //    we are invoking via `realloc` (see invariant 3. in [`Self::ptr`])
            // - `old_metadata.layout()` is the same layout that was used
            //   to allocate the buffer behind `old_ptr` (see invariant 1. in [`Self::ptr`])
            // - `new_metadata.layout().size()` is greater than zero, see
            //   1. in [`PtrMetadata::layout`]
            // - `new_metadata.layout().size()` does not overflow `isize`
            //   since both the old and the new layout have the same alignment
            //   and `new_metadata.layout()` already includes the required
            //   padding. See 2. in [`PtrMetadata::layout`]
            let raw_ptr = unsafe {
                realloc(
                    old_ptr.as_ptr().cast(),
                    old_metadata.layout(),
                    new_metadata.layout().size(),
                ) as *mut NodeHeader
            };

            let Some(raw_ptr) = NonNull::new(raw_ptr) else {
                handle_alloc_error(new_metadata.layout())
            };
            // SAFETY:
            // - The layout of the new allocation matches `new_metadata`.
            let new_ptr = unsafe { PtrWithMetadata::new(raw_ptr, new_metadata) };
            // SAFETY:
            // - After the reallocation, `raw_ptr` is backed by an allocation with
            //   the same alignment as the old allocation and a size that is at least
            //   as large as the old allocation.
            let old_ptr = unsafe { PtrWithMetadata::new(raw_ptr, old_metadata) };
            (new_ptr, old_ptr)
        };

        // Since we are _expanding_, we set fields in right-to-left order.
        // This ensures that we don't accidentally overwrite any data that we
        // may still need to copy from later.

        // SAFETY:
        // - We have exclusive access to the destination buffer,
        //   since it was (re)allocated earlier in this function.
        unsafe { new_ptr.write_value(data) };

        // We set aside the first byte of the new child node's label
        // before moving it into the newly created gap.
        let new_child_first_byte = new_child.label()[0];

        // Children pointers
        {
            // Copy the `[i..]` range from the old buffer into the `[i+1..]` range of the new buffer.
            {
                // SAFETY:
                // - The offsetted pointer is within bounds because the caller guarantees
                //   that `i` is smaller than or equal to `old_n_children`.
                let old_i_th = unsafe { old_ptr.children().ptr().add(i) };
                // SAFETY:
                // The offsetted pointer is within bounds because the caller guarantees
                // that `i` is strictly smaller than `old_n_children` + 1.
                let new_i_plus_1_th = unsafe { new_ptr.children().ptr().add(i + 1) };
                // SAFETY:
                // - The source range is within bounds because `i + (old_n_children - i)`
                //   is the length of the old buffer.
                // - The destination range is within bounds because `i + 1 + (old_n_children - i)`
                //   is the length of the new buffer.
                // - Both pointers are well-aligned.
                // - We have exclusive access to the destination buffer since it
                //   was (re)allocated earlier in this function.
                unsafe { new_i_plus_1_th.copy_from(old_i_th, old_n_children - i) };
            }

            // Set the `i`th element in the new buffer to the new child node
            {
                // SAFETY:
                // The offsetted pointer is within bounds because the caller guarantees
                // that `i` is strictly smaller than `old_n_children` + 1.
                let new_i = unsafe { new_ptr.children().ptr().add(i) };
                // Insert the new child node in the newly created gap
                // SAFETY:
                // - The pointer is well-aligned.
                // - We have exclusive access to the destination buffer since it
                //   was (re)allocated earlier in this function.
                unsafe { new_i.write(new_child) };
            }

            // Copy the `[..i]` range from the old buffer to the new buffer.
            // SAFETY:
            // - The source and destination ranges are within bounds
            //   because `i` is smaller than or equal to `old_n_children`.
            // - Both pointers are well-aligned.
            // - We have exclusive access to the destination buffer since it
            //   was (re)allocated earlier in this function.
            unsafe {
                new_ptr
                    .children()
                    .ptr()
                    .copy_from(old_ptr.children().ptr(), i)
            };
        }

        // Children first bytes
        {
            // Copy the `[i..]` range from the old buffer into the `[i+1..]` range of the new buffer.
            {
                // SAFETY:
                // - The offsetted pointer is within bounds because the caller guarantees
                //   that `i` is smaller than or equal to `old_n_children`.
                let old_i_th = unsafe { old_ptr.children_first_bytes().ptr().add(i) };
                // SAFETY:
                // The offsetted pointer is within bounds because the caller guarantees
                // that `i` is strictly smaller than `old_n_children` + 1.
                let new_i_plus_1_th = unsafe { new_ptr.children_first_bytes().ptr().add(i + 1) };
                // SAFETY:
                // - The source range is within bounds because `i + (old_n_children - i)`
                //   is the length of the old buffer.
                // - The destination range is within bounds because `i + 1 + (old_n_children - i)`
                //   is the length of the new buffer.
                // - Both pointers are well-aligned.
                // - We have exclusive access to the destination buffer since it
                //   was (re)allocated earlier in this function.
                unsafe { new_i_plus_1_th.copy_from(old_i_th, old_n_children - i) };
            }

            // Set the `i`th element in the new buffer to the first-byte of the new child node
            {
                // SAFETY:
                // The offsetted pointer is within bounds because the caller guarantees
                // that `i` is strictly smaller than `old_n_children` + 1.
                let new_i = unsafe { new_ptr.children_first_bytes().ptr().add(i) };
                // SAFETY:
                // - The pointer is well-aligned.
                // - We have exclusive access to the destination buffer since it
                //   was (re)allocated earlier in this function.
                unsafe { new_i.write(new_child_first_byte) };
            }

            // Copy the `[..i]` range from the old buffer to the new buffer.
            // SAFETY:
            // - The source and destination ranges are within bounds
            //   because `i` is smaller than or equal to `old_n_children`.
            // - Both pointers are well-aligned.
            // - We have exclusive access to the destination buffer since it
            //   was (re)allocated earlier in this function.
            unsafe {
                new_ptr
                    .children_first_bytes()
                    .ptr()
                    .copy_from(old_ptr.children_first_bytes().ptr(), i)
            };
        }

        // Neither the label length nor the label value have changed, so nothing to do there.

        // Update the header to reflect the new child count.
        // SAFETY:
        // - We have exclusive access to the buffer behind this pointer,
        //   since it was freshly (re)allocated earlier in this function.
        unsafe { new_ptr.write_header(new_header) };

        // SAFETY:
        // - All fields have been initialized correctly.
        unsafe { new_ptr.assume_init() }
    }

    /// Remove the child that occupies the i-th spot in the children arrays.
    ///
    /// # Safety
    ///
    /// `i` must be lower than the current number of children.
    unsafe fn remove_child_unchecked(mut self, i: usize) -> Self {
        debug_assert!(
            i < self.n_children() as usize,
            "Index out of bounds: {} >= {}",
            i,
            self.n_children()
        );

        let old_n_children = self.n_children() as usize;
        let data = self.data_mut().take();
        let new_header = NodeHeader {
            label_len: self.label_len(),
            n_children: self.n_children() - 1,
        };

        let old_ptr = self.downgrade();
        // SAFETY:
        // 1. Satisfied thanks to 1. in [`PtrWithMetadata`]'s documentation, with respect to the `old_ptr` instance.
        // 2. We are shrinking the node, so the size of the allocation behind `old_ptr` is equal or greater
        //    than the size dictated by `new_header.metadata()`.
        // 3. The alignment of the allocation behind `old_ptr` matches the alignment dictated by `new_header.metadata()`.
        let mut new_ptr = unsafe { PtrWithMetadata::new(old_ptr.ptr(), new_header.metadata()) };

        // Since we are _shrinking_, we set fields in left-to-right order.
        // This ensures that we don't accidentally overwrite any data that we
        // may still need to copy from later.

        // Update the header to reflect the new number of children.
        // SAFETY:
        // - We have exclusive access to the node's buffer, since this function
        //   took ownership of `self`.
        unsafe { new_ptr.write_header(new_header) };

        // The label value is unchanged, and its offset doesn't depend
        // on the number of children, so nothing to do there.

        // The `[..i]` range of the children's first bytes array doesn't change.
        // Its offset doesn't depend on the number of children, so nothing to do there.
        //
        // The `[i+1..]` range must be shifted to the left by one position to
        // become the `[i..]` range for the new node.
        //
        // SAFETY:
        // 1. `i + 1 + n_entries = old_n_children`, so we're within bounds.
        // 2. We have exclusive access to the buffer, since this function
        //    took ownership of `self`.
        // 3. All elements in the `[i+1..]` range are correctly initialized, since they were
        //    for `self` and we haven't touched them (yet).
        unsafe {
            // We use the `old_ptr` here since we need to read the `old_n_children`th entry,
            // which would be past the end of the array according to the new (shrunk) layout.
            old_ptr.children_first_bytes().shift_left(
                i,
                NonZeroUsize::new(1).unwrap(),
                old_n_children - (i + 1),
            )
        };

        // Adjust the children pointers array
        {
            // The value of the `[..i]` range of the children pointers array doesn't change.
            // Nonetheless, its offset does depend on the number of children, so we need
            // to perform a copy operation to shift it to the left (if needed).
            //
            // SAFETY:
            // 1. The caller guarantees that `i` is strictly smaller than `old_n_children`, so
            //    both source and destination ranges are in bounds.
            // 2. We have exclusive access to the buffer, since this function
            //    took ownership of `self`.
            // 3. All elements in the `[..i]` range are correctly initialized, since they were
            //    for `self` and we haven't touched them (yet).
            unsafe {
                new_ptr
                    .children()
                    .ptr()
                    .copy_from(old_ptr.children().ptr(), i)
            };

            // Drop the child we are removing
            {
                // SAFETY:
                // - The caller guarantees that `i` is strictly smaller than `old_n_children`.
                let old_i_th = unsafe { old_ptr.children().ptr().add(i) };
                // SAFETY:
                // - The pointer is well-aligned and points to a memory location that's correctly
                //   initialized, since it was for `self` and we haven't touched it (yet).
                // 2. We have exclusive access to the buffer, since this function
                //    took ownership of `self`.
                unsafe { old_i_th.drop_in_place() };
            }

            // The `[i+1..]` range of the children pointers array needs to be shifted to the left
            // by one position to become the `[i..]` range of the new node.
            {
                // SAFETY:
                // - `i + 1` is smaller than or equal to `old_n_children`, since the caller guarantees
                //   that `i` is strictly smaller than `old_n_children`.
                let i_th_ptr = unsafe { new_ptr.children().ptr().add(i) };
                // SAFETY:
                // - `i + 1` is smaller than or equal to `old_n_children`, since the caller guarantees
                //   that `i` is strictly smaller than `old_n_children`.
                let i_plus_1_ptr = unsafe { old_ptr.children().ptr().add(i + 1) };
                // SAFETY:
                // 1. `(i + 1) + (old_n_children - (i + 1)) = old_n_children`, so we're within bounds
                //    for the source.
                //    `i + (old_n_children - (i + 1)) = old_n_children - 1`, so we're within bounds
                //    for the destination.
                // 2. We have exclusive access to the buffer, since this function
                //    took ownership of `self`.
                // 3. All elements in the `[i+1..]` range are correctly initialized, since they were
                //    for `self` and we haven't touched them (yet).
                unsafe { i_th_ptr.copy_from(i_plus_1_ptr, old_n_children - (i + 1)) };
            }
        }

        // Set the value
        // SAFETY:
        // - We have exclusive access to the destination buffer,
        //   since this function takes ownership of `self`.
        unsafe { new_ptr.write_value(data) };

        // Reallocate to the smaller size
        let (old_ptr, old_metadata) = old_ptr.into_parts();
        let (_, new_metadata) = new_ptr.into_parts();
        debug_assert!(
            old_metadata.layout().size() >= new_metadata.layout().size(),
            "When removing a child, the size of the allocation should not increase"
        );
        let new_ptr = {
            // SAFETY:
            // - `old_ptr` was allocated via the same global allocator
            //    we are invoking via `realloc` (see invariant 3. in [`Self::ptr`])
            // - `old_metadata.layout()` is the same layout that was used
            //   to allocate the buffer behind `old_ptr` (see invariant 1. in [`Self::ptr`])
            // - `new_metadata.layout().size()` is greater than zero, see
            //   1. in [`PtrMetadata::layout`]
            // - `new_metadata.layout().size()` does not overflow `isize`
            //   since both the old and the new layout have the same alignment
            //   and `new_metadata.layout()` already includes the required
            //   padding. See 2. in [`PtrMetadata::layout`]
            unsafe {
                realloc(
                    old_ptr.as_ptr().cast(),
                    old_metadata.layout(),
                    new_metadata.layout().size(),
                ) as *mut NodeHeader
            }
        };

        let Some(new_ptr) = NonNull::new(new_ptr) else {
            // The reallocation failed!
            handle_alloc_error(new_metadata.layout())
        };

        // The buffer is already correctly initialized since we performed the shifts
        // before reallocating the buffer.
        Self {
            ptr: new_ptr,
            _phantom: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "The closure panicked")]
    /// If the machinery we use in `map` is not working as expected,
    /// this test will trigger the dereferencing of a dangling pointer,
    /// which will in turn cause a SIGSEGV crash.
    fn test_map_with_panicking_closure() {
        let mut node = Node::<i32>::new_leaf(&[1, 2, 3], Some(42));
        node.map(|_| panic!("The closure panicked"));
    }
}
