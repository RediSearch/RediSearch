use std::{
    alloc::{dealloc, handle_alloc_error, realloc},
    cmp::Ordering,
    ffi::c_char,
    fmt,
    marker::PhantomData,
    mem::ManuallyDrop,
    num::NonZeroUsize,
    ptr::NonNull,
};

use crate::{
    layout::{NodeHeader, PtrMetadata, PtrWithMetadata},
    utils::{longest_common_prefix, memchr_c_char, strip_prefix, to_string_lossy},
};

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
pub struct Node<Data> {
    /// # Safety invariants
    ///
    /// 1. The layout of the buffer behind this pointer matches the layout mandated by its header.
    /// 2. All node fields are correctly initialized.
    /// 3. The buffer behind this pointer was allocated using the global allocator.
    pub(crate) ptr: std::ptr::NonNull<NodeHeader>,
    pub(crate) _phantom: PhantomData<Data>,
}

impl<Data: Clone> Clone for Node<Data> {
    fn clone(&self) -> Self {
        // Allocate a new buffer with the same layout of the node we're cloning.
        let mut new_ptr = self.metadata().allocate();

        // SAFETY:
        // - We have exclusive access to the buffer that `new_ptr` points to,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_header(*self.header()) };

        // Copy the label.
        //
        // SAFETY:
        // 1. The capacity of the label buffer matches the length of the label, since
        //    we used the same header to compute the required layout.
        // 2. The two buffers don't overlap. The destination buffer was freshly allocated
        //    earlier in this function.
        // 3. We have exclusive access to the destination buffer,
        //    since it was allocated earlier in this function.
        unsafe { new_ptr.label().copy_from_slice_nonoverlapping(self.label()) };

        // Copy the children first bytes.
        //
        // SAFETY:
        // 1. The capacity of the destination buffer matches the length of the source, since
        //    we used the same header to compute the required layout.
        // 2. The two buffers don't overlap. The destination buffer was freshly allocated
        //    earlier in this function.
        // 3. We have exclusive access to the destination buffer,
        //    since it was allocated earlier in this function.
        unsafe {
            new_ptr
                .children_first_bytes()
                .copy_from_slice_nonoverlapping(self.children_first_bytes())
        };

        // Clone the children
        {
            let mut next_ptr = new_ptr.children_ptr();
            for child in self.children() {
                // SAFETY:
                // - The destination data is all contained within a single allocation.
                // - We have exclusive access to the destination buffer,
                //   since it was allocated earlier in this function.
                // - The destination pointer is well aligned, see 1. in [`PtrMetadata::child_ptr`]
                unsafe { next_ptr.write(child.clone()) };
                // SAFETY:
                // - The offsetted pointer doesn't overflow `isize`, since it is within the bounds
                //   of an allocation for a well-formed `Layout` instance.
                // - The offsetted pointer is within the bounds of the allocation, thanks to
                //   layout we used for the buffer.
                unsafe { next_ptr = next_ptr.add(1) };
            }
        }

        // Clone the value if present
        // SAFETY:
        // - We have exclusive access to the destination buffer,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_value(self.data().cloned()) };

        // SAFETY:
        // - All fields have been initialized.
        unsafe { new_ptr.assume_init() }
    }
}

/// Constructors.
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
    pub(crate) fn downgrade(self) -> PtrWithMetadata<Data> {
        // We don't want the destructor to run, otherwise the buffer
        // will be freed.
        let self_ = ManuallyDrop::new(self);
        // SAFETY:
        // - The buffer size+alignment and the metadata match.
        unsafe { PtrWithMetadata::new(self_.ptr, self_.metadata()) }
    }

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
    pub(crate) unsafe fn new_unchecked<const N: usize>(
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
        // - The destination range is all contained within a single allocation,
        //   since the length of the children array we are writing matches the number of children
        //   expected by the header of the node we allocated.
        // - We have exclusive access to the destination buffer, since it was allocated earlier in this function.
        // - The destination pointer is well aligned, see 1. in [`PtrMetadata::children_ptr`]
        unsafe {
            new_ptr.children_ptr().cast().write(children);
        };

        // Set the value.
        // SAFETY:
        // - We have exclusive access to the destination buffer,
        //   since it was allocated earlier in this function.
        unsafe { new_ptr.write_value(value) };

        // SAFETY:
        // - All fields have been initialized, we can now safely return the newly created node.
        unsafe { new_ptr.assume_init() }
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
    pub unsafe fn split_unchecked(
        mut self,
        offset: usize,
        extra_child: Option<Node<Data>>,
    ) -> Self {
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
            child_ptr
                .children_ptr()
                .copy_from_nonoverlapping(old_ptr.children_ptr(), child_header.n_children as usize)
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
            // - The destination range is all contained within newly allocated buffer.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            // - The destination pointer is well aligned, see 1. in [`PtrMetadata::children_ptr`]
            unsafe { new_ptr.children_ptr().cast().write(children) };
        } else {
            // We add the newly created child node to the node's children.

            // SAFETY:
            // - The number of children matches the buffer capacity.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            unsafe { new_ptr.children_first_bytes().write([child.label()[0]]) };
            // SAFETY:
            // - The destination range is all contained within newly allocated buffer.
            // - We have exclusive access to the destination buffer, since it was (re)allocated earlier in this function.
            // - The destination pointer is well aligned, see 1. in [`PtrMetadata::children_ptr`]
            unsafe { new_ptr.children_ptr().write(child) };
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
    pub fn prepend(mut self, prefix: &[c_char]) -> Self {
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
                .children_ptr()
                .copy_from(old_ptr.children_ptr(), new_header.n_children as usize);
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
    pub unsafe fn add_child_unchecked(mut self, new_child: Node<Data>, i: usize) -> Self {
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
                let old_i_th = unsafe { old_ptr.children_ptr().add(i) };
                // SAFETY:
                // The offsetted pointer is within bounds because the caller guarantees
                // that `i` is strictly smaller than `old_n_children` + 1.
                let new_i_plus_1_th = unsafe { new_ptr.children_ptr().add(i + 1) };
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
                let new_i = unsafe { new_ptr.children_ptr().add(i) };
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
            unsafe { new_ptr.children_ptr().copy_from(old_ptr.children_ptr(), i) };
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
    pub unsafe fn remove_child_unchecked(mut self, i: usize) -> Self {
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
            // which would be past the end of the array according to the new (shrinked) layout.
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
            unsafe { new_ptr.children_ptr().copy_from(old_ptr.children_ptr(), i) };

            // Drop the child we are removing
            {
                // SAFETY:
                // - The caller guarantees that `i` is strictly smaller than `old_n_children`.
                let old_i_th = unsafe { old_ptr.children_ptr().add(i) };
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
                let i_th_ptr = unsafe { new_ptr.children_ptr().add(i) };
                // SAFETY:
                // - `i + 1` is smaller than or equal to `old_n_children`, since the caller guarantees
                //   that `i` is strictly smaller than `old_n_children`.
                let i_plus_1_ptr = unsafe { old_ptr.children_ptr().add(i + 1) };
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

/// Accessor methods.
impl<Data> Node<Data> {
    /// Returns a reference to the header for this node.
    #[inline]
    fn header(&self) -> &NodeHeader {
        // SAFETY:
        // - The header field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        unsafe { self.ptr.as_ref() }
    }

    /// Returns the layout and field offsets for the allocated buffer backing this node.
    fn metadata(&self) -> PtrMetadata<Data> {
        self.header().metadata()
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
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let label_ptr = unsafe { PtrMetadata::<Data>::label_ptr(self.ptr) };
        // SAFETY:
        // - The label field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        unsafe { std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize) }
    }

    /// Returns a mutable reference to the data associated with this node, if any.
    pub fn data_mut(&mut self) -> &mut Option<Data> {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let mut data_ptr = unsafe { self.metadata().value_ptr(self.ptr) };
        // SAFETY:
        // - The data field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - We have exclusive access to the data field since this method takes a mutable reference to `self`.
        unsafe { data_ptr.as_mut() }
    }

    /// Returns a reference to the data associated with this node, if any.
    pub fn data(&self) -> Option<&Data> {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let data_ptr = unsafe { self.metadata().value_ptr(self.ptr) };
        // SAFETY:
        // - The data field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        let data: &Option<Data> = unsafe { data_ptr.as_ref() };
        data.as_ref()
    }

    /// Returns a reference to the children of this node.
    ///
    /// # Invariants
    ///
    /// The index of a child in this array matches the index of its first byte
    /// in the array returned by [`Self::children_first_bytes`].
    pub fn children(&self) -> &[Node<Data>] {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let children_ptr = unsafe { self.metadata().children_ptr(self.ptr) };
        // SAFETY:
        // - The children field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        unsafe { std::slice::from_raw_parts(children_ptr.as_ptr(), self.n_children() as usize) }
    }

    /// Returns a mutable reference to the children of this node.
    ///
    /// # Invariants
    ///
    /// The index of a child in this array matches the index of its first byte
    /// in the array returned by [`Self::children_first_bytes`].
    pub fn children_mut(&mut self) -> &mut [Node<Data>] {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let children_ptr = unsafe { self.metadata().children_ptr(self.ptr) };
        // SAFETY:
        // - The children field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        // - We have exclusive access to the children field since this method takes a mutable reference to `self`.
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
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let ptr = unsafe { self.metadata().child_first_bytes_ptr(self.ptr) };
        // SAFETY:
        // - The field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
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
            match longest_common_prefix(current.label(), key) {
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
                    // SAFETY:
                    // - Both `key` and `current.label()` are at least one byte long,
                    //   since `longest_common_prefix` found that their `0`th bytes differ.
                    let new_root = unsafe { Node::new_unchecked(&[], children, None) };

                    std::mem::forget(std::mem::replace(current, new_root));
                    break;
                }
                Some((equal_up_to, _)) => {
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
                    let old_root = std::mem::replace(current, placeholder());

                    // SAFETY:
                    // - `key` is at least `equal_up_to` bytes long, since `longest_common_prefix`
                    //   found that its `equal_up_to` byte differed from the corresponding byte in
                    //   the current label.
                    let (_, new_child_suffix) = unsafe { key.split_at_unchecked(equal_up_to) };
                    let new_child = Node::new_leaf(new_child_suffix, Some(f(None)));
                    // SAFETY:
                    // - `old_root.label()` is at least `equal_up_to` bytes long, since `longest_common_prefix`
                    //   found that its `equal_up_to` byte differed from the corresponding byte in
                    //   `key`.
                    let new_root =
                        unsafe { old_root.split_unchecked(equal_up_to, Some(new_child)) };

                    std::mem::forget(std::mem::replace(current, new_root));
                    break;
                }
                None => {
                    match key.len().cmp(&(current.label_len() as usize)) {
                        Ordering::Less => {
                            // The key we want to insert is a strict prefix of the current node's label.
                            // Therefore we need to insert a new _parent_ node.
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
                            let old_root = std::mem::replace(current, placeholder());

                            // SAFETY:
                            // - In this branch, `old_root.label()` is strictly longer than `key`,
                            //   so `key.len()` is in range for `old_root.label()`.
                            let mut new_root = unsafe { old_root.split_unchecked(key.len(), None) };
                            *new_root.data_mut() = Some(f(None));

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

                            // SAFETY:
                            // - In this branch, `key` is strictly longer than `current.label()`,
                            //   so `current.label_len()` is in range for `key`.
                            key = unsafe { key.get_unchecked(current.label_len() as usize..) };
                            let first_byte = key[0];
                            match current.child_index_starting_with(first_byte) {
                                Some(i) => {
                                    current =
                                        // SAFETY:
                                        // - The index returned by `child_index_starting_with` is
                                        //   always in range for the children pointers array.
                                        unsafe { current.children_mut().get_unchecked_mut(i) };
                                    // Recursion!
                                    continue;
                                }
                                None => {
                                    let insertion_index = current
                                        .children_first_bytes()
                                        .binary_search(&first_byte)
                                        // We know we won't find match at this point.
                                        .unwrap_err();

                                    let root = std::mem::replace(current, placeholder());
                                    let new_child = Node::new_leaf(key, Some(f(None)));
                                    // SAFETY:
                                    // - The index returned by `binary_search` is
                                    //   never greater than the length of the searched array.
                                    let old_root = unsafe {
                                        root.add_child_unchecked(new_child, insertion_index)
                                    };

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
            current = current.child_starting_with(*first_byte)?;
        }
    }

    /// Get a reference to the child node whose label starts with the given byte.
    /// Returns `None` if there is no such child.
    pub fn child_starting_with(&self, c: c_char) -> Option<&Node<Data>> {
        let i = self.child_index_starting_with(c)?;
        // SAFETY:
        // Guaranteed by invariant 1. in [`Self::child_index_starting_with`].
        Some(unsafe { self.children().get_unchecked(i) })
    }

    /// Get the index of the child node whose label starts with the given byte.
    /// Returns `None` if there is no such child.
    ///
    /// # Invariants
    ///
    /// 1. The index returned by this function is guaranteed to be within
    ///    the bounds of the children pointers array and the children
    ///    first bytes array.
    #[inline]
    pub fn child_index_starting_with(&self, c: c_char) -> Option<usize> {
        memchr_c_char(c, self.children_first_bytes())
    }

    /// Remove the descendant of this node that matches the given key, if any.
    ///
    /// Returns the data associated with the removed node, if any.
    pub fn remove_descendant(&mut self, key: &[c_char]) -> Option<Data> {
        // Find the index of child whose label starts with the first byte of the key,
        // as well as the child itself.
        // If the we find none, there's nothing to remove.
        // Note that `key.first()?` will cause this function to return None if the key is empty.
        let child_index = self.child_index_starting_with(*key.first()?)?;
        let child = &mut self.children_mut()[child_index];

        let suffix = strip_prefix(key, child.label())?;

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
                // SAFETY:
                // Guaranteed by invariant 1. in [`Self::child_index_starting_with`].
                current = unsafe { current.remove_child_unchecked(child_index) };
                std::mem::forget(std::mem::replace(self, current));
            } else {
                // If there's a single grandchild,
                // we merge the grandchild into the child.
                child.merge_child_if_possible();
            }

            data
        } else {
            let data = child.remove_descendant(suffix);
            child.merge_child_if_possible();
            data
        }
    }

    /// If `self` has exactly one child, and `self` doesn't hold
    /// any data, merge child into `self`, by moving the child's data and
    /// children into `self`.
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
        // SAFETY:
        // - The pointer was allocated via the same global allocator
        //    we are invoking via `dealloc` (see invariant 3. in [`Self::ptr`])
        // - `old_self.metadata().layout()` is the same layout that was used
        //   to allocate the buffer (see invariant 1. in [`Self::ptr`])
        unsafe {
            dealloc(old_self.ptr.as_ptr().cast(), old_self.metadata().layout());
        }
    }

    /// The memory usage of this node and his descendants, in bytes.
    pub fn mem_usage(&self) -> usize {
        self.metadata().layout().size()
            + self.children().iter().map(|c| c.mem_usage()).sum::<usize>()
    }

    /// The number of descendants of this node, plus 1.
    pub fn n_nodes(&self) -> usize {
        self.n_children() as usize + self.children().iter().map(|c| c.n_nodes()).sum::<usize>()
    }
}

impl<Data> Drop for Node<Data> {
    fn drop(&mut self) {
        let layout = self.metadata().layout();
        // SAFETY:
        // - We have exclusive access to buffer.
        // - The field is correctly initialized (see invariant 2. in [`Self::ptr`])
        // - The pointer is valid since it comes from a reference.
        unsafe { std::ptr::drop_in_place(self.data_mut()) };
        // SAFETY:
        // - We have exclusive access to buffer.
        // - The field is correctly initialized (see invariant 2. in [`Self::ptr`])
        // - The pointer is valid since it comes from a reference.
        unsafe { std::ptr::drop_in_place(self.children_mut()) };

        // SAFETY:
        // - The pointer was allocated via the same global allocator
        //    we are invoking via `dealloc` (see invariant 3. in [`Self::ptr`])
        // - `layout` is the same layout that was used
        //   to allocate the buffer (see invariant 1. in [`Self::ptr`])
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
