//! Utilities for computing the layout of allocations.
use super::{header::AllocationHeader, BranchingNode, leaf::LeafNode, Node};
use std::alloc::Layout;
use std::ffi::c_char;

impl<Data> BranchingNode<Data> {
    /// Gets the layout of the allocated memory for a [`BranchingNode<Data>`] with the given capacity.
    pub const fn layout(n_children: usize, label_len: usize) -> Layout {
        let mut layout = Layout::new::<AllocationHeader>();
        // Number of children
        layout = match layout.extend(Layout::new::<u8>()) {
            Ok((layout, _)) => layout,
            Err(_) => panic!("Boom"),
        };

        let Ok(c_char_array) = Layout::array::<c_char>(n_children + label_len) else {
            panic!("Boom")
        };
        layout = match layout.extend(c_char_array) {
            Ok((layout, _)) => layout,
            Err(_) => {
                // The panic message must be known at compile-time if we want `allocation_layout` to be a `const fn`.
                // Therefore we can't capture the error (nor the faulty capacity value) in the panic message.
                panic!(
                    "The size of the allocated buffer for a branching node would exceed `isize::MAX`, \
                which is the maximum size that can be allocated."
                )
            }
        };

        let Ok(children_pointers) = Layout::array::<std::ptr::NonNull<Node<Data>>>(n_children)
        else {
            panic!("Boom")
        };
        layout = match layout.extend(children_pointers) {
            Ok((layout, _)) => layout,
            Err(_) => {
                // The panic message must be known at compile-time if we want `allocation_layout` to be a `const fn`.
                // Therefore we can't capture the error (nor the faulty capacity value) in the panic message.
                panic!(
                    "The size of the allocated buffer for a branching node would exceed `isize::MAX`, \
                which is the maximum size that can be allocated."
                )
            }
        };
        layout.pad_to_align()
    }

    /// Gets the alignment for the allocation owned by a [`BranchingNode<Data>`].
    pub const fn allocation_alignment() -> usize {
        // Alignment doesn't change with capacity, so we can use an arbitrary value.
        // Since:
        // - the capacity value is known at compile-time
        // - `allocation_layout` is a `const` function
        // we can mark `alloc_align` as `const` and be sure that `alloc_align` will be
        // computed at compile time for any `Data` that may end up being used in our
        // program as a type for the children of [`BranchingNode<Data>`].
        Self::layout(1, 1).align()
    }

    /// Gets the padding that must be inserted between the end of the AllocationHeader field
    /// and the start of the elements array to ensure proper alignment.
    ///
    /// # Performance
    ///
    /// This value will be computed at compile time for any `Data` that may end up being used in our
    /// program as a type for the children of a [`BranchingNode<Data>`], since the function is `const`
    /// and takes no runtime arguments.
    pub const fn allocation_header_field_padding() -> usize {
        let alloc_align = Self::allocation_alignment();
        let allocation_header_size = std::mem::size_of::<AllocationHeader>();
        alloc_align.saturating_sub(allocation_header_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocation_header_field_padding() {
        todo!()
    }
}
