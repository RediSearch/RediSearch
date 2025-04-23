//! Primitives to manage the expected memory layout of the heap-allocated buffer for each [`Node`].
//!
//! Check out [`NodeLayout`]'s documentation for more details.
use crate::node::Node;
use std::{alloc::Layout, ffi::c_char, marker::PhantomData, num::NonZeroUsize, ptr::NonNull};

#[repr(C)]
#[derive(Clone, Copy, Debug)]
/// The first field in the allocated buffer for a [`Node`].
///
/// [`NodeHeader::layout`] can be used to compute the layout of
/// the buffer allocated for a [`Node`].
pub(super) struct NodeHeader {
    /// The length of the label associated with this node.
    ///
    /// It can be 0, for the root node.
    pub label_len: u16,
    /// The number of children of this node.
    pub n_children: u8,
}

impl NodeHeader {
    /// Computes the metadata (layout and field offsets) required to
    /// work with a [`Node`] of a given label length and number of children.
    pub(super) fn metadata<Data>(self) -> PtrMetadata<Data> {
        PtrMetadata::<Data>::compute(self)
    }
}

/// Information about the layout of the buffer allocated for a [`Node`]
/// and the offset of each field within that buffer.
///
/// [`Node`] is a custom dynamically-sized type (DST)—a type whose size and
/// alignment can't be determined at compile-time.
///
/// # Our goals
///
/// Our goal is to be as fast as possible while minimizing memory usage.
///
/// All the data associated with a node is stored, inline, within a single
/// allocated buffer.
/// This strategy allows us to:
///
/// - Minimize pointer indirection when performing operations on the node.
/// - Minimize the amount of memory spent on "metadata". E.g. we would need
///   to store a pointer to the label, a pointer to the children, and a pointer to
///   the array of first bytes.
///
/// It comes at the cost of increased complexity (see later section) as well as
/// increased number of allocations/reallocations (since we don't have spare capacity).
/// Benchmarks have shown that the performance penalty of this strategy
/// is within reasonable bounds.
///
/// # Why do we need to manage our own layout?
///
/// If we tried to define [`Node`] as a "normal" Rust struct, it'd look like this:
///
/// ```rust,compile_fail
/// #[repr(C)]
/// struct Node<Data> {
///     label_len: u16,
///     n_children: u8,
///     label: [u8; self.label_len],
///     children_first_bytes: [u8; self.n_children],
///     children: [NonNull<std::ffi::c_void>; self.n_children],
///     data: Option<Data>
/// }
/// ```
///
/// Unfortunately, the Rust compiler can't reason about it since the length of those
/// arrays is only known at runtime. We could try downgrading to slices:
///
/// ```rust,compile_fail
/// #[repr(C)]
/// struct Node<Data> {
///     label_len: u16,
///     n_children: u8,
///     label: [u8],
///     children_first_bytes: [u8],
///     children: [NonNull<std::ffi::c_void>],
///     data: Option<Data>
/// }
/// ```
///
/// but it wouldn't be enough. The Rust compiler wants to know the _offset_ of
/// each field at compile-time, and that's only possible if you have at most one
/// dynamically-sized field and it occurs last.
///
/// Unfortunately, our [`Node`] contains _multiple_ fields whose size is not known at
/// compile-time: the label, the first bytes of children, and the children pointers.
///
/// So, here we are, forced to manage our memory layout manually!
///
/// # Fields ordering
///
/// The field order has been carefully chosen to minimize the amount of padding required
/// to align our type.
///
/// We have optimized, in particular, for `Data=NonNull<*mut c_void>`, the scenario we have in
/// [`crate::ffi`]. In that case, padding is minimal: `(2 + 1 + label_len + n_children) % 8`,
/// located between the end of the array of children first bytes and the start of the
/// array of children pointers.
///
/// # Terminology
///
/// This struct is named `PtrMetadata` since it plays the same role of
/// [`Pointee::Metadata`](https://doc.rust-lang.org/std/ptr/trait.Pointee.html),
/// an unstable feature to streamline custom DSTs.
///
/// ## Further reading
///
/// - [Rust reference on DSTs](https://doc.rust-lang.org/reference/dynamically-sized-types.html)
pub(super) struct PtrMetadata<Data> {
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
    // Capture the `Data` type to ensure we can refer to its size and
    // alignment in our offset calculations.
    _phantom: PhantomData<Data>,
    #[cfg(debug_assertions)]
    /// The length of the label associated with this node.
    ///
    /// This field is only used in builds with debug assertions enabled
    /// to ensure that some of our invariants/safety preconditions
    /// are indeed upheld.
    label_len: usize,
    #[cfg(debug_assertions)]
    /// The number of children for this node.
    ///
    /// This field is only used in builds with debug assertions enabled
    /// to ensure that some of our invariants/safety preconditions
    /// are indeed upheld.
    n_children: usize,
}

impl<Data> PtrMetadata<Data> {
    /// The offset (in bytes) of the label associated with this node,
    /// relative to the beginning of the allocated buffer.
    ///
    /// Since the label comes straight after the node header, we can
    /// compute its offset at compile-time.
    const LABEL_OFFSET: usize = const {
        let layout = Layout::new::<NodeHeader>();

        // The offset doesn't depend on the actual number of children.
        let Ok(label) = Layout::array::<c_char>(1) else {
            // `1 (array size)` is guaranteed to be less than `isize::MAX`.
            unreachable!()
        };
        let Ok((_, offset)) = layout.extend(label) else {
            // `2 (header) + 1 (label)` is guaranteed to be less than `isize::MAX`.
            unreachable!()
        };
        offset
    };

    /// Compute the layout of a node, given its header.
    pub const fn compute(header: NodeHeader) -> Self {
        let layout = Layout::new::<NodeHeader>();

        // Node label
        let Ok(label) = Layout::array::<c_char>(header.label_len as usize) else {
            // The label length is a `u16`, so we will never overflow `isize::MAX` here
            // since `u16::MAX` is less than `isize::MAX`.
            unreachable!()
        };
        let Ok((layout, _)) = layout.extend(label) else {
            // `2 + u16::MAX` is guaranteed to be less than `isize::MAX`.
            unreachable!()
        };

        // Children first-bytes
        let Ok(first_bytes) = Layout::array::<c_char>(header.n_children as usize) else {
            // The number of children is a `u8`, so we will never overflow `isize::MAX` here
            // since `u8::MAX` is less than `isize::MAX`.
            unreachable!()
        };
        let Ok((layout, children_first_bytes_offset)) = layout.extend(first_bytes) else {
            // `2 + u16::MAX + u8::MAX` is guaranteed to be less than `isize::MAX`.
            unreachable!()
        };

        // Children pointers
        let Ok(children_pointers) = Layout::array::<Node<Data>>(header.n_children as usize) else {
            // The number of children is a `u8`, so we will never overflow `isize::MAX` here
            // since `u8::MAX * size_of::<usize>` is less than `isize::MAX`.
            unreachable!()
        };
        let Ok((layout, children_offset)) = layout.extend(children_pointers) else {
            // `2 + u16::MAX + u8::MAX + u8::MAX * size_of::<usize>` is guaranteed to be less
            // than `isize::MAX`.
            unreachable!()
        };

        // Value
        let Ok((layout, value_offset)) = layout.extend(Layout::new::<Option<Data>>()) else {
            // This may happen for a comically large `Data` type.
            panic!("Capacity overflow when adding the node value field to the layout of the node");
        };

        Self {
            layout: layout.pad_to_align(),
            children_first_bytes_offset,
            children_offset,
            value_offset,
            _phantom: PhantomData,
            #[cfg(debug_assertions)]
            label_len: header.label_len as usize,
            #[cfg(debug_assertions)]
            n_children: header.n_children as usize,
        }
    }

    /// Allocate a new buffer using [`Self::layout`] as its memory layout.
    pub(super) fn allocate(self) -> PtrWithMetadata<Data> {
        let ptr = {
            // SAFETY:
            // `layout.size()` is greater than zero, see 1. in [`AllocationInfo::layout`]
            unsafe { std::alloc::alloc(self.layout()) as *mut NodeHeader }
        };
        let Some(ptr) = NonNull::new(ptr) else {
            std::alloc::handle_alloc_error(self.layout())
        };
        // SAFETY:
        // 1. We allocated the buffer behind `ptr` using the global allocator, just above.
        // 2.
        // +
        // 3. The layout used to allocate the buffer is exactly the layout mandated by
        //    the metadata we are attaching to it.
        unsafe { PtrWithMetadata::new(ptr, self) }
    }

    /// The size and alignment of the allocated buffer for this node.
    ///
    /// # Invariants
    ///
    /// 1. The size of the layout is always greater than zero, since it includes
    ///    the size of a [`NodeHeader`], which is known to be at least three bytes.
    /// 2. The size of the layout includes the required trailing padding (if any)
    ///    to ensure that the layout is properly aligned.
    pub const fn layout(&self) -> Layout {
        self.layout
    }

    /// A pointer to the first element of the child first-bytes array for this node.
    ///
    /// # Invariants
    ///
    /// 1. If the safety preconditions are met, the returned pointer is well-aligned
    ///    to write/read a slice of `c_char`.
    ///
    /// # Safety
    ///
    /// a. `header_ptr` must point to an allocation with the same alignment of [`Self::layout`].
    /// b. `header_ptr` must point to an allocation that's big enough to contain the offsetted pointer
    ///    returned by this function.
    ///
    /// The requirements are automatically verified if `header_ptr` is backed by an allocation
    /// created using the layout returned by [`Self::layout`].
    pub const unsafe fn child_first_bytes_ptr(
        &self,
        header_ptr: NonNull<NodeHeader>,
    ) -> NonNull<c_char> {
        // SAFETY:
        // The safety preconditions must be verified by the caller.
        unsafe { header_ptr.byte_offset(self.children_first_bytes_offset as isize) }.cast()
    }

    /// A pointer to the first element of the child pointer array for this node.
    ///
    /// # Invariants
    ///
    /// 1. If the safety preconditions are met, the returned pointer is well-aligned
    ///    to write/read a slice of `NonNull<Node<Data>>`.
    ///
    /// # Safety
    ///
    /// a. `header_ptr` must point to an allocation with the same alignment of [`Self::layout`].
    /// b. `header_ptr` must point to an allocation that's big enough to contain the offsetted pointer
    ///    returned by this function.
    ///
    /// The requirements are automatically verified if `header_ptr` is backed by an allocation
    /// created using the layout returned by [`Self::layout`].
    pub const unsafe fn children_ptr(
        &self,
        header_ptr: NonNull<NodeHeader>,
    ) -> NonNull<Node<Data>> {
        // SAFETY:
        // The safety preconditions must be verified by the caller.
        unsafe { header_ptr.byte_offset(self.children_offset as isize) }.cast()
    }

    /// A pointer to the label associated with this node.
    ///
    /// # Invariants
    ///
    /// 1. If the safety preconditions are met, the returned pointer is well-aligned
    ///    to write/read a slice of `c_char`s.
    ///
    /// # Safety
    ///
    /// a. `header_ptr` must point to an allocation with the same alignment of [`Self::layout`].
    /// b. `header_ptr` must point to an allocation that's big enough to contain the offsetted pointer
    ///    returned by this function.
    ///
    /// The requirements are automatically verified if `header_ptr` is backed by an allocation
    /// created using the layout returned by [`Self::layout`].
    pub const unsafe fn label_ptr(header_ptr: NonNull<NodeHeader>) -> NonNull<c_char> {
        // SAFETY:
        // The safety preconditions must be verified by the caller.
        unsafe { header_ptr.byte_offset(Self::LABEL_OFFSET as isize) }.cast()
    }

    /// A pointer to value stored in this node.
    ///
    /// # Invariants
    ///
    /// 1. If the safety preconditions are met, the returned pointer is well-aligned
    ///    to write/read an instance of `Option<Data>`.
    ///
    /// # Safety
    ///
    /// a. `header_ptr` must point to an allocation with the same alignment of [`Self::layout`].
    /// b. `header_ptr` must point to an allocation that's big enough to contain the offsetted pointer
    ///    returned by this function.
    ///
    /// The requirements are automatically verified if `header_ptr` is backed by an allocation
    pub const unsafe fn value_ptr(&self, header_ptr: NonNull<NodeHeader>) -> NonNull<Option<Data>> {
        // SAFETY:
        // The safety preconditions must be verified by the caller.
        unsafe { header_ptr.byte_offset(self.value_offset as isize) }.cast()
    }
}

/// Ties together a pointer and a compatible metadata.
///
/// # Invariants
///
/// 1. [`Self::ptr`] points to a single allocation, performed via the global allocator.
/// 2. The size of the allocation behind [`Self::ptr`] is equal or greater than the size dictated by [`Self::metadata`].
/// 3. The alignment of the allocation behind [`Self::ptr`] matches the alignment dictated by [`Self::metadata`].
///
/// Requirement 3. is automatically satisfied for any layout generated via a [`NodeHeader`] instance, since all nodes
/// have the same alignment.
///
/// # We could be stricter, but we chose not to
///
/// We could require [`Self::ptr`] to point at an allocation whose size is **equal** to the size dictated by [`Self::metadata`].
/// It'd be a stricter requirement, but we chose to weaken it to allow us to handle more scenarios (namely, reallocations) via
/// the same abstraction.
pub(super) struct PtrWithMetadata<Data> {
    ptr: NonNull<NodeHeader>,
    metadata: PtrMetadata<Data>,
}

impl<Data> PtrWithMetadata<Data> {
    /// Creates a new `PtrWithMetadata` instance.
    ///
    /// # Safety
    ///
    /// `ptr` must satisfy the safety invariants enumerated in [`Self`]'s documentation.
    pub(super) const unsafe fn new(ptr: NonNull<NodeHeader>, metadata: PtrMetadata<Data>) -> Self {
        Self { ptr, metadata }
    }

    /// The pointer to the beginning of the allocated buffer.
    pub(super) fn ptr(&self) -> NonNull<NodeHeader> {
        self.ptr
    }

    /// Write a header value to the expected offset.
    ///
    /// It will overwrite any value previously stored at the offset, without
    /// running their destructor.
    ///
    /// # Safety
    ///
    /// 1. You must have exclusive access to the buffer that [`Self::ptr`] points to.
    pub(super) unsafe fn write_header(&mut self, header: NodeHeader) {
        // SAFETY:
        // - The data we are writing falls within the boundaries of a single allocated buffer,
        //   thanks to 1. and 2. in `Self`'s documentation.
        // - The caller guarantees that they have exclusive access to the buffer we are writing to.
        // - The pointer is well aligned for the header type, thanks to 3. in `Self`'s documentation.
        unsafe { self.ptr.write(header) }
    }

    /// Manipulate the buffer portion related to this node's label.
    pub(super) fn label(&self) -> LabelBuffer<Data> {
        LabelBuffer(self)
    }

    /// Manipulate the buffer portion related to this node's children first bytes.
    pub(super) fn children_first_bytes(&self) -> ChildrenFirstBytesBuffer<Data> {
        ChildrenFirstBytesBuffer(self)
    }

    /// Manipulate the buffer portion related to this node's children.
    pub(super) fn children(&self) -> ChildrenBuffer<Data> {
        ChildrenBuffer(self)
    }

    /// Write a value to the expected offset.
    ///
    /// It will overwrite any value previously stored at the offset, without
    /// running their destructor.
    ///
    /// # Safety
    ///
    /// 1. You must have exclusive access to the buffer that [`Self::ptr`] points to.
    pub(super) unsafe fn write_value(&mut self, value: Option<Data>) {
        // SAFETY: This is safe because:
        // 1. `self.ptr` was verified to be properly allocated with the correct layout
        //    when this struct was created (see safety invariant #1).
        // 2. The metadata's layout guarantees proper alignment for this field.
        let value_ptr = unsafe { self.metadata.value_ptr(self.ptr) };
        // SAFETY:
        // - The data we are writing falls within the boundaries of a single allocated buffer,
        //   thanks to 1. and 2. in `Self`'s documentation.
        // - The caller guarantees that they have exclusive access to the buffer we are writing to.
        // - The pointer is well aligned for the `Option<Data>` type, thanks to 3. in `Self`'s documentation.
        unsafe { value_ptr.write(value) }
    }

    /// Promote the non-null pointer to a well-formed [`Node`] instance.
    ///
    /// # Safety
    ///
    /// 1. All fields must have been properly initialized.
    pub(super) unsafe fn assume_init(self) -> Node<Data> {
        Node {
            ptr: self.ptr,
            _phantom: PhantomData,
        }
    }

    /// Decompose `self` into its constituent parts.
    pub(super) fn into_parts(self) -> (NonNull<NodeHeader>, PtrMetadata<Data>) {
        (self.ptr, self.metadata)
    }
}

/// A struct that groups together methods to manipulate the buffer allocated
/// to store this node's label.
pub(super) struct LabelBuffer<'a, Data>(&'a PtrWithMetadata<Data>);

impl<Data> LabelBuffer<'_, Data> {
    /// Returns a pointer to beginning of the label buffer.
    ///
    /// # Invariants
    ///
    /// 1. The returned pointer is well-aligned to write/read a slice of `c_char`s.
    fn ptr(&self) -> NonNull<c_char> {
        // SAFETY: This is safe because:
        // 1. `self.ptr` was verified to be properly allocated with the correct layout
        //    when this struct was created (see safety invariant #1).
        // 2. The metadata's layout guarantees proper alignment for this field.
        unsafe { PtrMetadata::<Data>::label_ptr(self.0.ptr) }
    }

    /// Copy the contents of `src` into the label buffer.
    ///
    /// # Safety
    ///
    /// 1. The number of elements in `src` must be less than or equal to the capacity of the label buffer.
    /// 2. `src` and the label buffer must not overlap.
    /// 3. You have exclusive access to the label buffer.
    pub(super) unsafe fn copy_from_slice_nonoverlapping(&mut self, src: &[c_char]) {
        #[cfg(debug_assertions)]
        {
            assert!(
                src.len() <= self.0.metadata.label_len,
                "The length of the source slice exceeds the label buffer capacity"
            );
        }
        // There is no risk of a double-free on drop because `[c_char]` is `Copy`.
        //
        // SAFETY:
        // - The source data is all contained within a single allocation, since it's a well-formed slice.
        // - The destination data is all contained within a single allocation, thanks to 1.
        // - We have exclusive access to the destination buffer, thanks to 3.
        // - No one else is mutating the source buffer, since we hold a `&` reference to it.
        // - Both source and destination pointers are well aligned, see 1. in [`LabelBuffer::ptr`]
        // - The two buffers don't overlap, thanks to 2.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr().as_ptr(), src.len()) };
    }

    /// Shifts `n_elements` elements to the right by `offset` positions in the label buffer.
    ///
    /// The elements in the range `[0, offset)` are left untouched. This operation is safe, since
    /// `c_char` (our label type) is `Copy`.
    ///
    /// # Example
    ///
    /// Shift right with offset 3 and n_elements 2:
    ///
    /// ```text
    ///
    /// Old state: [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    ///
    /// New state: [ 0, 1, 2, 0, 1, 5, 6, 7, 8, 9]
    ///                       ^^^^
    ///                       The old elements have been overwritten.
    /// ```
    ///
    /// # Safety
    ///
    /// 1. `offset` + `n_elements` must not exceed the capacity of the label buffer.
    /// 2. You must have exclusive access to the label buffer.
    /// 3. The first `n_elements` elements in the buffer must be correctly initialized.
    pub(super) unsafe fn shift_right(&mut self, offset: usize, n_elements: usize) {
        #[cfg(debug_assertions)]
        {
            assert!(
                offset + n_elements <= self.0.metadata.label_len,
                "The shift operation would write beyond the end of the label buffer"
            );
        }
        let ptr = self.ptr();
        // SAFETY:
        // The offsetted pointer is within bounds of the label buffer thanks to 1.
        let shifted_ptr = unsafe { ptr.add(offset) };
        // SAFETY:
        // - The source is valid for reads of `n_elements` elements, thanks to 1.
        // - The destination is valid for writes of `n_elements` elements, thanks to 1,
        //   and it isn't invalidated by reading the source.
        // - The source and destination pointers are properly aligned,
        //   see 1. in [`LabelBuffer::ptr`].
        unsafe { shifted_ptr.copy_from(ptr, n_elements) };
    }
}

/// A struct that groups together methods to manipulate the buffer allocated
/// to store the first byte of the children of this node.
pub(super) struct ChildrenFirstBytesBuffer<'a, Data>(&'a PtrWithMetadata<Data>);

impl<Data> ChildrenFirstBytesBuffer<'_, Data> {
    /// Returns a pointer to beginning of the children first-bytes buffer.
    ///
    /// # Invariants
    ///
    /// 1. The returned pointer is well-aligned to write/read a slice of `c_char`s.
    pub(super) fn ptr(&self) -> NonNull<c_char> {
        // SAFETY: This is safe because:
        // 1. `self.0.ptr` was verified to be properly allocated with the correct layout
        //    when this struct was created (see safety invariant #1 in `PtrWithMetadata`).
        // 2. The metadata's layout guarantees proper alignment for this field.
        unsafe { self.0.metadata.child_first_bytes_ptr(self.0.ptr) }
    }

    /// Copy the contents of `src` into the children first-bytes buffer.
    ///
    /// # Safety
    ///
    /// 1. The number of elements in `src` must be less than or equal to the capacity of the buffer.
    /// 2. `src` and the destination buffer must not overlap.
    /// 3. You have exclusive access to the children first-bytes buffer.
    pub(super) unsafe fn copy_from_slice_nonoverlapping(&mut self, src: &[c_char]) {
        #[cfg(debug_assertions)]
        {
            assert!(
                src.len() <= self.0.metadata.n_children,
                "The length of the source slice exceeds the children first-bytes buffer capacity"
            );
        }
        // There is no risk of a double-free on drop because `[c_char]` is `Copy`.
        //
        // SAFETY:
        // - The source data is all contained within a single allocation, since it's a well-formed slice.
        // - The destination data is all contained within a single allocation, thanks to 1.
        // - We have exclusive access to the destination buffer, thanks to 3.
        // - No one else is mutating the source buffer, since we hold a `&` reference to it.
        // - Both source and destination pointers are well aligned, see 1. in [`ChildrenFirstBytesBuffer::ptr`]
        // - The two buffers don't overlap, thanks to 2.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr().as_ptr(), src.len()) };
    }

    /// Shifts `n_elements` elements to the left by `by` positions in the children first-bytes buffer.
    ///
    /// This operation copies elements from `[target + by..(target + n_elements - 1) + by]`
    /// to `[target..target + n_elements - 1]`, effectively overwriting the elements
    /// in `[target..target + by]`.
    ///
    /// # Example
    ///
    /// Shift left with target 2, by 1, and n_elements 4:
    ///
    /// ```text
    /// Old state: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    ///                   ^  ^
    ///                   |  |
    ///              target  target+by
    ///
    /// New state: [0, 1, 3, 4, 5, 6, 6, 7, 8, 9]
    ///                   ^^^^^^^^^^
    ///                   n_elements
    /// ```
    ///
    /// # Safety
    ///
    /// 1. `target + by + n_elements` must not exceed the capacity of the buffer.
    /// 2. You must have exclusive access to the children first-bytes buffer.
    /// 3. The elements in `[target + by..(target + n_elements - 1) + by]` must be correctly initialized.
    pub(super) unsafe fn shift_left(&mut self, target: usize, by: NonZeroUsize, n_elements: usize) {
        #[cfg(debug_assertions)]
        {
            assert!(
                target + by.get() + n_elements <= self.0.metadata.n_children,
                "The shift operation would read from beyond the end of the buffer"
            );
        }
        // SAFETY:
        // The offsetted pointer is in bounds, thanks to 1.
        let destination = unsafe { self.ptr().add(target) };
        // SAFETY:
        // The offsetted pointer is in bounds, thanks to 1.
        let source = unsafe { self.ptr().add(target + by.get()) };
        // SAFETY:
        // - The source is valid for reads of `n_elements` elements, thanks to 1. and 3.
        // - The destination is valid for writes of `n_elements` elements, thanks to 1.,
        //   and it isn't invalidated by reading the source.
        // - The source and destination pointers are properly aligned,
        //   see 1. in [`ChildrenFirstBytesBuffer::ptr`].
        unsafe { destination.copy_from(source, n_elements) };
    }

    /// Write the contents of `bytes` into the children first-bytes buffer.
    ///
    /// # Safety
    ///
    /// 1. The number of elements in `bytes` must be less than or equal to the capacity of the buffer.
    /// 2. You have exclusive access to the children first-bytes buffer.
    pub(super) unsafe fn write<const N: usize>(&mut self, bytes: [c_char; N]) {
        #[cfg(debug_assertions)]
        {
            assert!(
                N <= self.0.metadata.n_children,
                "The number of elements must be less than or equal to the number of children for this node."
            );
        }
        // SAFETY:
        // - The pointer is valid for writes of `N` elements, thanks to 1.
        // - The pointer is properly aligned, see 2. in [`ChildrenFirstBytesBuffer::ptr`].
        unsafe { self.ptr().cast().write(bytes) }
    }
}

/// A struct that groups together methods to manipulate the buffer allocated
/// to store the children of this node.
pub(super) struct ChildrenBuffer<'a, Data>(&'a PtrWithMetadata<Data>);

impl<Data> ChildrenBuffer<'_, Data> {
    /// Returns a pointer to beginning of the children buffer.
    ///
    /// # Invariants
    ///
    /// 1. The returned pointer is well-aligned to write/read a slice of `NonNull<Node<Data>>`s.
    pub(super) fn ptr(&self) -> NonNull<Node<Data>> {
        // SAFETY: This is safe because:
        // 1. `self.0.ptr` was verified to be properly allocated with the correct layout
        //    when this struct was created (see safety invariant #1 in `PtrWithMetadata`).
        // 2. The metadata's layout guarantees proper alignment for this field.
        unsafe { self.0.metadata.children_ptr(self.0.ptr) }
    }

    /// Write the contents of `source` into the children buffer.
    ///
    /// # Safety
    ///
    /// 1. The number of elements in `source` must be less than or equal to the capacity of the buffer.
    /// 2. You have exclusive access to the children buffer.
    pub(super) unsafe fn write<const N: usize>(&mut self, source: [Node<Data>; N]) {
        #[cfg(debug_assertions)]
        {
            assert!(
                N <= self.0.metadata.n_children,
                "The number of elements must be less than or equal to the number of children for this node."
            );
        }
        // SAFETY:
        // - The pointer is valid for writes of `N` elements, thanks to 1.
        // - The pointer is properly aligned, see 2. in [`ChildrenBuffer::ptr`].
        unsafe { self.ptr().cast().write(source) }
    }
}
