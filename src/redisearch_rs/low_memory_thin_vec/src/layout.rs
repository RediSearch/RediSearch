//! Utilities for computing the layout of allocations.
use crate::header::Header;
use std::alloc::Layout;

/// Gets the layout of the allocated memory for a `LowMemoryThinVec<T>` with the given capacity.
pub(crate) const fn allocation_layout<T>(cap: usize) -> Layout {
    let mut vec = Layout::new::<Header>();
    let elements = match Layout::array::<T>(cap) {
        Ok(elements) => elements,
        Err(_) => {
            // The panic message must be known at compile-time if we want `allocation_layout` to be a `const fn`.
            // Therefore we can't capture the error (nor the faulty capacity value) in the panic message.
            panic!(
                "The size of the array of elements within `LowMemoryThinVec<T>` would exceed `isize::MAX`, \
                which is the maximum size that can be allocated."
            )
        }
    };
    vec = match vec.extend(elements) {
        Ok((layout, _)) => layout,
        Err(_) => {
            // The panic message must be known at compile-time if we want `allocation_layout` to be a `const fn`.
            // Therefore we can't capture the error (nor the faulty capacity value) in the panic message.
            panic!(
                "The size of the allocated buffer for `LowMemoryThinVec` would exceed `isize::MAX`, \
                which is the maximum size that can be allocated."
            )
        }
    };
    vec.pad_to_align()
}

/// Gets the alignment for the allocation owned by `LowMemoryThinVec<T>`.
pub(crate) const fn allocation_alignment<T>() -> usize {
    // Alignment doesn't change with capacity, so we can use an arbitrary value.
    // Since:
    // - the capacity value is known at compile-time
    // - `allocation_layout` is a `const` function
    // we can mark `alloc_align` as `const` and be sure that `alloc_align` will be
    // computed at compile time for any `T` that may end up being used in our
    // program as a type for the elements within `LowMemoryThinVec<T>`.
    allocation_layout::<T>(1).align()
}

/// Gets the padding that must be inserted between the end of the header field
/// and the start of the elements array to ensure proper alignment.
///
/// # Performance
///
/// This value will be computed at compile time for any `T` that may end up being used in our
/// program as a type for the elements within `LowMemoryThinVec<T>`, since the function is `const`
/// and takes no runtime arguments.
pub(crate) const fn header_field_padding<T>() -> usize {
    let alloc_align = allocation_alignment::<T>();
    let header_size = std::mem::size_of::<Header>();
    alloc_align.saturating_sub(header_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_field_padding() {
        // Zero header padding needed if storing `u8`s,
        // since the whole allocation is aligned to 2,
        // the alignment of the header.
        assert_eq!(header_field_padding::<u8>(), 0);

        // With `u16` elements, both elements and header have the
        // same alignment, so no padding is needed.
        assert_eq!(header_field_padding::<u16>(), 0);

        // With `u32` elements, the whole allocation is aligned to 4,
        // but the header is 4 bytes long, so no padding is needed.
        assert_eq!(header_field_padding::<u32>(), 0);

        // With `u64` elements, the whole allocation is aligned to 8,
        // so the header needs 4 bytes of padding to be aligned.
        assert_eq!(header_field_padding::<u64>(), 4);

        // With `u128` elements, the whole allocation is aligned to 16,
        // so the header needs 12 bytes of padding to be aligned.
        assert_eq!(header_field_padding::<u128>(), 12);
    }
}
