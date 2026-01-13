//! Utilities for computing the layout of allocations.
use crate::{VecCapacity, header::Header};
use std::alloc::Layout;

/// Gets the layout of the allocated memory for a `LowMemoryThinVec<T, S>` with the given capacity.
pub(crate) const fn allocation_layout<T, S: VecCapacity>(cap: usize) -> Layout {
    let mut vec = Layout::new::<Header<S>>();
    let Ok(elements) = Layout::array::<T>(cap) else {
        // The panic message must be known at compile-time if we want `allocation_layout` to be a `const fn`.
        // Therefore we can't capture the error (nor the faulty capacity value) in the panic message.
        panic!(
            "The size of the array of elements within `LowMemoryThinVec<T>` would exceed `isize::MAX`, \
                        which is the maximum size that can be allocated."
        )
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

/// Gets the alignment for the allocation owned by `LowMemoryThinVec<T, S>`.
pub(crate) const fn allocation_alignment<T, S: VecCapacity>() -> usize {
    // Alignment doesn't change with capacity, so we can use an arbitrary value.
    // Since:
    // - the capacity value is known at compile-time
    // - `allocation_layout` is a `const` function
    // we can mark `alloc_align` as `const` and be sure that `alloc_align` will be
    // computed at compile time for any `T` that may end up being used in our
    // program as a type for the elements within `LowMemoryThinVec<T, S>`.
    allocation_layout::<T, S>(1).align()
}

/// Gets the padding that must be inserted between the end of the header field
/// and the start of the elements array to ensure proper alignment.
///
/// # Performance
///
/// This value will be computed at compile time for any `T` and `S` that may end up being used in our
/// program as types within `LowMemoryThinVec<T, S>`, since the function is `const`
/// and takes no runtime arguments.
pub(crate) const fn header_field_padding<T, S: VecCapacity>() -> usize {
    let alloc_align = allocation_alignment::<T, S>();
    let header_size = std::mem::size_of::<Header<S>>();
    alloc_align.saturating_sub(header_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_field_padding_u16() {
        // With u16 size type (Header is 4 bytes)

        // Zero header padding needed if storing `u8`s,
        // since the whole allocation is aligned to 4 (header alignment),
        assert_eq!(header_field_padding::<u8, u16>(), 0);

        // With `u16` elements, both elements and header have the
        // same alignment, so no padding is needed.
        assert_eq!(header_field_padding::<u16, u16>(), 0);

        // With `u32` elements, the whole allocation is aligned to 4,
        // but the header is 4 bytes long, so no padding is needed.
        assert_eq!(header_field_padding::<u32, u16>(), 0);

        // With `u64` elements, the whole allocation is aligned to 8,
        // so the header needs 4 bytes of padding to be aligned.
        assert_eq!(header_field_padding::<u64, u16>(), 4);

        // With `u128` elements, the whole allocation is aligned to 16,
        // so the header needs 12 bytes of padding to be aligned.
        assert_eq!(header_field_padding::<u128, u16>(), 12);
    }

    #[test]
    fn test_header_field_padding_u8() {
        // With u8 size type (Header is 2 bytes)

        // Zero header padding needed if storing `u8`s.
        assert_eq!(header_field_padding::<u8, u8>(), 0);

        // With `u16` elements, alignment is 2, header is 2 bytes, no padding.
        assert_eq!(header_field_padding::<u16, u8>(), 0);

        // With `u32` elements, alignment is 4, header is 2 bytes, 2 bytes padding.
        assert_eq!(header_field_padding::<u32, u8>(), 2);

        // With `u64` elements, alignment is 8, header is 2 bytes, 6 bytes padding.
        assert_eq!(header_field_padding::<u64, u8>(), 6);
    }

    #[test]
    fn test_header_field_padding_u32() {
        // With u32 size type (Header is 8 bytes)

        // Zero header padding needed if storing `u8`s.
        assert_eq!(header_field_padding::<u8, u32>(), 0);

        // With `u64` elements, alignment is 8, header is 8 bytes, no padding.
        assert_eq!(header_field_padding::<u64, u32>(), 0);

        // With `u128` elements, alignment is 16, header is 8 bytes, 8 bytes padding.
        assert_eq!(header_field_padding::<u128, u32>(), 8);
    }

    #[test]
    fn test_header_field_padding_u64() {
        // With u64 size type (Header is 16 bytes)

        // Zero header padding needed for smaller types.
        assert_eq!(header_field_padding::<u8, u64>(), 0);
        assert_eq!(header_field_padding::<u64, u64>(), 0);

        // With `u128` elements, alignment is 16, header is 16 bytes, no padding.
        assert_eq!(header_field_padding::<u128, u64>(), 0);
    }
}
